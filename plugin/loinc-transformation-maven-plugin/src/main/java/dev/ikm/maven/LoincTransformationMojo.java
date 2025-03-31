package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.assembler.SemanticAssembler;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.AxiomSyntax;
import dev.ikm.tinkar.composer.template.USDialect;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;

import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;

import org.apache.maven.plugins.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static dev.ikm.tinkar.terms.TinkarTerm.*;

@Mojo(name = "run-loinc-transformation", defaultPhase = LifecyclePhase.INSTALL)
public class LoincTransformationMojo extends AbstractMojo {
    private static final Logger LOG = LoggerFactory.getLogger(LoincTransformationMojo.class.getSimpleName());

    @Parameter(property = "origin.namespace", required = true)
    String namespaceString;

    private File partCsv;

    private File loincCsv;

    @Parameter(property = "datastorePath", required = true)
    private String datastorePath;

    @Parameter(property = "inputDirectoryPath", required = true)
    private String inputDirectoryPath;

    @Parameter(property = "dataOutputPath", required = true)
    private String dataOutputPath;

    @Parameter(property = "controllerName", defaultValue = "Open SpinedArrayStore")
    private String controllerName;

    @Parameter(property = "threadCount", defaultValue = "4")
    private int threadCount;

    private UUID namespace;
    private ExecutorService executorService;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        LOG.info("########## Loinc Transformer Starting...");

        this.namespace = UUID.fromString(namespaceString);
        File datastore = new File(datastorePath);

        try {
            unzipRawData(inputDirectoryPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.executorService = Executors.newFixedThreadPool(threadCount);

        // Start by initializing the datastore
        initializeDatastore(datastore);
        EntityService.get().beginLoadPhase();

        try {
            Composer composer = new Composer("Loinc Transformer Composer");

            // Process part.csv first, then process loinc.csv
            // This avoids potential concurrent modification issues with the composer
            try {
                LOG.info("Starting part.csv processing...");
                List<PartData> filteredParts = processPartCsvAsync();
                createPartConceptsAsync(filteredParts, composer);
                LOG.info("Part.csv processing completed");

                LOG.info("Starting loinc.csv processing...");
                processLoincRowsAsync(composer);
                LOG.info("Loinc.csv processing completed");
            } catch (Exception e) {
                LOG.error("Error during data processing", e);
            }

            // Commit all sessions after both processes are complete
            LOG.info("Committing all sessions...");
            composer.commitAllSessions();
            LOG.info("Sessions committed successfully");
        } finally {
            executorService.shutdown();
            EntityService.get().endLoadPhase();
            PrimitiveData.stop();
            LOG.info("########## Loinc Transformation Completed.");
        }
    }


    private void unzipRawData(String zipFilePath) throws IOException {
        File outputDirectory = new File(dataOutputPath);
        try(ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                File newFile = new File(outputDirectory, zipEntry.getName());
                if(zipEntry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    try(FileOutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer,0,len);
                        }
                    }
                }
                zis.closeEntry();
            }
        }
        searchDataFolder(outputDirectory);
    }

    private File searchDataFolder(File dir) {
        if (dir.isDirectory()){
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().equals("Part.csv")) {
                        partCsv = file;
                    } else if (file.getName().equals("Loinc.csv")) {
                        loincCsv = file;
                    }
                    File found = searchDataFolder(file);
                    if (found != null) {
                        return found;
                    }
                }
            }
        }
        return null;
    }


    /**
     * Process part.csv by filtering for specific part types
     * Returns a list of filtered PartData objects
     */
    private List<PartData> processPartCsvAsync() {
        LOG.info("Starting part.csv processing");

        List<PartData> filteredPartData = Collections.synchronizedList(new ArrayList<>());

        try (BufferedReader reader = new BufferedReader(new FileReader(partCsv))) {
            String header = reader.readLine(); // Skip header

            // Read all lines and convert to a list for parallel processing
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            // Process lines in parallel
            lines.parallelStream().forEach(csvLine -> {
                String[] columns = splitCsvLine(csvLine);
                if (columns.length < 5) {
                    LOG.warn("Invalid line in part.csv " + csvLine);
                    return;
                }

                String partNumber = removeQuotes(columns[0]); // A
                String partTypeName = removeQuotes(columns[1]); // B
                String partName = removeQuotes(columns[2]); // C
                String partDisplayName = removeQuotes(columns[3]); // D
                String status = removeQuotes(columns[4]); // E

                // Only process rows with the target part types
                if (TARGET_PART_TYPES.contains(partTypeName)) {
                    PartData partData = new PartData(partNumber, partTypeName, partName, partDisplayName, status);
                    filteredPartData.add(partData);
                }
            });
        } catch (IOException e) {
            LOG.error("Error reading part.csv", e);
            return new ArrayList<>();
        }

        LOG.info("Filtered " + filteredPartData.size() + " part entries with target part types");
        return filteredPartData;
    }

    // List of specific part types to filter for
    private static final Set<String> TARGET_PART_TYPES = Set.of(
            "COMPONENT",
            "PROPERTY",
            "TIME",
            "SYSTEM",
            "SCALE",
            "METHOD",
            "CLASS"
    );

    /**
     * Create concepts for the filtered part data
     */
    private void createPartConceptsAsync(List<PartData> filteredPartData, Composer composer) {

        // Lock object for synchronizing access to the composer
        final Object composerLock = new Object();

        // Use CompletableFuture for parallel processing of part concepts
        List<CompletableFuture<Void>> partConceptFutures = new ArrayList<>();

        // Process part data in batches to reduce contention
        int batchSize = 10;
        List<List<PartData>> batches = new ArrayList<>();

        for (int i = 0; i < filteredPartData.size(); i += batchSize) {
            int end = Math.min(i + batchSize, filteredPartData.size());
            batches.add(new ArrayList<>(filteredPartData.subList(i, end)));
        }

        LOG.info("Split part data into " + batches.size() + " batches for processing");

        // Process each batch in a separate future
        for (List<PartData> batch : batches) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (PartData partData : batch) {
                    // Synchronize access to the composer object
                    synchronized (composerLock) {
                        try {
                            createLoincPartConcept(partData, composer);
                        } catch (Exception e) {
                            LOG.error("Error creating part concept for " + partData.getPartTypeName(), e);
                        }
                    }
                }
            }, executorService);
            partConceptFutures.add(future);
        }

        // Wait for all part concept creation tasks to complete
        try {
            CompletableFuture.allOf(partConceptFutures.toArray(new CompletableFuture[0])).join();
            LOG.info("Part concept creation completed");
        } catch (Exception e) {
            LOG.error("Error waiting for part concept creation to complete", e);
        }
    }

    /**
     * Process LOINC rows and create semantics
     */
    private void processLoincRowsAsync(Composer composer) {
        LOG.info("Starting LOINC.csv processing");

        // Lock object for synchronizing access to the composer
        final Object composerLock = new Object();

        try (BufferedReader reader = new BufferedReader(new FileReader(loincCsv))) {
            String header = reader.readLine(); // skip header

            // Read all lines from LOINC.csv
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            LOG.info("Read " + lines.size() + " LOINC rows for processing");

            // Process lines in chunks for better memory management
            final int CHUNK_SIZE = 100;
            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            for (int i = 0; i < lines.size(); i += CHUNK_SIZE) {
                final int startIndex = i;
                final int endIndex = Math.min(i + CHUNK_SIZE, lines.size());

                CompletableFuture<Void> chunkFuture = CompletableFuture.runAsync(() -> {
                    try {
                        // Process all rows in this chunk sequentially but with synchronized composer access
                        for (int j = startIndex; j < endIndex; j++) {
                            final String csvLine = lines.get(j);

                            String[] columns = splitCsvLine(csvLine);
                            if (columns.length < 40) {
                                LOG.warn("Invalid loinc.csv row (insufficient columns): " + csvLine);
                                continue;
                            }

                            // Synchronize access to the composer object
                            synchronized (composerLock) {
                                try {
                                    createLoincRowConcept(composer, columns);
                                } catch (Exception e) {
                                    LOG.error("Error creating LOINC concept for row: " + j, e);
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error processing LOINC chunk " + startIndex + " to " + endIndex, e);
                    }
                }, executorService);

                allFutures.add(chunkFuture);
            }

            // Wait for all chunk processing to complete
            try {
                CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
                LOG.info("LOINC processing completed");
            } catch (Exception e) {
                LOG.error("Error waiting for LOINC processing to complete", e);
            }

        } catch (IOException e) {
            LOG.error("Error reading loinc.csv for semantic processing", e);
        }
    }


    /**
     * Creates a new LOINC concept based on the provided part data.
     */
    private void createLoincPartConcept(PartData partData, Composer composer) {
        State state = "ACTIVE".equals(partData.getStatus()) ? State.ACTIVE : State.INACTIVE;

        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace); // Regenstrief Institute, Inc. Author
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace); // Loinc Module??
        EntityProxy.Concept path = LoincUtility.getPathConcept(); // Master Path

        UUID conceptUuid = UuidT5Generator.get(namespace, partData.getPartNumber());

        EntityProxy.Concept loincNumConcept = LoincUtility.getLoincNumConcept(namespace);

        Session session = composer.open(state, author, module, path);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));
            EntityProxy.Concept parent = EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, partData.getPartTypeName())));
            session.compose((ConceptAssembler assembler) -> {
                assembler.concept(concept)
                        .attach((FullyQualifiedName fqn) -> fqn
                                .language(ENGLISH_LANGUAGE)
                                .text(partData.getPartName()) // Column C
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()))
                        .attach((Synonym syn) -> syn
                                .language(ENGLISH_LANGUAGE)
                                .text(partData.getPartDisplayName()) // Column D
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()))
                        .attach((Identifier identifier) -> identifier
                                .source(loincNumConcept)  // loinc num - connecting loinc concept
                                .identifier(partData.getPartNumber())) // Column A
                        .attach(new StatedAxiom()
                                .isA(parent)); // Column B
            });

            // Create the two Description Semantics
            createDescriptionSemantic(session, concept, partData.getPartDisplayName(), FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE);
            createDescriptionSemantic(session, concept, partData.getPartName(), REGULAR_NAME_DESCRIPTION_TYPE);

            // Create the Identifier Semantic
            createIdentifierSemantic(session, concept, partData.getPartNumber());

            // Create the Axiom Semantic for Part Concepts
            createAxiomSemanticForPartConcept(session, concept, partData.getPartTypeName());
        } catch (Exception e) {
            LOG.error("Error creating concept for part: " + partData.getPartTypeName(), e);
        }
    }

    /**
     * Creates a new LOINC concept based on the LOINC row data.
     * This creates a concept for each row in the LOINC CSV.
     */
    private void createLoincRowConcept(Composer composer, String[] columns) {
        String loincNum = removeQuotes(columns[0]);
        String longCommonName = removeQuotes(columns[25]);
        String consumerName = removeQuotes(columns[12]);
        String shortName = removeQuotes(columns[20]);
        String relatedNames2 = removeQuotes(columns[19]);
        String displayName = removeQuotes(columns[39]);
        String definitionDescription = removeQuotes(columns[10]);
        String status = removeQuotes(columns[11]); // STATUS column

        State state = State.ACTIVE;
        if ("DEPRECATED".equals(status)) {
            state = State.INACTIVE;
        }

        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace);
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace);
        EntityProxy.Concept path = LoincUtility.getPathConcept();

        UUID conceptUuid = UuidT5Generator.get(namespace, loincNum);
        String identifier = UuidT5Generator.get(namespace, loincNum).toString(); // Using the LOINC_NUM as identifier

        Session session = composer.open(state, author, module, path);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));
            EntityProxy.Semantic semantic;

            if ("TRIAL".equals(status)) {
                EntityProxy.Pattern pattern = LoincUtility.getLoincTrialStatusPattern(namespace);
                semantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept + loincNum + status)));
                session.compose((SemanticAssembler assembler) -> {
                    assembler.semantic(semantic)
                            .pattern(pattern)
                            .reference(concept)
                            .fieldValues(fv -> fv.with(""));
                });

            } else if ("DISCOURAGED".equals(status)) {
                EntityProxy.Pattern pattern = LoincUtility.getLoincDiscouragedPattern(namespace);
                semantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept + loincNum + status)));
                session.compose((SemanticAssembler assembler) -> {
                    assembler.semantic(semantic)
                            .pattern(pattern)
                            .reference(concept)
                            .fieldValues(fv -> fv.with(""));
                });
            }

            session.compose((ConceptAssembler assembler) -> {
                // Create the concept with FQN and synonyms
                assembler.concept(concept)
                        .attach((FullyQualifiedName fqn) -> fqn
                                .language(ENGLISH_LANGUAGE)
                                .text(longCommonName) // LONG_COMMON_NAME as FQN
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()));

                if (consumerName != null && !consumerName.isEmpty()) {
                    assembler.attach((Synonym syn) -> syn
                            .language(ENGLISH_LANGUAGE)
                            .text(consumerName)
                            .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                            .attach(usDialect()));
                }

                if (shortName != null && !shortName.isEmpty()) {
                    assembler.attach((Synonym syn) -> syn
                            .language(ENGLISH_LANGUAGE)
                            .text(shortName)
                            .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                            .attach(usDialect()));
                }

                if (displayName != null && !displayName.isEmpty()) {
                    assembler.attach((Synonym syn) -> syn
                            .language(ENGLISH_LANGUAGE)
                            .text(displayName)
                            .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                            .attach(usDialect()));
                }

                assembler.attach((Identifier identifierObj) -> identifierObj
                        .source(UNIVERSALLY_UNIQUE_IDENTIFIER)
                        .identifier(identifier));
            });

            // Create description semantics for non-empty fields
            if (!longCommonName.isEmpty()) {
                createDescriptionSemantic(session, concept, longCommonName, FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE);
            }

            if (!consumerName.isEmpty()) {
                createDescriptionSemantic(session, concept, consumerName, REGULAR_NAME_DESCRIPTION_TYPE);
            }

            if (!shortName.isEmpty()) {
                createDescriptionSemantic(session, concept, shortName, REGULAR_NAME_DESCRIPTION_TYPE);
            }

            if (!relatedNames2.isEmpty()) {
                createDescriptionSemantic(session, concept, relatedNames2, REGULAR_NAME_DESCRIPTION_TYPE);
            }

            if (!displayName.isEmpty()) {
                createDescriptionSemantic(session, concept, displayName, REGULAR_NAME_DESCRIPTION_TYPE);
            }

            if (!definitionDescription.isEmpty()) {
                createDescriptionSemantic(session, concept, definitionDescription, DEFINITION_DESCRIPTION_TYPE);
            }

            // Create identifier semantic
            createIdentifierSemantic(session, concept, loincNum);

            // Create axiom semantic using the existing method
            createAxiomSemanticsLoincConcept(session, concept,
                    loincNum,                   // LOINC_NUM
                    removeQuotes(columns[1]),   // COMPONENT
                    removeQuotes(columns[2]),   // PROPERTY
                    removeQuotes(columns[3]),   // TIME_ASPCT
                    removeQuotes(columns[4]),   // SYSTEM
                    removeQuotes(columns[5]),   // SCALE_TYP
                    removeQuotes(columns[6]));   // METHOD_TYP

            // Create Loinc Class semantic
            createLoincClassSemantic(session, concept,
                    removeQuotes(columns[7]),   // CLASS
                    removeQuotes(columns[13])); // CLASSTYPE

            // Create Example UCUM Units semantic if not empty
            if (!removeQuotes(columns[24]).isEmpty()) {
                createExampleUcumUnitsSemantic(session, concept,
                        removeQuotes(columns[24]));  // EXAMPLE_UNITS
            }

            // Create Test Membership semantic
//            createTestMembershipSemantic(session, concept,
//                    removeQuotes(columns[21]));  // ORDER_OBS
        } catch (Exception e) {
            LOG.error("Error creating concept for LOINC: " + loincNum, e);
        }
    }


    /**
     * Creates a description semantic with the specified description type.
     *
     * @param session The current session
     * @param concept The concept to attach the description to
     * @param description The description text
     * @param descriptionType The type of description (FQN, Regular Name, Definition)
     */
    private void createDescriptionSemantic(Session session, EntityProxy.Concept concept, String description,
                                           EntityProxy.Concept descriptionType) {
        String typeStr = descriptionType.equals(TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE) ? "FQN" :
                descriptionType.equals(TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE) ? "Regular" : "Definition";

        EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + description)));

        try {
            session.compose((SemanticAssembler semanticAssembler) -> semanticAssembler
                    .semantic(semantic)
                    .pattern(TinkarTerm.DESCRIPTION_PATTERN)
                    .reference(concept)
                    .fieldValues(fieldValues -> fieldValues
                            .with(TinkarTerm.ENGLISH_LANGUAGE)
                            .with(description)
                            .with(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                            .with(descriptionType)
                    ));
        } catch (Exception e) {
            LOG.error("Error creating " + typeStr + " description semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates an identifier semantic based on LOINC_NUM.
     */
    private void createIdentifierSemantic(Session session, EntityProxy.Concept concept, String identifier) {
        EntityProxy.Concept identifierSource = LoincUtility.getLoincNumConcept(namespace);
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + identifier))))
                        .pattern(IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fv -> fv
                                .with(identifierSource)
                                .with(identifier));
            });
        } catch (Exception e) {
            LOG.error("Error creating identifier semantic for concept: " + concept, e);
        }
    }

    private void createAxiomSemanticForPartConcept(Session session, EntityProxy.Concept concept, String partTypeName) {
        EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + partTypeName)));
        EntityProxy.Concept parentConcept = LoincUtility.getParentForPartType(namespace, partTypeName);
        try {
            if (parentConcept!= null) {
                session.compose(new StatedAxiom()
                                .semantic(axiomSemantic)
                                .isA(parentConcept),
                        concept);
            } else {
                session.compose(new StatedAxiom()
                                    .semantic(axiomSemantic),
                            concept);
            }
        } catch (Exception e) {
            LOG.error("Error creating state definition semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a stated definition semantic that attaches an [IS A] relationship to [Observable Entity]
     * and includes role group fields for COMPONENT, PROPERTY, TIME_ASPCT, SYSTEM, SCALE_TYP, and METHOD_TYP.
     */
    private void createAxiomSemanticsLoincConcept(Session session, EntityProxy.Concept concept, String loincNum,
                                                String component, String property, String timeAspect,
                                                String system, String scaleType, String methodType) {

        String owlExpressionWithPublicIds = LoincUtility.buildOwlExpression(namespace, loincNum, component,property, timeAspect,system,scaleType,methodType);
        EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + component)));
        try {
            session.compose(new AxiomSyntax()
                            .semantic(axiomSemantic)
                            .text(owlExpressionWithPublicIds),
                    concept);
        } catch (Exception e) {
            LOG.error("Error creating state definition semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a LOINC class semantic that attaches CLASS and CLASSTYPE.
     */
    private void createLoincClassSemantic(Session session, EntityProxy.Concept concept,
                                          String loincClass, String loincClassType) {
        EntityProxy.Pattern loinClassPattern = LoincUtility.getLoincClassPattern(namespace);
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + loincClass))))
                        .reference(concept)
                        .pattern(loinClassPattern)
                        .fieldValues(fv -> fv
                                .with(loincClass)
                                .with(loincClassType)
                        );
            });
        } catch (Exception e) {
            LOG.error("Error creating LOINC class semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates an Example UCUM Units semantic using the EXAMPLE_UNITS string.
     */
    private void createExampleUcumUnitsSemantic(Session session, EntityProxy.Concept concept, String exampleUnits) {
        EntityProxy.Pattern exampleUnitsPattern = LoincUtility.getExampleUnitsPattern(namespace);
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace,concept.toString() + exampleUnits))))
                        .reference(concept)
                        .pattern(exampleUnitsPattern)
                        .fieldValues(fv -> fv.with(exampleUnits));
            });
        } catch (Exception e) {
            LOG.error("Error creating UCUM units semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a Test Ordered/Reported/Subset Membership semantic based on the ORDER_OBS column.
     * "Order" -> Test Orderable Pattern,
     * "Observed" -> Test Reportable Pattern,
     * "Both" -> both patterns,
     * "Subset" -> Test Subset Pattern.
     */
    private void createTestMembershipSemantic(Session session, EntityProxy.Concept concept, String orderObs) {
        LOG.info("CREATING TESTMEMBERSHIP SEMANTIC");
        LOG.info("Order OBS: " + orderObs);
        EntityProxy.Pattern pattern;
        EntityProxy.Pattern pattern2 = null;
        if ("Order".equalsIgnoreCase(orderObs)) {
            pattern = LoincUtility.getTestOrderablePattern(namespace);
        } else if ("Observation".equalsIgnoreCase(orderObs)) {
            pattern = LoincUtility.getTestReportablePattern(namespace);
        }  else if ("Subset".equalsIgnoreCase(orderObs)) {
            pattern = LoincUtility.getTestSubsetPattern(namespace);
        } else if ("Both".equalsIgnoreCase(orderObs)) {
            pattern = LoincUtility.getTestOrderablePattern(namespace);
            pattern2 = LoincUtility.getTestReportablePattern(namespace);
        } else {
            pattern = null;
        }

        try {
            EntityProxy.Pattern finalPattern = pattern2;
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + orderObs))))
                        .pattern(pattern)
                        .reference(concept)
                        .fieldValues(fv -> fv.with(""));
                if (finalPattern != null){
                    assembler.pattern(finalPattern);
                }
            });

        } catch (Exception e) {
            LOG.error("Error creating test membership semantic for concept: " + concept, e);
        }
    }

    private void initializeDatastore(File datastore){
        CachingService.clearAll();
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, datastore);
        PrimitiveData.selectControllerByName(controllerName);
        PrimitiveData.start();
    }

    private String[] splitCsvLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Toggle the inQuotes flag when we see a quote
                inQuotes = !inQuotes;
                // Also add the quote character to preserve it for later removal
                sb.append(c);
            } else if (c == ',' && !inQuotes) {
                // If we reach a comma and we're not in quotes, add the token
                tokens.add(sb.toString());
                sb.setLength(0);
            } else {
                // Otherwise add the character to the token
                sb.append(c);
            }
        }

        // Add the last token
        tokens.add(sb.toString());

        return tokens.toArray(new String[0]);
    }

    private String removeQuotes(String column) {
        return column.replaceAll("^\"|\"$", "").trim();
    }
    private USDialect usDialect() {
        return new USDialect().acceptability(PREFERRED);
    }

    private static class PartData {
        private final String partNumber;
        private final String partTypeName;
        private final String partName;
        private final String partDisplayName;
        private final String status;

        public PartData(String partNumber, String partTypeName, String partName, String partDisplayName, String status) {
            this.partNumber = partNumber;
            this.partTypeName = partTypeName;
            this.partName = partName;
            this.partDisplayName = partDisplayName;
            this.status = status;
        }

        public String getPartNumber() {
            return partNumber;
        }

        public String getPartTypeName() {
            return partTypeName;
        }

        public String getPartName() {
            return partName;
        }

        public String getPartDisplayName() {
            return partDisplayName;
        }

        public String getStatus() {
            return status;
        }
    }

}