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
import dev.ikm.tinkar.composer.template.AxiomSyntax;
import dev.ikm.tinkar.composer.template.Definition;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedNavigation;
import dev.ikm.tinkar.composer.template.Synonym;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER;
import static dev.ikm.tinkar.terms.TinkarTerm.USER;

@Mojo(name = "run-loinc-transformation", defaultPhase = LifecyclePhase.INSTALL)
public class LoincTransformationMojo extends AbstractMojo {
    private static final Logger LOG = LoggerFactory.getLogger(LoincTransformationMojo.class.getSimpleName());

    @Parameter(property = "origin.namespace", required = true)
    String namespaceString;

    private File partCsv;

    private File loincCsv;

    private File componentCsv;

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
    private final String loincAuthorStr = "Regenstrief Institute, Inc. Author";
    private final EntityProxy.Concept loincAuthor = LoincUtility.makeConceptProxy(namespace, loincAuthorStr);

    private final Map<String,String> idToStatus = new ConcurrentHashMap<>();
    private final Map<String,List<String>> parentCache = new HashMap<>();
    private final Set<String> processedMultiParentCodes = new HashSet<>();

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        LOG.info("########## Loinc Transformer Starting...");

        this.namespace = UUID.fromString(namespaceString);
        File datastore = new File(datastorePath);

        LoincUtility.clearCaches();

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
                createLoincAuthor(composer);
                List<PartData> filteredParts = processPartCsvAsync();
                processComponentParentCache();
                processComponentRowsAsync(filteredParts, composer);
                createPartConceptsAsync(filteredParts, composer);
                processLeftOverComponents(composer);
                processLoincRowsAsync(composer);
            } catch (Exception e) {
                LOG.error("Error during data processing", e);
            }
            LOG.info("Creating Concepts for Sets...");
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

    private void createLoincAuthor(Composer composer) {
        createConcept(composer, loincAuthorStr, "LOINC Author",
                "Regenstrief Institute, Inc. Author - The entity responsible for publishing LOINC",
                loincAuthor, USER);
    }

    private void createConcept(Composer composer, String fullyQualifiedName, String synonym, String definition,
                               EntityProxy.Concept identifier, EntityProxy.Concept parent, EntityProxy.Concept... children) {

        Session session = composer.open(State.ACTIVE, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);

        session.compose((ConceptAssembler conceptAssembler) -> {
                    conceptAssembler.concept(identifier)
                            .attach((FullyQualifiedName fqn) -> fqn
                                    .language(ENGLISH_LANGUAGE)
                                    .text(fullyQualifiedName)
                                    .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                    .attach(usDialect()))
                            .attach((Identifier id) -> id
                                    .source(UNIVERSALLY_UNIQUE_IDENTIFIER)
                                    .identifier(identifier.asUuidArray()[0].toString()))
                            .attach(new StatedNavigation()
                                    .parents(parent)
                                    .children(children));
                        conceptAssembler.attach(new StatedAxiom()
                                .isA(parent));
                    if (synonym != null) {
                        conceptAssembler.attach((Synonym syn) -> syn
                                .language(ENGLISH_LANGUAGE)
                                .text(synonym)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()));
                    }
                    if (definition != null) {
                        conceptAssembler.attach((Definition defn) -> defn
                                .language(ENGLISH_LANGUAGE)
                                .text(definition)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()));
                    }
                }
        );
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
                    } else if (file.getName().equals("ComponentHierarchyBySystem.csv")) {
                        componentCsv = file;
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

    protected void processComponentParentCache() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(componentCsv))) {

            String line;
            //skip first two lines of Component file:
            br.readLine();
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] columns = (line.split("\",\""));

                String id = columns[3].replace("\"", ""); //Removes quotation marks (") from first column
                String parentId = columns[2].replace("\"", ""); //Removes quotation marks (") from last column

                List<String> parents = parentCache.get(id);
                if (parents == null) {
                    parents = new ArrayList<String>();
                }
                parents.add(parentId);
                parentCache.put(id, parents);
            }
        }
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
                    idToStatus.put(partNumber, status);
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
     * Process Component rows and create semantics
     */
    private void processComponentRowsAsync(List<PartData> parts, Composer composer) throws Exception {
        LOG.info("Starting ComponentHierarchyBySystem.csv processing");

        // Lock object for synchronizing access to the composer
        final Object composerLock = new Object();

        try (BufferedReader reader = new BufferedReader(new FileReader(componentCsv))) {
            reader.readLine(); // skip header

            // handle first line of data as addition to existing starter data Component
            String componentLine = reader.readLine();
            String[] cols = splitCsvLine(componentLine);
            // This should exist already from LoincStarterData.
            EntityProxy.Concept component = LoincUtility.makeConceptProxy(namespace, "Component");
            // Add new UUID to existing Component.
            EntityProxy.Concept newComponent = EntityProxy.Concept.make("Component", component.asUuidArray()[0], UuidT5Generator.get(namespace, removeQuotes(cols[3])));
            Session session = composer.open(State.ACTIVE, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
            session.compose((ConceptAssembler conceptAssembler) -> conceptAssembler.concept(newComponent));

            // Read all lines from ComponentHierarchyBySystem.csv
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }

            LOG.info("Read " + lines.size() + " ComponentHierarchyBySystem rows for processing");

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
                            if (columns.length < 5) {
                                LOG.warn("Invalid ComponentHierarchyBySystem.csv row (insufficient columns): " + csvLine);
                                continue;
                            }

                            // Synchronize access to the composer object
                            synchronized (composerLock) {
                                try {
                                    createComponentRowConcept(parts, composer, columns);
                                } catch (Exception e) {
                                    LOG.error("Error creating ComponentHierarchyBySystem concept for row: " + j, e);
                                    throw e;
                                }
                            }
                        }
                    } catch (Exception e) {
                        LOG.error("Error processing ComponentHierarchyBySystem ", e);//chunk " + startIndex + " to " + endIndex, e);
                        throw e;
                    }
                }, executorService);

                allFutures.add(chunkFuture);
            }

            // Wait for all chunk processing to complete
            try {
                CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
                LOG.info("ComponentHierarchyBySystem processing completed");
            } catch (Exception e) {
                LOG.error("Error waiting for ComponentHierarchyBySystem processing to complete", e);
            }

        } catch (IOException e) {
            LOG.error("Error reading ComponentHierarchyBySystem.csv for semantic processing", e);
            throw e;
        }
    }

    /**
     * Creates a new LOINC concept based on the provided part data.
     */
    private void createComponentRowConcept(List<PartData> parts, Composer composer, String[] columns) {
        String code = removeQuotes(columns[3]);
        String codeText = removeQuotes(columns[4]);
        String immediateParent = removeQuotes(columns[2]);

        // skip if not prefixed by LP
        if (!code.startsWith("LP") || processedMultiParentCodes.contains(code)) {
            return;
        }
        try {
            // This concept will be created later on when we parse the Part file.
            State state = State.ACTIVE;
            Session session = composer.open(state, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
            Session activeSession = composer.open(State.ACTIVE, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
            EntityProxy.Concept rowConcept = EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, code)));
            // Create the Axiom Semantic
            EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, rowConcept.publicId().asUuidArray()[0] + code + "AXIOM")));
            Entity entity = EntityService.get().getEntityFast(axiomSemantic.asUuidArray()[0]);
            int count = 2;
            while (entity!=null) {
                axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, rowConcept.publicId().asUuidArray()[0] + code + "AXIOM" + count)));
                entity = EntityService.get().getEntityFast(axiomSemantic.asUuidArray()[0]);
                count++;
            }
            // We can have multiple axiomSemantics in the case where we have a Part with multiple parents
            // This will also get created during the parsing of Part later on
            List<String> parents = parentCache.get(code);
            List<EntityProxy.Concept> parentConcepts = new ArrayList<>();
            if (!parents.isEmpty()) {
                parents.forEach(parent -> {
                    parentConcepts.add(EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, parent))));
                });
                processedMultiParentCodes.add(code);
            } else {
                parentConcepts.add(EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, immediateParent))));
            }
            EntityProxy.Concept[] parentArr = new EntityProxy.Concept[parentConcepts.size()];
            parentArr = parentConcepts.toArray(parentArr);
            try {
                activeSession.compose(new StatedAxiom()
                    .semantic(axiomSemantic)
                    .isA(parentArr),
                    rowConcept);
            } catch (Exception e) {
                LOG.error("Error creating stated definition semantic for concept: " + rowConcept, e);
                throw e;
            }

            LoincUtility.addComponentPartToCache(code, codeText);
            // This will not be created later on. This is a new concept only existing in the Component file
            String obsEnt = "Observable Entity of Component " + codeText;
            EntityProxy.Concept rowConcept2 = EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, obsEnt)));
            session.compose((ConceptAssembler concept) -> concept
                    .concept(rowConcept2)
                    .attach((FullyQualifiedName fqn) -> fqn
                            .language(TinkarTerm.ENGLISH_LANGUAGE)
                            .text(obsEnt)
                            .caseSignificance(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                    )
            );

            String owlExpressionWithPublicIds = LoincUtility.buildComponentOwlExpression(namespace, rowConcept2, rowConcept);
            EntityProxy.Semantic axiomSemantic2 = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, rowConcept2.publicId().asUuidArray()[0] + codeText + "AXIOM")));
            try {
                activeSession.compose(new AxiomSyntax()
                                .semantic(axiomSemantic2)
                                .text(owlExpressionWithPublicIds),
                        rowConcept2);
            } catch (Exception e) {
                LOG.error("Error creating stated definition semantic for concept: " + rowConcept2, e);
            }
        } catch (Exception e) {
            LOG.error("Error creating concept for Component: " + code, e);
            throw e;
        }
    }

    /**
     * Process the concepts in the Component cache that were not found in the Part file.
     * All concepts should be considered ACTIVE
     * @param composer
     */
    private void processLeftOverComponents(Composer composer) {
        // We need to be consistent with our states across Component, Part, and Loinc
        Session session = composer.open(State.ACTIVE, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
        BiConsumer<String,String> consumer = (code, codeText) -> {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(UuidT5Generator.get(namespace, code)));
                session.compose((ConceptAssembler assembler) -> assembler
                        .concept(concept)
                        .attach((FullyQualifiedName fqn) -> fqn
                                .language(TinkarTerm.ENGLISH_LANGUAGE)
                                .text(codeText)
                                .caseSignificance(TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE)
                        )
                );
            createIdentifierSemantic(session, concept, code);
        };
        LoincUtility.forEachComponent(consumer);
    }

    /**
     * Creates a new LOINC concept based on the provided part data.
     */
    private void createLoincPartConcept(PartData partData, Composer composer) {
        State state = State.ACTIVE;

        EntityProxy.Concept author = loincAuthor; // Regenstrief Institute, Inc. Author
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace); // Loinc Module??
        EntityProxy.Concept path = LoincUtility.getPathConcept(); // Master Path

        UUID conceptUuid = UuidT5Generator.get(namespace, partData.getPartNumber());

        LoincUtility.addPartToCache(partData.getPartName().toLowerCase(), partData.getPartTypeName(), partData.getPartNumber());

        EntityProxy.Concept loincNumConcept = LoincUtility.getLoincNumConcept(namespace);

        Session session = composer.open(state, author, module, path);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));
            session.compose((ConceptAssembler assembler) -> {
                assembler.concept(concept);
            });

            // Create the two Description Semantics
            createDescriptionSemantic(session, concept, partData.getPartDisplayName(), TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE);
            createDescriptionSemantic(session, concept, partData.getPartName(), TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE);

            // Create the Identifier Semantic
            createIdentifierSemantic(session, concept, partData.getPartNumber());

            // Create the Axiom Semantic for Part Concepts
            // if the getPartNumber code is not in the Component cache we built in previous step, then createAxiom... call
            if (LoincUtility.removeComponentPartFromCache(partData.getPartNumber()) == null) {
                createAxiomSemanticForPartConcept(session, concept, partData.getPartTypeName());
            }
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

            EntityProxy.Concept author = loincAuthor;
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
                    assembler.concept(concept);
                });

                // Create description semantics for non-empty fields
                if (!longCommonName.isEmpty()) {
                    createDescriptionSemantic(session, concept, longCommonName, FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE);
                }

                if (!consumerName.isEmpty()) {
                    createDescriptionSemantic(session, concept, consumerName, TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE);
                }

                if (!shortName.isEmpty()) {
                    createDescriptionSemantic(session, concept, shortName, TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE);
                }

                if (!relatedNames2.isEmpty()) {
                    createDescriptionSemantic(session, concept, relatedNames2, TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE);
                }

                if (!displayName.isEmpty()) {
                    createDescriptionSemantic(session, concept, displayName, TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE);
                }

                if (!definitionDescription.isEmpty()) {
                    createDescriptionSemantic(session, concept, definitionDescription, TinkarTerm.DEFINITION_DESCRIPTION_TYPE);
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
                if (!removeQuotes(columns[21]).isEmpty()) {
                    createTestMembershipSemantic(session, concept,
                            removeQuotes(columns[21]));
                }
                ;  // ORDER_OBS
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
        String typeStr = descriptionType.equals(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE) ? "FQN" :
                descriptionType.equals(TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE) ? "Regular" : "Definition";

        EntityProxy.Semantic semantic = EntityProxy.Semantic.make(
                PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + description + typeStr + "DESC")));

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
                                PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + identifier))))
                        .pattern(TinkarTerm.IDENTIFIER_PATTERN)
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
        EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + partTypeName + "AXIOM")));
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
            String owlExpressionWithPublicIds = LoincUtility.buildOwlExpression(namespace, loincNum, component, property, timeAspect, system, scaleType, methodType);
            EntityProxy.Semantic axiomSemantic = EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + component + "AXIOM")));
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
        int classTypeInt = Integer.parseInt(loincClassType);
        String loincClassPartNumber = LoincUtility.getPartNumberFromCache(loincClass.toLowerCase(), "CLASS");
        EntityProxy.Concept loincClassPartConcept = LoincUtility.makeConceptProxy(namespace, loincClassPartNumber);
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + loincClass))))
                        .reference(concept)
                        .pattern(loinClassPattern)
                        .fieldValues(fv -> fv
                                .with(loincClassPartConcept)
                                .with(classTypeInt)
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
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace,concept.publicId().asUuidArray()[0] + exampleUnits + "UCUM"))))
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
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.publicId().asUuidArray()[0] + orderObs + "TESTMEM"))))
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
                inQuotes = !inQuotes;
                sb.append(c);
            } else if (c == ',' && !inQuotes) {
                tokens.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        tokens.add(sb.toString());
        return tokens.toArray(new String[0]);
    }

    private String removeQuotes(String column) {
        return column.replaceAll("^\"|\"$", "").trim();
    }
    private USDialect usDialect() {
        return new USDialect().acceptability(TinkarTerm.PREFERRED);
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