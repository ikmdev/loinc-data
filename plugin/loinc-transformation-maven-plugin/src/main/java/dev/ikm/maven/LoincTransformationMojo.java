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
import dev.ikm.tinkar.composer.template.*;
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

        // Load part.csv into a map keyed by PartTypeName (Column B) , 1
        Map<String, PartData> partMap = loadPartMap();

        // Read loinc.csv and collect unique values from the eight fields.
        Set<String> uniqueValues = collectUniqueValuesFromLoinc();

        initializeDatastore(datastore);
        EntityService.get().beginLoadPhase();

        try {
            Composer composer = new Composer("Loinc Transformer Composer");

            // Use CompletableFuture for parallel processing of part concepts
            List<CompletableFuture<Void>> partConceptFutures = new ArrayList<>();
            for (String value : uniqueValues) {
                PartData partData = partMap.get(value);
                if (partData == null) {
                    LOG.warn("No matching part found in part.csv for value: " + value);
                    continue;
                }

                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    createLoincPartConcept(partData, composer);
                }, executorService);
                partConceptFutures.add(future);
            }
            // Wait for all part concept creation tasks to complete
            CompletableFuture.allOf(partConceptFutures.toArray(new CompletableFuture[0])).join();
            // Process LOINC rows for semantics and create LOINC concepts in parallel
            processLoincRowsMultithreaded(composer, partMap);
            composer.commitAllSessions();
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


    // Read loinc.csv and collect unique values from the eight fields.
    // CSV Columns
    // 0: LOINC_NUM, 1: COMPONENT, 2: PROPERTY, 3: TIME_ASPCT, 4: SYSTEM,
    // 5: SCALE_TYP, 6: METHOD_TYP, 7: CLASS, 13: CLASSTYPE
    private Map<String, PartData> loadPartMap() throws MojoExecutionException {
        Map<String, PartData> partMap = new ConcurrentHashMap<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(partCsv))) {
            String header = reader.readLine();
            String line;
            while((line = reader.readLine()) != null){
                String[] columns = splitCsvLine(line);
                if(columns.length < 5) {
                    LOG.warn("Invalid line in part.csv " + line);
                    continue;
                }
                String partNumber = removeQuotes(columns[0]); // A
                String partTypeName = removeQuotes(columns[1]); // B
                String partName = removeQuotes(columns[2]); // C
                String partDisplayName = removeQuotes(columns[3]); // D
                String status = removeQuotes(columns[4]); // E

                PartData partData = new PartData(partNumber, partTypeName, partName, partDisplayName, status);
                partMap.put(partTypeName, partData);
            }
        } catch (IOException e){
            throw new MojoExecutionException("Error reading part.csv", e);
        }
        return partMap;
    }

    /**
     * Collects unique values from the specified columns in loinc.csv
     */
    private Set<String> collectUniqueValuesFromLoinc() throws MojoExecutionException {
        Set<String> uniqueValues = ConcurrentHashMap.newKeySet();
        Map<String, String> partToLoincMap = new ConcurrentHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(loincCsv))) {
            String header = reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = splitCsvLine(line);
                if (columns.length < 14) {
                    LOG.warn("Invalid line in loinc.csv: " + line);
                    continue;
                }
                uniqueValues.add(removeQuotes(columns[1]));  // COMPONENT
                uniqueValues.add(removeQuotes(columns[2]));  // PROPERTY
                uniqueValues.add(removeQuotes(columns[3]));  // TIME_ASPCT
                uniqueValues.add(removeQuotes(columns[4]));  // SYSTEM
                uniqueValues.add(removeQuotes(columns[5]));  // SCALE_TYP
                uniqueValues.add(removeQuotes(columns[6]));  // METHOD_TYP
                uniqueValues.add(removeQuotes(columns[7]));  // CLASS
                uniqueValues.add(removeQuotes(columns[13])); // CLASSTYPE
            }

        } catch (IOException e) {
            throw new MojoExecutionException("Error reading loinc.csv", e);
        }
        return uniqueValues;
    }

    /**
     * Creates a new LOINC concept based on the provided part data.
     */
    private void createLoincPartConcept(PartData partData, Composer composer) {
        State state = "ACTIVE".equals(partData.getStatus()) ? State.ACTIVE : State.INACTIVE;

        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace); // Regenstrief Institute, Inc. Author
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace); // Loinc Module??
        EntityProxy.Concept path = LoincUtility.getPathConcept(namespace); // Master Path

        UUID conceptUuid = UuidT5Generator.get(namespace, partData.getPartNumber());

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
                                .source(TinkarTerm.LOINC_COMPONENT)  // loinc num - connecting loinc concept
                                .identifier(partData.getPartNumber())) // Column A
                        .attach(new StatedAxiom()
                                .isA(parent)); // Column B
            });
        } catch (Exception e) {
            LOG.error("Error creating concept for part: " + partData.getPartTypeName(), e);
        }
    }

    /**
     * Creates a new LOINC concept based on the LOINC row data.
     * This creates a concept for each row in the LOINC CSV.
     */
    // TODO: Incorporate / attach Pattern
    private void createLoincRowConcept(Composer composer, String[] columns) {
        String loincNum = removeQuotes(columns[0]);
        String longCommonName = removeQuotes(columns[25]);
        String consumerName = removeQuotes(columns[12]);
        String shortName = removeQuotes(columns[20]);
        String displayName = removeQuotes(columns[39]);
        String status = removeQuotes(columns[36]); // STATUS column

        State state = State.ACTIVE;
        if ("DEPRECATED".equals(status)) {
            state = State.INACTIVE;
        }

        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace);
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace);
        EntityProxy.Concept path = LoincUtility.getPathConcept(namespace);

        UUID conceptUuid = UuidT5Generator.get(namespace, loincNum);
        String identifier = UuidT5Generator.get(namespace, loincNum).toString(); // Using the LOINC_NUM as identifier

        Session session = composer.open(state, author, module, path);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

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
                        .source(TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER)
                        .identifier(identifier));

                // Add status pattern semantics if needed
                if ("TRIAL".equals(status)) {
                    // TODO: Add LOINC TRIAL STATUS Pattern Semantic
                    // Implement when pattern is defined
                } else if ("DISCOURAGED".equals(status)) {
                    // TODO: Add LOINC DISCOURAGED STATUS Pattern Semantic
                    // Implement implemented when pattern is defined
                }
            });
        } catch (Exception e) {
            LOG.error("Error creating concept for LOINC: " + loincNum, e);
        }
    }

    /**
     * Process LOINC rows and create semantics in parallel
     */
    private void processLoincRowsMultithreaded(Composer composer, Map<String, PartData> partMap) {
        List<String[]> loincRows = new ArrayList<>();

        // First, read all rows from the CSV file
        try (BufferedReader reader = new BufferedReader(new FileReader(loincCsv))) {
            String header = reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = splitCsvLine(line);
                if (columns.length < 40) {
                    LOG.warn("Invalid loinc.csv row (insufficient columns): " + line);
                    continue;
                }
                loincRows.add(columns);
            }
        } catch (IOException e) {
            LOG.error("Error reading loinc.csv for semantic processing", e);
            return;
        }

        // Process rows in parallel
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String[] columns : loincRows) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                // Create a LOINC row concept
                createLoincRowConcept(composer, columns);

                // Process part-related semantics
                processLoincRowForSemantics(composer, columns, partMap);
            }, executorService);

            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    /**
     * Process a single LOINC row for semantic creation
     */
    private void processLoincRowForSemantics(Composer composer, String[] columns, Map<String, PartData> partMap) {
        // Determine the partData by checking the eight fields in order.
        String[] candidateIndices = { columns[1], columns[2], columns[3], columns[4],
                columns[5], columns[6], columns[7], columns[13] };
        PartData partData = null;
        for (String candidate : candidateIndices) {
            partData = partMap.get(removeQuotes(candidate));
            if (partData != null) {
                break;
            }
        }
        if (partData == null) {
            LOG.warn("No matching part found for loinc.csv row with LOINC_NUM: " + removeQuotes(columns[0]));
            return;
        }

        UUID conceptUuid = UuidT5Generator.get(namespace, partData.getPartNumber());
        State state = "ACTIVE".equals(partData.getStatus()) ? State.ACTIVE : State.INACTIVE;
        long time = System.currentTimeMillis();
        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace);
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace);
        EntityProxy.Concept path = LoincUtility.getPathConcept(namespace);
        Session session = composer.open(state, time, author, module, path);

        try {
            EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));

            // Create the six semantics by calling separate methods.
            createDescriptionSemantic(session, concept,
                    removeQuotes(columns[25]), // LONG_COMMON_NAME
                    removeQuotes(columns[12]), // CONSUMER_NAME
                    removeQuotes(columns[20]), // SHORTNAME
                    removeQuotes(columns[19]), // RELATEDNAMES2
                    removeQuotes(columns[39]), // DISPLAYNAME
                    removeQuotes(columns[10])  // DEFINITIONDESCRIPTION
            );

            createIdentifierSemantic(session, concept,
                    removeQuotes(columns[0])   // LOINC_NUM
            );

            createStatedDefinitionSemantic(session, concept,
                    removeQuotes(columns[1]), // COMPONENT
                    removeQuotes(columns[2]), // PROPERTY
                    removeQuotes(columns[3]), // TIME_ASPCT
                    removeQuotes(columns[4]), // SYSTEM
                    removeQuotes(columns[5]), // SCALE_TYP
                    removeQuotes(columns[6])  // METHOD_TYP
            );

            createLoincClassSemantic(session, concept,
                    removeQuotes(columns[7]),  // CLASS
                    removeQuotes(columns[13])  // CLASSTYPE
            );

            createExampleUcumUnitsSemantic(session, concept,
                    removeQuotes(columns[24])  // EXAMPLE_UNITS
            );

            createTestMembershipSemantic(session, concept,
                    removeQuotes(columns[21])  // ORDER_OBS
            );
        } catch (Exception e) {
            LOG.error("Error creating semantics for LOINC: " + removeQuotes(columns[0]), e);
        }
    }

    /**
     * Creates a description semantic using LONG_COMMON_NAME, CONSUMER_NAME, SHORTNAME, RELATEDNAMES2,
     * DISPLAYNAME, and DEFINITIONDESCRIPTION.
     */
    private void createDescriptionSemantic(Session session, EntityProxy.Concept concept,
                                           String longCommonName, String consumerName,
                                           String shortName, String relatedNames2,
                                           String displayName, String definitionDescription) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace,concept.toString() + longCommonName))))
                        .pattern(TinkarTerm.DESCRIPTION_PATTERN)
                        .reference(concept)
                        .fieldValues(fv -> fv
                                .with(longCommonName)
                                .with(consumerName)
                                .with(shortName)
                                .with(relatedNames2)
                                .with(displayName)
                                .with(definitionDescription)
                        );
            });
        } catch (Exception e) {
            LOG.error("Error creating description semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates an identifier semantic based on LOINC_NUM.
     */
    private void createIdentifierSemantic(Session session, EntityProxy.Concept concept, String loincNum) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + loincNum))))
                        .pattern(TinkarTerm.IDENTIFIER_PATTERN)
                        .reference(concept)
                        .fieldValues(fv -> fv.with(loincNum));
            });
        } catch (Exception e) {
            LOG.error("Error creating identifier semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a stated definition semantic that attaches an [IS A] relationship to [Observable Entity]
     * and includes role group fields for COMPONENT, PROPERTY, TIME_ASPCT, SYSTEM, SCALE_TYP, and METHOD_TYP.
     */
    // TODO: Incorporate / attach Pattern
    // TODO: Reference configureSemanticsForConcept in snomed-ct data - place URI in Utility
    private void createStatedDefinitionSemantic(Session session, EntityProxy.Concept concept,
                                                String component, String property, String timeAspect,
                                                String system, String scaleType, String methodType) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + component))))
                        .reference(concept)
                        .fieldValues(fv -> fv
                                .with(component)
                                .with(property)
                                .with(timeAspect)
                                .with(system)
                                .with(scaleType)
                                .with(methodType)
                        );
            });
        } catch (Exception e) {
            LOG.error("Error creating state definition semantic for concept: " + concept, e);
        }
    }

    /**
     * Creates a LOINC class semantic that attaches CLASS and CLASSTYPE.
     */
    // TODO: Incorporate / attach Pattern
    private void createLoincClassSemantic(Session session, EntityProxy.Concept concept,
                                          String loincClass, String loincClassType) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + loincClass))))
                        .reference(concept)
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
    // TODO: Incorporate / attach UCUM Pattern
    private void createExampleUcumUnitsSemantic(Session session, EntityProxy.Concept concept, String exampleUnits) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace,concept.toString() + exampleUnits))))
                        .reference(concept)
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
//TODO: incorporate pattern based on orderObs String, fix semantic and field values
    private void createTestMembershipSemantic(Session session, EntityProxy.Concept concept, String orderObs) {
        if ("Order".equalsIgnoreCase(orderObs)) {

        } else if ("Observed".equalsIgnoreCase(orderObs)) {

        } else if ("Both".equalsIgnoreCase(orderObs)) {

        } else if ("Subset".equalsIgnoreCase(orderObs)) {

        }

        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(PublicIds.of(UuidT5Generator.get(namespace, concept.toString() + orderObs))))
//                        .pattern()
                        .reference(concept)
                        .fieldValues(fv -> {

                        });
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
        return line.split(",");
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