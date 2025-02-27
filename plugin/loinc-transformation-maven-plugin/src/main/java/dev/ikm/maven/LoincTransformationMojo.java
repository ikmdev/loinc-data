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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static dev.ikm.tinkar.terms.TinkarTerm.*;

@Mojo(name = "run-loinc-transformation", defaultPhase = LifecyclePhase.INSTALL)
public class LoincTransformationMojo extends AbstractMojo {
    private static final Logger LOG = LoggerFactory.getLogger(LoincTransformationMojo.class.getSimpleName());

    @Parameter(property = "origin.namespace", required = true)
    String namespaceString;

    @Parameter(property = "partCsv", required = true)
    private File partCsv;

    @Parameter(property = "loincCsv", required = true)
    private File loincCsv;

    @Parameter(property = "datastorePath", required = true)
    private String datastorePath;

    @Parameter(property = "dataOutputPath", required = true)
    private String dataOutputPath;

    @Parameter(property = "controllerName", defaultValue = "Open SpinedArrayStore")
    private String controllerName;

    private UUID namespace;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        LOG.info("########## Loinc Transformer Starting...");

        this.namespace = UUID.fromString(namespaceString);
        File datastore = new File(datastorePath);

        // Load part.csv into a map keyed by PartTypeName (Column B) , 1
        Map<String, PartData> partMap = new HashMap<>();
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

        // Read loinc.csv and collect unique values from the eight fields.
        // CSV Columns
        // 0: LOINC_NUM, 1: COMPONENT, 2: PROPERTY, 3: TIME_ASPCT, 4: SYSTEM,
        // 5: SCALE_TYP, 6: METHOD_TYP, 7: CLASS, 13: CLASSTYPE
        Set<String> uniqueValues = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(loincCsv))) {
            String header = reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = splitCsvLine(line);
                if (columns.length < 14) {
                    LOG.warn("Invalid line in loinc.csv: " + line);
                    continue;
                }
                uniqueValues.add(removeQuotes(columns[0])); // LOINC NUM
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
//        uniqueValues.remove(""); // remove any empty values?

        initializeDatastore(datastore);
        EntityService.get().beginLoadPhase();

        try {
            Composer composer = new Composer("Loinc Transformer Composer");

            //For each unique value from loinc.csv, find the corresponding part and create a concept.
            for (String value : uniqueValues) {
                PartData partData = partMap.get(value);
                if (partData == null) {
                    LOG.warn("No matching part found in part.csv for value: " + value);
                    continue;
                }
                createLoincPartConcept(partData, composer);
            }
            // process every row in loinc.csv to create additional semantics.
            processLoincRowsForSemantics(composer, partMap);
            composer.commitAllSessions();
        } finally {
            EntityService.get().endLoadPhase();
            PrimitiveData.stop();
            LOG.info("########## Loinc Transformation Completed.");
        }
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
                                .source(TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER)
                                .identifier(conceptUuid.toString()))
                        .attach((Identifier identifier) -> identifier
                                .source(TinkarTerm.LOINC_COMPONENT)  // is this the correct Tinkar Term?
                                .identifier(partData.getPartNumber())) // Column A
                        .attach(new StatedAxiom()
                                .isA(parent)); // Column B
            });
        } catch (Exception e) {
            LOG.error("Error creating concept for part: " + partData.getPartTypeName(), e);
        }
    }

    private void processLoincRowsForSemantics(Composer composer, Map<String, PartData> partMap) {
        try (BufferedReader reader = new BufferedReader(new FileReader(loincCsv))) {
            String header = reader.readLine(); // skip header
            String line;
            while ((line = reader.readLine()) != null) {
                String[] columns = splitCsvLine(line);
                if (columns.length < 40) {
                    LOG.warn("Invalid loinc.csv row (insufficient columns): " + line);
                    continue;
                }
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
                    LOG.warn("No matching part found for loinc.csv row: " + line);
                    continue;
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
                    LOG.error("Error creating semantics for row: " + line, e);
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading loinc.csv for semantic processing", e);
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
    private void createStatedDefinitionSemantic(Session session, EntityProxy.Concept concept,
                                               String component, String property, String timeAspect,
                                               String system, String scaleType, String methodType) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace, concept.toString()))))
                        .reference(concept)
                        .fieldValues(fv -> fv
//                                .with(TinkarTerm.OBSERVABLE_ENTITY) // Placeholder for Observable Entity concept ??
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
    private void createLoincClassSemantic(Session session, EntityProxy.Concept concept,
                                          String loincClass, String loincClassType) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace, concept.toString()))))
//                        .pattern(TinkarTerm.LOINC) // Placeholder constant - Attach
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
    private void createExampleUcumUnitsSemantic(Session session, EntityProxy.Concept concept, String exampleUnits) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace,concept.toString()))))
//                        .pattern(TinkarTerm.LOINC_UCUM_UNITS_PATTERN) // Placeholder constant
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
    private void createTestMembershipSemantic(Session session, EntityProxy.Concept concept, String orderObs) {
        try {
            session.compose((SemanticAssembler assembler) -> {
                assembler.semantic(EntityProxy.Semantic.make(
                                PublicIds.of(UuidT5Generator.get(namespace,concept.toString()))))
//                        .pattern(TinkarTerm.TEST_MEMBERSHIP_PATTERN) // Placeholder constant
                        .reference(concept)
                        .fieldValues(fv -> {
                            if ("Order".equalsIgnoreCase(orderObs)) {
                                fv.with("Test Orderable Pattern"); // CONSTANT??
                            } else if ("Observed".equalsIgnoreCase(orderObs)) {
                                fv.with("Test Reportable Pattern");
                            } else if ("Both".equalsIgnoreCase(orderObs)) {
                                fv.with("Test Orderable Pattern").with("Test Reportable Pattern");
                            } else if ("Subset".equalsIgnoreCase(orderObs)) {
                                fv.with("Test Subset Pattern");
                            }
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