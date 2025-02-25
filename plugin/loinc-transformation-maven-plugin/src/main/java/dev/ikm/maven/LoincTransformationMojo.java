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
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.composer.template.USDialect;
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

        Composer composer = new Composer("Loinc Transformer Composer");

        //For each unique value from loinc.csv, find the corresponding part and create a concept.
        for (String value : uniqueValues) {
            PartData partData = partMap.get(value);
            if (partData == null) {
                LOG.warn("No matching part found in part.csv for value: " + value);
                continue;
            }
            createLoincConcept(partData, composer);
        }

        LOG.info("########## Loinc Transformation Completed.");
    }

    /**
     * Creates a new LOINC concept based on the provided part data.
     */
    private void createLoincConcept(PartData partData, Composer composer) {

        // Create a UUID for the new concept using the part number.

        // Determine the state from the status string from Part Data.
        State state = "ACTIVE".equals(partData.getStatus()) ? State.ACTIVE : State.INACTIVE;

        long time = System.currentTimeMillis(); // What is the correct time reference?

        EntityProxy.Concept author = LoincUtility.getAuthorConcept(namespace); // Regenstrief Institute, Inc. Author
        EntityProxy.Concept module = LoincUtility.getModuleConcept(namespace); // Loinc Module??
        EntityProxy.Concept path = LoincUtility.getPathConcept(namespace); // Master Path

        UUID conceptUuid = UuidT5Generator.get(namespace, partData.getPartNumber());

        Session session = composer.open(state, time, author, module, path);

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
                        .attach((Identifier identifier) -> identifier
                                .source(TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER)
                                .identifier(conceptUuid.toString()))
                        .attach((Identifier identifier) -> identifier
                                .source(TinkarTerm.LOINC_COMPONENT)  // is this the correct Tinkar Term?
                                .identifier(partData.getPartNumber())) // Column A
                        .attach(new StatedAxiom()
                                .isA(parent)); // Column B
                // DO WE NEED THE REGULAR NAME?
            });
        } catch (Exception e) {
            LOG.error("Error creating concept for part: " + partData.getPartTypeName(), e);
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