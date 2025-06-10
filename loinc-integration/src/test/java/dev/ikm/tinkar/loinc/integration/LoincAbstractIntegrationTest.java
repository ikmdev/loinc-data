package dev.ikm.tinkar.loinc.integration;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class LoincAbstractIntegrationTest {
    Logger log = LoggerFactory.getLogger(LoincAbstractIntegrationTest.class);
    static String namespaceString;
    boolean isPart;

    @AfterAll
    public static void shutdown() {
        PrimitiveData.stop();
    }

    @BeforeAll
    public static void setup() {
        CachingService.clearAll();
        //Note. Dataset needed to be generated within repo, with command 'mvn clean install'
        namespaceString = System.getProperty("origin.namespace"); // property set in pom.xml
        File datastore = new File(System.getProperty("datastorePath")); // property set in pom.xml
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, datastore);
        PrimitiveData.selectControllerByName("Open SpinedArrayStore");
        PrimitiveData.start();
    }

    /**
     * Find FilePath
     *
     * @param baseDir
     * @param fileKeyword
     * @return absolutePath
     * @throws IOException
     */
    protected String findFilePath(String baseDir, String fileKeyword) throws IOException {

        try (Stream<Path> dirStream = Files.walk(Paths.get(baseDir))) {
            Path targetDir = dirStream.filter(Files::isDirectory)
//                    .filter(path -> path.toFile().getAbsoluteFile().toString().toLowerCase().contains(dirKeyword.toLowerCase()))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Target DIRECTORY not found"));

            try (Stream<Path> fileStream = Files.walk(targetDir)) {
                Path targetFile = fileStream.filter(Files::isRegularFile)
                        .filter(path -> path.toFile().getAbsoluteFile().toString().toLowerCase().contains(fileKeyword.toLowerCase()))
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Target FILE not found for: " + fileKeyword));

                return targetFile.toAbsolutePath().toString();
            }
        }

    }

    /**
     * Process sourceFilePath
     *
     * @param sourceFilePath
     * @param errorFile
     * @return File status, either Found/NotFound
     * @throws IOException
     */
    protected int processLoincFile(String sourceFilePath, String errorFile) throws IOException {
    	isPart = false;
        int notFound = 0, tempIndex = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(sourceFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(errorFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("\"LOINC_NUM\"")) continue;
                String[] columns = (line.split("\",\""));
                switch (columns.length) {
                    case 40:
                        columns[0] = columns[0].replace("\"", ""); //Removes quotation marks (") from first column
                        columns[39] = columns[39].replace("\"", ""); //Removes quotation marks (") from last column

                        if (!assertLine(columns)) {
                            notFound++;
                            bw.write(line);
                            bw.newLine();
                        }
                        break;
                    case 42:
                        tempIndex = 15; //Index for column EXMPL_ANSWERS with values
                        columns[tempIndex] = columns[tempIndex] + "," + columns[tempIndex+1] + "," + columns[tempIndex+2]; //Reassign values split by regex ("Negative","Positive","Intermediate")
                        columns[tempIndex] = columns[tempIndex].substring(1, columns[tempIndex].length() - 1); //Removes first and last quotation marks (") from String
                        do {
                            tempIndex++;
                            columns[tempIndex] = columns[tempIndex+2];
                        } while (tempIndex < 39); //Loop to reassign the String[] Array size to 40 for assertLine() method
                        columns[40] = null;
                        columns[41] = null;
                        columns[0] = columns[0].replace("\"", ""); //Removes quotation marks (") from first column
                        columns[39] = columns[39].replace("\"", ""); //Removes quotation marks (") from last column

                        if (!assertLine(columns)) {
                            notFound++;
                            bw.write(line);
                            bw.newLine();
                        }
                        break;
                    default:
                        log.warn("Invalid loinc.csv row (# of columns '"+ columns.length +"' not matching criteria): " + line);
                }
            }
        }
        log.info("We found file: " + sourceFilePath);
        return notFound;
    }

    /**
     * Process sourceFilePath
     *
     * @param sourceFilePath
     * @param errorFile
     * @return File status, either Found/NotFound
     * @throws IOException
     */
    protected int processPartFile(String sourceFilePath, String errorFile) throws IOException {
    	isPart = true;
        int notFound = 0, tempIndex = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(sourceFilePath));
             BufferedWriter bw = new BufferedWriter(new FileWriter(errorFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("\"PartNumber\"")) continue;
                String[] columns = (line.split("\",\""));

                columns[0] = columns[0].replace("\"", ""); //Removes quotation marks (") from first column
                columns[4] = columns[4].replace("\"", ""); //Removes quotation marks (") from last column

                //Only process rows with the target PartTypeName Transformed. Otherwise, null Entity is returned.
                if (TARGET_PART_TYPES.contains(columns[1])) {
                    if (!assertLine(columns)) { 
                        notFound++;
                        bw.write(line);
                        bw.newLine();
                    }
                }
            }
        }
        log.info("We found file: " + sourceFilePath);
        return notFound;
    }

    protected Map<String,List<String>> processComponentHierarchy(String sourceFilePath) throws IOException {
        Map<String,List<String>> parentCache = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(sourceFilePath))) {
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
        log.info("We found file: " + sourceFilePath);
        return parentCache;
    }

    protected UUID uuid(String id) {
//        return UuidUtil.fromSNOMED(id);
//        return UuidT5Generator.get(UUID.fromString("3094dbd1-60cf-44a6-92e3-0bb32ca4d3de"), id);
        return UuidT5Generator.get(UUID.fromString(namespaceString), id);
    }

    protected String removeQuotes(String column) {
        return column.replaceAll("^\"|\"$", "").trim();
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

    protected static class ConceptMapValue {
        EntityProxy.Concept conceptDescType;
        String term;

        ConceptMapValue(EntityProxy.Concept conceptDescType, String term) {
            this.conceptDescType = conceptDescType;
            this.term = term;
        }
    }

    protected abstract boolean assertLine(String[] columns);
}
