package dev.ikm.tinkar.loinc.integration;

import dev.ikm.maven.LoincUtility;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.EntityProxy;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincDefinitionSemanticIT extends LoincAbstractIntegrationTest {

	public static final String MODULE_DESCRIPTION = "LOINC® modules (SOLOR)";
    /**
     * Test LoincRowConcepts Loinc.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertConcept().
     */
    @Test
    public void testLoincDefinitionSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/LoincCsv_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
        int notFound = processFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Loinc.csv semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertLine(String[] columns) {
        UUID id = uuid(columns[0]);
        StateSet active = null;
        if (columns[11].equals("ACTIVE") || columns[11].equals("TRIAL") || columns[11].equals("DISCOURAGED")) {
            active = StateSet.ACTIVE;
        } else {
            active = StateSet.INACTIVE;
        }
        StampCalculator stampCalc = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));
        ConceptRecord entity = EntityService.get().getEntityFast(id);
        Latest<ConceptVersionRecord> latest = stampCalc.latest(entity);
        
        EntityProxy.Concept module = LoincUtility.getModuleConcept();
        
        // String identifierSchemeId = columns[0];
        // UUID uuid = UuidUtil.fromSNOMED(identifierSchemeId);
        // EntityProxy.Concept loincNameConcept = LoincUtility.getLoincNumConcept(uuid);

        return latest.isPresent() && MODULE_DESCRIPTION.equals(module.description());
    }
}
