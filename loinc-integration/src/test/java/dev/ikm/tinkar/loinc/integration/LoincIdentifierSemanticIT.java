package dev.ikm.tinkar.loinc.integration;

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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincIdentifierSemanticIT extends LoincAbstractIntegrationTest {

    /**
     * Test LoincIdentifier Loinc.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertConcept().
     */
    @Test
    public void testIdentifierConceptSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/LoincCsv_identifiers_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
        int notFound = processLoincFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Loinc.csv 'Identifier' semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertLine(String[] columns) {
        StateSet state = null;
        if (columns[11].equals("ACTIVE") || columns[11].equals("TRIAL") || columns[11].equals("DISCOURAGED")) {
            state = StateSet.ACTIVE;
        } else {
            state = StateSet.INACTIVE;
        }

        String identifier = columns[0];
        UUID uuid = UuidUtil.fromSNOMED(identifier);

        StampCalculator stampCalc = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(state, Coordinates.Position.LatestOnMaster()));
        ConceptRecord entity = EntityService.get().getEntityFast(uuid);
        Latest<ConceptVersionRecord> latest = stampCalc.latest(entity);

        return latest.isPresent();
    }
}
