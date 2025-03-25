package dev.ikm.tinkar.loinc.integration;

import dev.ikm.tinkar.common.id.IntIds;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StampPositionRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincRowConceptSemanticIT extends LoincAbstractIntegrationTest {

    /**
     * Test LoincRowConcepts Loinc.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertConcept().
     */
    @Test
    public void testLoincRowConceptSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/LoincCsv_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
        int notFound = processFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Loinc.csv semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertLine(String[] columns) {
//        String loincNum = columns[0];
        String component = columns[1];
        String property = columns[2];
        String timeAspct = columns[3];
        String system = columns[4];
        String scaleTyp = columns[5];
        String methodTyp = columns[6];
        String className = columns[7];
        String versionLastChange = columns[8];
        String changeType = columns[9];
        String definitionDesc = columns[10];
//        String status = columns[11];
        String consumerName = columns[12];
        String classType = columns[13];
        String formula = columns[14];
        String exampleAnswers = columns[15];
        String surveyQuestTxt = columns[16];
        String surveyQuestSrc = columns[17];
        String unitRequired = columns[18];
        String relatedNames2 = columns[19];
        String shortName = columns[20];
        String orderObs = columns[21];
        String hl7FieldSubfieldId = columns[22];
        String extCopyrightNotice = columns[23];
        String exampleUnits = columns[24];
        String longCommonName = columns[25];
        String exampleUcumUnits = columns[26];
        String statusReason = columns[27];
        String statusText = columns[28];
        String changeReasonPublic = columns[29];
        String commonTestRank = columns[30];
        String commonOrderRank = columns[31];
        String hl7AttachmentStructure = columns[32];
        String extCopyrightLink = columns[33];
        String panelType = columns[34];
        String askAtOrderEntry = columns[35];
        String associatedObservations = columns[36];
        String versionFirstReleased = columns[37];
        String validHl7AttachmentRequest = columns[38];
        String displayName = columns[39];

        UUID id = uuid(columns[0]);
        StateSet active = (columns[11].equals("ACTIVE")) ? StateSet.ACTIVE : StateSet.INACTIVE;
        long effectiveDate = System.currentTimeMillis();
        StampPositionRecord stampPosition = StampPositionRecord.make(effectiveDate, TinkarTerm.MASTER_PATH.nid());
        // options for retrieving StampCalculators
        StampCalculator stampCalc = StampCoordinateRecord.make(active, stampPosition).stampCalculator();
        StampCalculator stampCalc2 = Calculators.Stamp.MasterLatest();
        StampCalculator stampCalc3 = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster(), IntIds.set.empty()));
        StampCalculator stampCalc4 = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));

        ConceptRecord entity = EntityService.get().getEntityFast(id);
        Latest<ConceptVersionRecord> latest = stampCalc4.latest(entity);

        return latest.isPresent();
    }
}
