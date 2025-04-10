package dev.ikm.tinkar.loinc.integration;

import dev.ikm.maven.LoincUtility;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.ConceptRecord;
import dev.ikm.tinkar.entity.ConceptVersionRecord;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.PatternEntityVersion;
import dev.ikm.tinkar.entity.SemanticRecord;
import dev.ikm.tinkar.entity.SemanticVersionRecord;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincAxiomSemanticIT extends LoincAbstractIntegrationTest {

    /**
     * Test LoincAxiom Loinc.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertConcept().
     */
//    @Test
    public void testLoincAxiomSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/LoincCsv_axioms_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
        int notFound = processLoincFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Loinc.csv 'Axiom' semantics. Details written to " + errorFile);
    }

    @Override
    protected boolean assertLine(String[] columns) {
        String loincNum = columns[0]; // LOINC_NUM
        UUID id = uuid(loincNum);

        StateSet state = null;
        if (columns[11].equals("ACTIVE") || columns[11].equals("TRIAL") || columns[11].equals("DISCOURAGED")) {
            state = StateSet.ACTIVE;
        } else {
            state = StateSet.INACTIVE;
        }

        String component = removeQuotes(columns[1]); // COMPONENT
        String property = removeQuotes(columns[2]); // PROPERTY
        String timeAspc = removeQuotes(columns[3]); // TIME_ASPCT
        String system = removeQuotes(columns[4]); // SYSTEM
        String scaleType = removeQuotes(columns[5]); // SCALE_TYP
        String methodType = removeQuotes(columns[6]); // METHOD_TYP

        StampCalculator stampCalc = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(state, Coordinates.Position.LatestOnDevelopment()));
        ConceptRecord entity = EntityService.get().getEntityFast(id);

        // To work with a SemanticRecord, we need to obtain the proper id (check code on the TransformationMojo)
        // concept.publicId().asUuidArray()[0] + partTypeName + "AXIOM")
//        SemanticRecord entity = EntityService.get().getEntityFast(id);

        if (entity != null) {
            PatternEntityVersion pattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest().latest(TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN).get();
            Latest<ConceptVersionRecord> latest = stampCalc.latest(entity);
//            Latest<SemanticVersionRecord> latest = stampCalc.latest(entity);

//            String fieldValue = pattern.getFieldWithMeaning(TinkarTerm.AXIOM_SYNTAX, latest.get());
            String owlAxiomStr = LoincUtility.buildOwlExpression(UuidUtil.SNOMED_NAMESPACE, loincNum, component, property, timeAspc, system, scaleType, methodType);
//            return latest.isPresent() && fieldValue.equals(owlAxiomStr);
            return latest.isPresent() && latest.get().entity().entityToString().equals(owlAxiomStr);
        }

        return false;

//        return latest.isPresent();
    }
}
