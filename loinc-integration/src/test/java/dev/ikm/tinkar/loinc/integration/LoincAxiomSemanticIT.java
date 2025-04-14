package dev.ikm.tinkar.loinc.integration;

import dev.ikm.maven.LoincUtility;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.component.Component;
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
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.entity.SemanticRecord;
import dev.ikm.tinkar.entity.SemanticVersionRecord;
import dev.ikm.tinkar.loinc.integration.LoincAbstractIntegrationTest.ConceptMapValue;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(Lifecycle.PER_CLASS)
public class LoincAxiomSemanticIT extends LoincAbstractIntegrationTest {

    /**
     * Test PartAxiom Part.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertLinePart().
     */	
	@BeforeAll
    @Test
    public void testLoincAxiomSemanticsPart() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/PartCsv_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Part.csv");
        int notFound = processPartFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Part.csv semantics. Details written to " + errorFile);
    }
	
    /**
     * Test LoincAxiom Loinc.csv Semantics.
     *
     * @result Reads content from file and validates Concept of Semantics by calling private method assertConcept().
     */
    @Test
    public void testLoincAxiomSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/LoincCsv_axioms_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
        int notFound = processLoincFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Loinc.csv 'Axiom' semantics. Details written to " + errorFile);
    }
    
    @Override
    protected boolean assertLinePart(String[] columns) {
        UUID id = uuid(columns[0]);
        
		Map<UUID, ConceptMapValue> termConceptMap = new HashMap<>(); 
        
		EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(id));

		String partNumber = removeQuotes(columns[0]);
		String partTypeName = removeQuotes(columns[1]);
		String partName = removeQuotes(columns[2]);
	
		LoincUtility.addPartToCache(partName.toLowerCase(), partTypeName, partNumber);

		UUID uuidAxiom = uuid((concept.publicId().asUuidArray()[0] + partTypeName + "AXIOM"));
	
		final StateSet active;
		if (columns[4].equals("ACTIVE") || columns[4].equals("TRIAL") || columns[4].equals("DISCOURAGED")) {
			active = StateSet.ACTIVE;
		} else {
			active = StateSet.INACTIVE;
		}
		
		AtomicBoolean matched = new AtomicBoolean(false);
		
		StampCalculator stampCalc = StampCalculatorWithCache
				.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));
	
		PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.DESCRIPTION_PATTERN).get();
        
		// NOT SURE if this part (forEach loop) is necessary since semanticEntity might not be needed for IT's logic
		EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.DESCRIPTION_PATTERN.nid(), semanticEntity -> {
			
			Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
			UUID semanticEntityUUID = semanticEntity.asUuidArray()[0];
			
			if(latest.isPresent())  {
				matched.set(true);
			} 
		});	
		
		return matched.get();
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
        
		PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN).get();
        
        EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(id));

        AtomicBoolean matched = new AtomicBoolean(true);
        
		EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN.nid(), semanticEntity -> {
			
			Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
			
			if (latest.isPresent()) {

				String fieldValue = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.AXIOM_SYNTAX, latest.get());
				    
				String owlAxiomStr = LoincUtility.buildOwlExpression(UUID.fromString(namespaceString), loincNum, component, property, timeAspc, system, scaleType, methodType);
				
				if (!latest.isPresent() || !fieldValue.equals(owlAxiomStr)) {
					matched.set(false);
				}
			}
		});	    		

        return matched.get();
    }
}
