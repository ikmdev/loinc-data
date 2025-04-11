package dev.ikm.tinkar.loinc.integration;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.component.Component;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.Coordinates;
import dev.ikm.tinkar.coordinate.stamp.StampCoordinateRecord;
import dev.ikm.tinkar.coordinate.stamp.StateSet;
import dev.ikm.tinkar.coordinate.stamp.calculator.Latest;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculator;
import dev.ikm.tinkar.coordinate.stamp.calculator.StampCalculatorWithCache;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.PatternEntityVersion;
import dev.ikm.tinkar.entity.SemanticEntityVersion;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.TinkarTerm;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincDefinitionPartSemanticIT extends LoincAbstractIntegrationTest {
	
	/**
	 * Test LoincDefinitionPart Loinc.csv Semantics.
	 *
	 * @result Reads content from file and validates Concept of Semantics by calling
	 *         private method assertConcept().
	 */
	@Test
	public void testLoincDefinitionPartSemantics() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/PartCsv_not_found.txt";

        String absolutePath = findFilePath(sourceFilePath, "Part.csv");
        int notFound = processPartFile(absolutePath, errorFile);

        assertEquals(0, notFound, "Unable to find " + notFound + " Part.csv semantics. Details written to " + errorFile);
	}
	
	private class ConceptMapValue { 
		Concept conceptDescType;
		String term;
		
		ConceptMapValue(Concept conceptDescType, String term) {
			this.conceptDescType = conceptDescType;
			this.term = term;
		}
	}
	
    @Override
    protected boolean assertLine(String[] columns) {
        UUID id = uuid(columns[0]);
                
		EntityProxy.Concept concept;
		concept = EntityProxy.Concept.make(PublicIds.of(id));

		String partName = removeQuotes(columns[2]);
		String partDisplayName = removeQuotes(columns[3]);
			
		final StateSet active;
		if (columns[4].equals("ACTIVE") || columns[4].equals("TRIAL") || columns[4].equals("DISCOURAGED")) {
			active = StateSet.ACTIVE;
		} else {
			active = StateSet.INACTIVE;
		}

		AtomicBoolean matched = new AtomicBoolean(true);
		AtomicInteger innerCount = new AtomicInteger(0);
		
		// Create description semantics for non-empty fields
		// FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE; // decriptionType for PartDisplayName
		// REGULAR_NAME_DESCRIPTION_TYPE;         // decriptionType for PartName
		
		Map<String, ConceptMapValue> conceptMap = new HashMap();
		if (!partName.isEmpty()) {
			conceptMap.put("REGULAR_NAME_DESCRIPTION_TYPE", getConceptMapValue(REGULAR_NAME_DESCRIPTION_TYPE, partName));
		}	
		
		if (!partDisplayName.isEmpty()) {
			conceptMap.put("FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE", getConceptMapValue(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE, partDisplayName));			
		}
		
		StampCalculator stampCalc = StampCalculatorWithCache
				.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));
		
		PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.DESCRIPTION_PATTERN).get();
        
		EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.DESCRIPTION_PATTERN.nid(), semanticEntity -> {
			innerCount.incrementAndGet();
			
			Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
			
			ConceptMapValue regularNameConceptMapValue = conceptMap.get("REGULAR_NAME_DESCRIPTION_TYPE");
			ConceptMapValue fullNameConceptMapValue = conceptMap.get("FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE");
			
			
			if(regularNameConceptMapValue != null && fullNameConceptMapValue != null) {
				if (latest.isPresent()) {
					
					Component descriptionType = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.DESCRIPTION_TYPE,
							latest.get());
												
					Component caseSensitivity = latestDescriptionPattern
							.getFieldWithMeaning(TinkarTerm.DESCRIPTION_CASE_SIGNIFICANCE, latest.get());
					
					String text = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.TEXT_FOR_DESCRIPTION, latest.get());
														
					if ( (!descriptionType.equals(regularNameConceptMapValue.conceptDescType) || !caseSensitivity.equals(DESCRIPTION_NOT_CASE_SENSITIVE) || !text.equals(regularNameConceptMapValue.term)) 
							&& (!descriptionType.equals(fullNameConceptMapValue.conceptDescType) || !caseSensitivity.equals(DESCRIPTION_NOT_CASE_SENSITIVE) || !text.equals(fullNameConceptMapValue.term))) {
						matched.set(false);
					}	
				} else {
					matched.set(false);
				}
			} else {
				matched.set(false);
			}
		});	
		
		innerCount.set(0);
		return matched.get();
    }
    
	private String removeQuotes(String column) {
		return column.replaceAll("^\"|\"$", "").trim();
	}
	
	private ConceptMapValue getConceptMapValue(Concept conceptDescType, String term) {
		return new ConceptMapValue(conceptDescType, term);
	}
}
