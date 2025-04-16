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

import static dev.ikm.tinkar.terms.TinkarTerm.DEFINITION_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincDescriptionSemanticIT extends LoincAbstractIntegrationTest {
	
	/**
	 * Test LoincDescription Loinc.csv Semantics.
	 *
	 * @result Reads content from file and validates Concept of Semantics by calling
	 *         private method assertConcept().
	 */
	@Test
	public void testLoincDescriptionSemantics() throws IOException {
		String sourceFilePath = "../loinc-origin/target/origin-sources";
		String errorFile = "target/failsafe-reports/LoincCsv_descriptions_not_found.txt";

		String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
		int notFound = processLoincFile(absolutePath, errorFile);
		
		assertEquals(0, notFound,
				"Unable to find " + notFound + " Loinc.csv 'Description' semantics. Details written to " + errorFile);
	}

	@Override
	protected boolean assertLine(String[] columns) {
		Map<UUID, ConceptMapValue> termConceptMap = new HashMap<>();
		
		UUID id = uuid(columns[0]);

		AtomicInteger innerCount = new AtomicInteger(0);
		
		String longCommonName = removeQuotes(columns[25]);
		String consumerName = removeQuotes(columns[12]);
		String shortName = removeQuotes(columns[20]);
		String relatedNames2 = removeQuotes(columns[19]);
		String displayName = removeQuotes(columns[39]);
		String definitionDescription = removeQuotes(columns[10]);

		EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(id));

		// Create description semantics for non-empty fields
		if (!longCommonName.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, longCommonName, "FQN"), getConceptMapValue(FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE, longCommonName));
		}

		if (!consumerName.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, consumerName, "Regular"), getConceptMapValue(REGULAR_NAME_DESCRIPTION_TYPE, consumerName));
		}

		if (!shortName.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, shortName, "Regular"), getConceptMapValue(REGULAR_NAME_DESCRIPTION_TYPE, shortName));
		}

		if (!relatedNames2.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, relatedNames2, "Regular"), getConceptMapValue(REGULAR_NAME_DESCRIPTION_TYPE, relatedNames2));
		}

		if (!displayName.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, displayName, "Regular"), getConceptMapValue(REGULAR_NAME_DESCRIPTION_TYPE, displayName));
		}

		if (!definitionDescription.isEmpty()) {
			termConceptMap.put(getConceptMapKey(concept, definitionDescription, "Definition"), getConceptMapValue(DEFINITION_DESCRIPTION_TYPE, definitionDescription));
		}

		final StateSet active;
		if (columns[11].equals("ACTIVE") || columns[11].equals("TRIAL") || columns[11].equals("DISCOURAGED")) {
			active = StateSet.ACTIVE;
		} else {
			active = StateSet.INACTIVE;
		}

		AtomicBoolean matched = new AtomicBoolean(true);
		
		StampCalculator stampCalc = StampCalculatorWithCache
				.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));
		
		PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.DESCRIPTION_PATTERN).get();
		
		EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.DESCRIPTION_PATTERN.nid(), semanticEntity -> {
			innerCount.incrementAndGet();
			
			Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
			UUID semanticEntityUUID = semanticEntity.asUuidArray()[0];
			
			ConceptMapValue cmv = termConceptMap.get(semanticEntityUUID);
						
			if(cmv != null) {
				if (latest.isPresent()) {
					Component descriptionType = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.DESCRIPTION_TYPE,
							latest.get());
					
					Component caseSensitivity = latestDescriptionPattern
							.getFieldWithMeaning(TinkarTerm.DESCRIPTION_CASE_SIGNIFICANCE, latest.get());
					
					String text = latestDescriptionPattern.getFieldWithMeaning(TinkarTerm.TEXT_FOR_DESCRIPTION, latest.get());
											
					if (!descriptionType.equals(cmv.conceptDescType) || !caseSensitivity.equals(DESCRIPTION_NOT_CASE_SENSITIVE) 
								|| !text.equals(cmv.term)) {
		
						matched.set(false);
					}	
				}
			} 
		});
		
		if(innerCount.get() == termConceptMap.size()) {
			innerCount.set(0);
			return matched.get();
		} 
		
		innerCount.set(0);
		return false;
	}
	
	private ConceptMapValue getConceptMapValue(Concept conceptDescType, String term) {
		return new ConceptMapValue(conceptDescType, term);
	}
	
	private UUID getConceptMapKey(Concept concept, String term, String typeStr) {
		return uuid(concept.publicId().asUuidArray()[0] + term + typeStr + "DESC");
	}
}
