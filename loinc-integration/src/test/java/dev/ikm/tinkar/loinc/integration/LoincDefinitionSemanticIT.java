package dev.ikm.tinkar.loinc.integration;

import dev.ikm.maven.LoincUtility;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
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
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.EntityProxy.Concept;
import dev.ikm.tinkar.terms.TinkarTerm;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dev.ikm.tinkar.terms.TinkarTerm.DEFINITION_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
import static dev.ikm.tinkar.terms.TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LoincDefinitionSemanticIT extends LoincAbstractIntegrationTest {

	int count = 0;
	
	/**
	 * Test LoincDefinition Loinc.csv Semantics.
	 *
	 * @result Reads content from file and validates Concept of Semantics by calling
	 *         private method assertConcept().
	 */
	@Test
	public void testLoincDefinitionSemantics() throws IOException {
		String sourceFilePath = "../loinc-origin/target/origin-sources";
		String errorFile = "target/failsafe-reports/LoincCsv_definitions_not_found.txt";

		String absolutePath = findFilePath(sourceFilePath, "Loinc.csv");
		int notFound = processLoincFile(absolutePath, errorFile);

		System.out.println(" >>>> COUNT >>> " + count);
		
		assertEquals(0, notFound,
				"Unable to find " + notFound + " Loinc.csv 'Definition' semantics. Details written to " + errorFile);
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
		Map<UUID, ConceptMapValue> termConceptMap = new HashMap<>();
		
		UUID id = uuid(columns[0]);

		AtomicInteger innerCount = new AtomicInteger(0);
		
		String longCommonName = removeQuotes(columns[25]);
		String consumerName = removeQuotes(columns[12]);
		String shortName = removeQuotes(columns[20]);
		String relatedNames2 = removeQuotes(columns[19]);
		String displayName = removeQuotes(columns[39]);
		String definitionDescription = removeQuotes(columns[10]);
		
		/*
		 * TinkarTerm.DEFINITION_DESCRIPTION_TYPE;
		 * TinkarTerm.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
		 * TinkarTerm.REGULAR_NAME_DESCRIPTION_TYPE;
		 */

		// System.out.println(">>>> ID: " + columns[0]);
		EntityProxy.Concept concept;
		concept = EntityProxy.Concept.make(PublicIds.of(id));

		EntityProxy.Concept descType = null;
		String term = "";
	
		// Create description semantics for non-empty fields
		if (!longCommonName.isEmpty()) {
			descType = FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE;
			term = longCommonName;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		if (!consumerName.isEmpty()) {
			descType = REGULAR_NAME_DESCRIPTION_TYPE;
			term = consumerName;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		if (!shortName.isEmpty()) {
			descType = REGULAR_NAME_DESCRIPTION_TYPE;
			term = shortName;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		if (!relatedNames2.isEmpty()) {
			descType = REGULAR_NAME_DESCRIPTION_TYPE;
			term = relatedNames2;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		if (!displayName.isEmpty()) {
			descType = REGULAR_NAME_DESCRIPTION_TYPE;
			term = displayName;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		if (!definitionDescription.isEmpty()) {
			descType = DEFINITION_DESCRIPTION_TYPE;
			term = definitionDescription;
			
			termConceptMap.put(getConceptMapKey(concept, term), getConceptMapValue(descType, term));
		}

		StateSet active = null;
		if (columns[11].equals("ACTIVE") || columns[11].equals("TRIAL") || columns[11].equals("DISCOURAGED")) {
			active = StateSet.ACTIVE;
		} else {
			active = StateSet.INACTIVE;
		}

		AtomicBoolean matched = new AtomicBoolean(false);
		
		StampCalculator stampCalc = StampCalculatorWithCache
				.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnMaster()));
		
		PatternEntityVersion latestDescriptionPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
				.latest(TinkarTerm.DESCRIPTION_PATTERN).get();
		
		EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.DESCRIPTION_PATTERN.nid(), semanticEntity -> {
			count++;
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
					
					//if (PublicId.equals(semanticEntity.publicId(), TinkarTerm.DEFINITION_DESCRIPTION_TYPE.publicId())) {
						
					if (descriptionType.equals(cmv.conceptDescType) && caseSensitivity.equals(DESCRIPTION_NOT_CASE_SENSITIVE) 
								&& text.equals(cmv.term)) {
		
						matched.set(true);
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

	private boolean termConceptMapCheck(Map<UUID, ConceptMapValue> map, Component descriptionType, Component caseSensitivity, String text) {
		UUID uuid;
		ConceptMapValue conceptMapValue;
		EntityProxy.Concept caseSensitivityConcept = DESCRIPTION_NOT_CASE_SENSITIVE;
		
		for (Map.Entry<UUID, ConceptMapValue> entry : map.entrySet()) {
			uuid = entry.getKey();
			conceptMapValue = entry.getValue();

			if (!descriptionType.equals(conceptMapValue.conceptDescType) || !caseSensitivity.equals(caseSensitivityConcept) 
					|| !text.equals(conceptMapValue.term)) {
				return false;
			}
        }
		
		return true;	
	}
	
	private ConceptMapValue getConceptMapValue(Concept conceptDescType, String term) {
		return new ConceptMapValue(conceptDescType, term);
	}
	
	private UUID getConceptMapKey(Concept concept, String term) {
		return uuid(concept.publicId().asUuidArray()[0] + term + "DESC");
	}
	
	private String removeQuotes(String column) {
		return column.replaceAll("^\"|\"$", "").trim();
	}
}
