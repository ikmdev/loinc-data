package dev.ikm.tinkar.loinc.integration;

import dev.ikm.maven.LoincUtility;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
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
import dev.ikm.tinkar.entity.graph.DiTreeEntity;
import dev.ikm.tinkar.entity.graph.EntityVertex;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testAxiomSemanticsPart() throws IOException {
        String sourceFilePath = "../loinc-origin/target/origin-sources";
        String errorFile = "target/failsafe-reports/PartCsv_descriptions_not_found.txt";

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
    protected boolean assertLine(String[] columns) {
    	if(isPart) {
            UUID id = uuid(columns[0]);

    		String partNumber = removeQuotes(columns[0]);
    		String partTypeName = removeQuotes(columns[1]);
    		String partName = removeQuotes(columns[2]);
    			
    		final StateSet active;
    		if (columns[4].equals("ACTIVE") || columns[4].equals("TRIAL") || columns[4].equals("DISCOURAGED")) {
    			active = StateSet.ACTIVE;
    		} else {
    			active = StateSet.INACTIVE;
    		}

    		AtomicBoolean matched = new AtomicBoolean(true);
    		AtomicInteger innerCount = new AtomicInteger(0);

    		UUID conceptUuid = UuidT5Generator.get(UUID.fromString(namespaceString), partNumber);
    		EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(conceptUuid));
    		EntityProxy.Concept parentConcept = LoincUtility.getParentForPartType(UUID.fromString(namespaceString), partTypeName);
    		
    		if (!partName.isEmpty() && !partTypeName.isEmpty() && !partNumber.isEmpty()) {
    			LoincUtility.addPartToCache(partName.toLowerCase(), partTypeName, partNumber);	
    		}
    		
	        StampCalculator stampCalc = StampCalculatorWithCache.getCalculator(StampCoordinateRecord.make(active, Coordinates.Position.LatestOnDevelopment()));
	        
			PatternEntityVersion latestAxiomPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(TinkarTerm.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN).get();
	
			EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.nid(), semanticEntity -> {
				
				Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
    			innerCount.incrementAndGet();
    			
    			if(latest.isPresent()) {		
    				if (parentConcept != null) { 
    					DiTreeEntity fieldValue = latestAxiomPattern.getFieldWithMeaning(TinkarTerm.EL_PLUS_PLUS_STATED_TERMINOLOGICAL_AXIOMS, latest.get());
    					
    					EntityVertex vertex = fieldValue.firstVertexWithMeaning(TinkarTerm.CONCEPT_REFERENCE).get();
    					if (!vertex.properties().containsValue(parentConcept)) {
    						matched.set(false);
    					}
    				}
    			} else {
    				matched.set(false);
    			}
    		});	
 
    		return matched.get() && innerCount.get() == 1;   		
    	} else {
    	
	        String loincNum = columns[0]; // LOINC_NUM
	        UUID id = uuid(loincNum);
	
	        final StateSet state;
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
	        
			PatternEntityVersion latestAxiomPattern = (PatternEntityVersion) Calculators.Stamp.DevelopmentLatest()
					.latest(TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN).get();
	        
	        EntityProxy.Concept concept = EntityProxy.Concept.make(PublicIds.of(id));
	
	        AtomicBoolean matched = new AtomicBoolean(true);
	        AtomicInteger innerCount = new AtomicInteger(0);
	        
			EntityService.get().forEachSemanticForComponentOfPattern(concept.nid(), TinkarTerm.OWL_AXIOM_SYNTAX_PATTERN.nid(), semanticEntity -> {
				
				Latest<SemanticEntityVersion> latest = stampCalc.latest(semanticEntity);
				
				if (latest.isPresent()) {
					String fieldValue = latestAxiomPattern.getFieldWithMeaning(TinkarTerm.AXIOM_SYNTAX, latest.get());
					String owlAxiomStr = LoincUtility.buildOwlExpression(UUID.fromString(namespaceString), loincNum, component, property, timeAspc, system, scaleType, methodType);
					
					if(owlAxiomStr != null) {
						innerCount.incrementAndGet();
					}
					
					if (!fieldValue.equals(owlAxiomStr)) {
						matched.set(false);
					} 
				} else {
					matched.set(false);
				}
			});	    		
	
	        return matched.get() && innerCount.get() == 1;
    	}
    }
}
