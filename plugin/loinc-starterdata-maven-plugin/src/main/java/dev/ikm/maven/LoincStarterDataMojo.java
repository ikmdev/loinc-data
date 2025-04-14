package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.assembler.PatternAssembler;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedNavigation;
import dev.ikm.tinkar.composer.template.USDialect;
import dev.ikm.tinkar.composer.template.Definition;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.composer.template.AxiomSyntax;

import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;

import java.io.File;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.parser.Entity;

import static dev.ikm.tinkar.terms.TinkarTerm.PHENOMENON;
import static dev.ikm.tinkar.terms.TinkarTerm.USER;
import static dev.ikm.tinkar.terms.TinkarTerm.IDENTIFIER_SOURCE;
import static dev.ikm.tinkar.terms.TinkarTerm.STATUS_VALUE;
import static dev.ikm.tinkar.terms.TinkarTerm.MODULE;
import static dev.ikm.tinkar.terms.TinkarTerm.COMPONENT_FIELD;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.MEMBERSHIP_SEMANTIC;
import static dev.ikm.tinkar.terms.TinkarTerm.STRING;
import static dev.ikm.tinkar.terms.TinkarTerm.PREFERRED;
import static dev.ikm.tinkar.terms.TinkarTerm.INTEGER_FIELD;
import static dev.ikm.tinkar.terms.TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER;

@Mojo(name = "run-loinc-starterdata", defaultPhase = LifecyclePhase.INSTALL)
public class LoincStarterDataMojo extends AbstractMojo {

    private static final long STAMP_TIME = System.currentTimeMillis();

    private final String loincAuthorStr = "Regenstrief Institute, Inc. Author";
    private final EntityProxy.Concept loincAuthor = makeConceptProxy(loincAuthorStr);

    @Parameter(property = "origin.namespace", required = true)
    String namespaceString;
    @Parameter(property = "datastorePath", required = true)
    private String datastorePath;
    @Parameter(property = "controllerName", defaultValue = "Open SpinedArrayStore")
    private String controllerName;

    private UUID namespace;

    public static final String LOINC_TRIAL_STATUS_PATTERN = "LOINC Trial Status Pattern";
    public static final String LOINC_DISCOURAGED_STATUS_PATTERN = "LOINC Discouraged Status Pattern";
    public static final String LOINC_CLASS_PATTERN = "LOINC Class Pattern";
    public static final String EXAMPLE_UCUM_UNITS_PATTERN = "Example UCUM Units Pattern";
    public static final String TEST_REPORTABLE_MEMBERSHIP_PATTERN = "Test Reportable Membership Pattern";
    public static final String TEST_SUBSET_MEMBERSHIP_PATTERN = "Test Subset Membership Pattern";
    public static final String TEST_ORDERABLE_MEMBERSHIP_PATTERN = "Test Orderable Membership Pattern";

    public static final String ATTRIBUTE_ID = "3af1c784-8a62-59e5-82e7-767de930843b";

    public void execute() throws MojoExecutionException {
        this.namespace = UUID.fromString(namespaceString);
        try {
            init();
            transform();
            cleanup();
        }
        catch (Exception e) {
            throw new MojoExecutionException("Failed to execute class", e);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(LoincStarterDataMojo.class);

    private final Composer composer = new Composer("LOINC");

    private void init() {
        File datastore = new File(datastorePath);
        LOG.info("Starting database");
        LOG.info("Loading data from {}", datastore.getAbsolutePath());
        CachingService.clearAll();
        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, datastore);
        PrimitiveData.selectControllerByName("Open SpinedArrayStore");
        PrimitiveData.start();
    }

    private void cleanup() {
        PrimitiveData.stop();
    }

    void transform() {
        EntityService.get().beginLoadPhase();
        try {
            createLoincAuthor();

            Session session = composer.open(State.ACTIVE, STAMP_TIME, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
            createConcepts(session);
            createPatterns(session);


            composer.commitAllSessions();
        } finally {
            EntityService.get().endLoadPhase();
        }
    }

    private void createLoincAuthor() {
        Session session = composer.open(State.ACTIVE, STAMP_TIME, TinkarTerm.USER, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
        createConcept(session, loincAuthorStr, "LOINC Author",
                "Regenstrief Institute, Inc. Author - The entity responsible for publishing LOINC",
                loincAuthor, USER);
    }

    private void createConcepts(Session session) {
        String obsEntityStr = "Observable Entity";
        createConcept(session, obsEntityStr, null, makeConceptProxy(obsEntityStr), PHENOMENON);

        String loincNumber = "LOINC Number";
        createConcept(session, loincNumber, "LOINC Code", "The unique LOINC Code is a string in the format of nnnnnnnn-n.", makeConceptProxy(loincNumber), IDENTIFIER_SOURCE);

        String attributeStr = "Attribute";
        EntityProxy.Concept attribute = EntityProxy.Concept.make(attributeStr, PublicIds.of(UUID.fromString(ATTRIBUTE_ID), UuidT5Generator.get(namespace, attributeStr))); // Force UUID of Concept Model Object Attribute)
//        EntityProxy.Concept attribute = makeConceptProxy(attributeStr);

        String componentStr = "Component";
        EntityProxy.Concept component = makeConceptProxy(componentStr);
        createConcept(session, componentStr, "First major axis-component or analyte", component, attribute);

        String propertyStr = "Property";
        EntityProxy.Concept property = makeConceptProxy(propertyStr);
        createConcept(session, propertyStr, "Second major axis-property observed (e.g., mass vs. substance)", property, attribute);

        String timeAspectStr = "Time Aspect";
        EntityProxy.Concept timeAspect = makeConceptProxy(timeAspectStr);
        createConcept(session, timeAspectStr, "Third major axis-timing of the measurement (e.g., point in time vs 24 hours)", timeAspect, attribute);

        String systemStr = "System";
        EntityProxy.Concept system = makeConceptProxy(systemStr);
        createConcept(session, systemStr, "Fourth major axis-type of specimen or system (e.g., serum vs urine)", system, attribute);

        String scaleStr = "Scale";
        EntityProxy.Concept scale = makeConceptProxy(scaleStr);
        createConcept(session, scaleStr, "Fifth major axis-scale of measurement (e.g., qualitative vs. quantitative)", scale, attribute);

        String methodStr = "Method";
        EntityProxy.Concept method = makeConceptProxy(methodStr);
        createConcept(session, methodStr, "Sixth major axis-method of measurement", method, attribute);

        String orderVObsStr = "Order Vs Observation";
        EntityProxy.Concept orderVObs = makeConceptProxy(orderVObsStr);

        String orderableStr = "Test Orderable";
        EntityProxy.Concept orderable = makeConceptProxy(orderableStr);
        createConcept(session, orderableStr, "Defines term as order only. We have defined them " +
                "only to make it easier to maintain panels or other sets within the LOINC construct. This field reflects " +
                "our best approximation of the terms intended use; it is not to be considered normative or a binding " +
                "resolution.", orderable, orderVObs);

        String reportableStr = "Test Reportable";
        EntityProxy.Concept reportable = makeConceptProxy(reportableStr);
        createConcept(session, reportableStr, "Defines term as observation only. We have defined " +
                "them only to make it easier to maintain panels or other sets within the LOINC construct. This field " +
                "reflects our best approximation of the terms intended use; it is not to be considered normative or a " +
                "binding resolution.", reportable, orderVObs);

        String testSubsetStr = "Test Subset";
        EntityProxy.Concept testSubset = makeConceptProxy(testSubsetStr);
        createConcept(session, testSubsetStr, "Subset, is used for terms that are subsets of a " +
                        "panel but do not represent a package that is known to be orderable. We have defined them only to make " +
                        "it easier to maintain panels or other sets within the LOINC construct. This field reflects our best " +
                        "approximation of the terms intended use; it is not to be considered normative or a binding resolution.",
                testSubset, orderVObs);

        createConcept(session, orderVObsStr,
                "Defines term as order only, observation only, or both. A fourth category, Subset, is used for terms that " +
                        "are subsets of a panel but do not represent a package that is known to be orderable. We have " +
                        "defined them only to make it easier to maintain panels or other sets within the LOINC construct. " +
                        "This field reflects our best approximation of the terms intended use; it is not to be considered " +
                        "normative or a binding resolution.", orderVObs, attribute, orderable, reportable, testSubset);

        String loincClassStr = "LOINC Class";
        EntityProxy.Concept loincClass = makeConceptProxy(loincClassStr);
        createConcept(session, loincClassStr, "An arbitrary classification of the terms " +
                "for grouping related observations together.", loincClass, attribute);

        String loincClassTypeStr = "LOINC ClassType";
        EntityProxy.Concept loincClassType = makeConceptProxy(loincClassTypeStr);
        createConcept(session, loincClassTypeStr,
                "1=Laboratory class; 2=Clinical class; 3=Claims attachments; 4=Surveys",
                loincClassType, attribute);

        createConcept(session, attributeStr, null, attribute, PHENOMENON, component, property,
                timeAspect, system, scale, method, orderVObs, loincClass, loincClassType);

        String trialStatusStr = "Trial Status";
        EntityProxy.Concept trialStatus = makeConceptProxy(trialStatusStr);
        createConcept(session, trialStatusStr, "Concept is experimental in nature. Use with caution as the concept " +
                "and associated attributes may change.", trialStatus, STATUS_VALUE);

        String discouragedStatusStr = "Discouraged Status";
        EntityProxy.Concept discouragedStatus = makeConceptProxy(discouragedStatusStr);
        createConcept(session, discouragedStatusStr, "Concept is not recommended for current use. New mappings " +
                        "to this concept are discouraged; although existing may mappings may continue to be valid in context.",
                discouragedStatus, STATUS_VALUE);

        String loincModuleStr = "LOINC Module";
        EntityProxy.Concept loincModule = makeConceptProxy(loincModuleStr);
        createConcept(session, loincModuleStr, "LOINC Core Module", "Module responsible for LOINC",
                loincModule, MODULE);

        String exampleUnitsStr = "Example Units (UCUM)";
        EntityProxy.Concept exampleUnits = makeConceptProxy(exampleUnitsStr);
        createConcept(session, exampleUnitsStr, "The Unified Code for Units of Measure (UCUM) is a code system intended to include all units of measures being contemporarily used in international science, engineering, and business. (www.unitsofmeasure.org) This field contains example units of measures for this term expressed as UCUM units.",
                exampleUnits, PHENOMENON);
    }

    private EntityProxy.Concept makeConceptProxy(String description) {
        return EntityProxy.Concept.make(description, UuidT5Generator.get(namespace, description));
    }

    private EntityProxy.Pattern makePatternProxy(String description) {
        return EntityProxy.Pattern.make(description, UuidT5Generator.get(namespace, description));
    }

    private void createConcept(Session session, String fullyQualifiedName, String definition,
                               EntityProxy.Concept identifier, EntityProxy.Concept parent, EntityProxy.Concept... children) {

        createConcept(session, fullyQualifiedName, null, definition, identifier, parent, children);
    }

    private void createConcept(Session session, String fullyQualifiedName, String synonym, String definition,
                               EntityProxy.Concept identifier, EntityProxy.Concept parent, EntityProxy.Concept... children) {

        session.compose((ConceptAssembler conceptAssembler) -> {
                    conceptAssembler.concept(identifier)
                            .attach((FullyQualifiedName fqn) -> fqn
                                    .language(ENGLISH_LANGUAGE)
                                    .text(fullyQualifiedName)
                                    .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                    .attach(usDialect()))
                            .attach((Identifier id) -> id
                                    .source(UNIVERSALLY_UNIQUE_IDENTIFIER)
                                    .identifier(identifier.asUuidArray()[0].toString()))
                            .attach(new StatedNavigation()
                                    .parents(parent)
                                    .children(children));
                    if(parent.publicId().contains(UUID.fromString(ATTRIBUTE_ID))){
                        conceptAssembler.attach((AxiomSyntax owlAxiom) -> owlAxiom
                                .text(String.format("SubObjectPropertyOf(:[%s] :[%s])", identifier.asUuidArray()[0], parent.asUuidArray()[0])));
                    } else {
                        conceptAssembler.attach(new StatedAxiom()
                                .isA(parent));
                    }
                    if (synonym != null) {
                        conceptAssembler.attach((Synonym syn) -> syn
                                .language(ENGLISH_LANGUAGE)
                                .text(synonym)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()));
                    }
                    if (definition != null) {
                        conceptAssembler.attach((Definition defn) -> defn
                                .language(ENGLISH_LANGUAGE)
                                .text(definition)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE)
                                .attach(usDialect()));
                    }
                }
        );
    }

    private void createPatterns(Session session) {

        String trialStatusStr = "Trial Status";
        EntityProxy.Concept trialStatus = makeConceptProxy(trialStatusStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(LOINC_TRIAL_STATUS_PATTERN))
                        .meaning(trialStatus)
                        .purpose(STATUS_VALUE)
                        .fieldDefinition(trialStatus, STATUS_VALUE, STRING))
                        .attach((FullyQualifiedName fqn) -> fqn
                                .text("Trial Status Pattern")
                                .language(ENGLISH_LANGUAGE)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                        .attach((Synonym synonym) -> synonym
                                .text("Trial Status Pattern")
                                .language(ENGLISH_LANGUAGE)
                                .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String discouragedStatusStr = "Discouraged Status";
        EntityProxy.Concept discouragedStatus = makeConceptProxy(discouragedStatusStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(LOINC_DISCOURAGED_STATUS_PATTERN))
                .meaning(discouragedStatus)
                .purpose(STATUS_VALUE)
                        .fieldDefinition(discouragedStatus, STATUS_VALUE, STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Discouraged Status Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Discouraged Status Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String loincClassStr = "LOINC Class";
        EntityProxy.Concept loincClass = makeConceptProxy(loincClassStr);
        String loincClassTypeStr = "LOINC ClassType";
        EntityProxy.Concept loincClassType = makeConceptProxy(loincClassTypeStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(LOINC_CLASS_PATTERN))
                        .meaning(loincClass)
                        .purpose(loincClass)
                        .fieldDefinition(
                                loincClass,
                                loincClass,
                                COMPONENT_FIELD)
                        .fieldDefinition(
                                loincClassType,
                                loincClassType,
                                INTEGER_FIELD))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("LOINC ClassType Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("LOINC ClassType Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String exampleUnitsStr = "Example Units (UCUM)";
        EntityProxy.Concept exampleUnits = makeConceptProxy(exampleUnitsStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(EXAMPLE_UCUM_UNITS_PATTERN))
                .meaning(exampleUnits)
                .purpose(exampleUnits)
                .fieldDefinition(
                        exampleUnits,
                        exampleUnits,
                        STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Example Units (UCUM) Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Example Units (UCUM) Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));;

        String orderableStr = "Test Orderable";
        EntityProxy.Concept orderable = makeConceptProxy(orderableStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(TEST_ORDERABLE_MEMBERSHIP_PATTERN))
                .meaning(orderable)
                .purpose(MEMBERSHIP_SEMANTIC).fieldDefinition(orderable, MEMBERSHIP_SEMANTIC, STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Test Orderable Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Test Orderable Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String reportableStr = "Test Reportable";
        EntityProxy.Concept reportable = makeConceptProxy(reportableStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(TEST_REPORTABLE_MEMBERSHIP_PATTERN))
                .meaning(reportable)
                .purpose(MEMBERSHIP_SEMANTIC).fieldDefinition(reportable, MEMBERSHIP_SEMANTIC, STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Test Reportable Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Test Reportable Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

        String testSubsetStr = "Test Subset";
        EntityProxy.Concept testSubset = makeConceptProxy(testSubsetStr);
        session.compose((PatternAssembler patternAssembler) -> patternAssembler.pattern(makePatternProxy(TEST_SUBSET_MEMBERSHIP_PATTERN))
                .meaning(testSubset)
                .purpose(MEMBERSHIP_SEMANTIC).fieldDefinition(testSubset, MEMBERSHIP_SEMANTIC, STRING))
                .attach((FullyQualifiedName fqn) -> fqn
                        .text("Test Subset Membership Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE))
                .attach((Synonym synonym) -> synonym
                        .text("Test Subset Membership Pattern")
                        .language(ENGLISH_LANGUAGE)
                        .caseSignificance(DESCRIPTION_NOT_CASE_SENSITIVE));

    }

    private USDialect usDialect() {
        return new USDialect().acceptability(PREFERRED);
    }
}