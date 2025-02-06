package dev.ikm.maven;

import dev.ikm.tinkar.composer.Composer;
import dev.ikm.tinkar.composer.Session;
import dev.ikm.tinkar.composer.assembler.ConceptAssembler;
import dev.ikm.tinkar.composer.template.Definition;
import dev.ikm.tinkar.composer.template.FullyQualifiedName;
import dev.ikm.tinkar.composer.template.Identifier;
import dev.ikm.tinkar.composer.template.StatedAxiom;
import dev.ikm.tinkar.composer.template.StatedNavigation;
import dev.ikm.tinkar.composer.template.Synonym;
import dev.ikm.tinkar.composer.template.USDialect;
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

import java.io.File;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dev.ikm.tinkar.terms.TinkarTerm.CASE_SENSITIVE_EVALUATION;
import static dev.ikm.tinkar.terms.TinkarTerm.DESCRIPTION_NOT_CASE_SENSITIVE;
import static dev.ikm.tinkar.terms.TinkarTerm.ENGLISH_LANGUAGE;
import static dev.ikm.tinkar.terms.TinkarTerm.IDENTIFIER_SOURCE;
import static dev.ikm.tinkar.terms.TinkarTerm.MODULE;
import static dev.ikm.tinkar.terms.TinkarTerm.PHENOMENON;
import static dev.ikm.tinkar.terms.TinkarTerm.PREFERRED;
import static dev.ikm.tinkar.terms.TinkarTerm.STATUS_VALUE;
import static dev.ikm.tinkar.terms.TinkarTerm.UNIVERSALLY_UNIQUE_IDENTIFIER;
import static dev.ikm.tinkar.terms.TinkarTerm.USER;

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

    public void execute() throws MojoExecutionException {
        init();
        transform();
        cleanup();
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
        createLoincAuthor();

        Session session = composer.open(State.ACTIVE, STAMP_TIME, loincAuthor, TinkarTerm.PRIMORDIAL_MODULE, TinkarTerm.PRIMORDIAL_PATH);
        createConcepts(session);

        composer.commitAllSessions();
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
        EntityProxy.Concept attribute = makeConceptProxy(attributeStr);

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
    }

    private EntityProxy.Concept makeConceptProxy(String description) {
        return EntityProxy.Concept.make(description, UUID.nameUUIDFromBytes(description.getBytes()));
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
                                    .children(children))
                            .attach(new StatedAxiom()
                                    .isA(parent));
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
                                .caseSignificance(CASE_SENSITIVE_EVALUATION)
                                .attach(usDialect()));
                    }
                }
        );
    }

    private USDialect usDialect() {
        return new USDialect().acceptability(PREFERRED);
    }
}