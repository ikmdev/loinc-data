package dev.ikm.maven;

import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;

import javax.swing.text.html.parser.Entity;
import java.util.UUID;

public class LoincUtility {
    private static final String loincAuthorStr = "Regenstrief Institute, Inc. Author";

    public static final String LOINC_TRIAL_STATUS_PATTERN = "LOINC Trial Status Pattern";
    public static final String LOINC_DISCOURAGED_STATUS_PATTERN = "LOINC Discouraged Status Pattern";
    public static final String LOINC_CLASS_PATTERN = "LOINC Class Pattern";
    public static final String EXAMPLE_UCUM_UNITS_PATTERN = "Example UCUM Units Pattern";
    public static final String TEST_REPORTABLE_MEMBERSHIP_PATTERN = "Test Reportable Membership Pattern";
    public static final String TEST_SUBSET_MEMBERSHIP_PATTERN = "Test Subset Membership Pattern";
    public static final String TEST_ORDERABLE_MEMBERSHIP_PATTERN = "Test Orderable Membership Pattern";

    /**
     * retrieves user concept
     * @return the snomed author
     */
    public static EntityProxy.Concept getAuthorConcept(UUID namespace){
        EntityProxy.Concept loincAuthor = EntityProxy.Concept.make(loincAuthorStr, UuidT5Generator.get(namespace,(loincAuthorStr)));
        return loincAuthor;
    }

    public static EntityProxy.Pattern getLoincTrialStatusPattern(UUID namespace){
        return makePatternProxy(namespace, LOINC_TRIAL_STATUS_PATTERN);
    }

    public static EntityProxy.Pattern getLoincDiscouragedPattern(UUID namespace){
        return makePatternProxy(namespace, LOINC_DISCOURAGED_STATUS_PATTERN);
    }

    public static EntityProxy.Pattern getLoincClassPattern(UUID namespace){
        return makePatternProxy(namespace, LOINC_CLASS_PATTERN);
    }

    public static EntityProxy.Pattern getExampleUnitsPattern(UUID namespace){
        return makePatternProxy(namespace, EXAMPLE_UCUM_UNITS_PATTERN);
    }

    public static EntityProxy.Pattern getTestReportablePattern(UUID namespace){
        return makePatternProxy(namespace, TEST_REPORTABLE_MEMBERSHIP_PATTERN);
    }

    public static EntityProxy.Pattern getTestSubsetPattern(UUID namespace){
        return makePatternProxy(namespace, TEST_SUBSET_MEMBERSHIP_PATTERN);
    }

    public static EntityProxy.Pattern getTestOrderablePattern(UUID namespace){
        return makePatternProxy(namespace, TEST_ORDERABLE_MEMBERSHIP_PATTERN);
    }

    public static EntityProxy.Concept getLoincNumConcept(UUID namespace) {
        String loincNumber = "LOINC Number";
        return makeConceptProxy(namespace, loincNumber);
    }

    private static EntityProxy.Pattern makePatternProxy(UUID namespace, String description) {
        return EntityProxy.Pattern.make(description, UuidT5Generator.get(namespace, description));
    }

    private static EntityProxy.Concept makeConceptProxy(UUID namespace, String description) {
        return EntityProxy.Concept.make(description, UuidT5Generator.get(namespace, description));
    }

    public static EntityProxy.Concept getModuleConcept(UUID namespace){
        String loincModuleStr = "LOINC Module";
        EntityProxy.Concept loincModule = makeConceptProxy(namespace, loincModuleStr);
        return loincModule;
    }

    public static EntityProxy.Concept getPathConcept(){
        return TinkarTerm.MASTER_PATH;
    }

    public static EntityProxy.Concept getParentForPartType(UUID namespace, String partType){
        EntityProxy.Concept parentConcept;

        switch(partType) {
            case "CLASS":
                String loincClassStr = "LOINC Class";
                parentConcept = makeConceptProxy(namespace, loincClassStr);
                break;
            case "COMPONENT":
                String componentStr = "Component";
                parentConcept = makeConceptProxy(namespace, componentStr);
                break;
            case "PROPERTY":
                String propertyStr = "Property";
                parentConcept = makeConceptProxy(namespace, propertyStr);
                break;
            case "TIME":
                String timeAspectStr = "Time Aspect";
                parentConcept = makeConceptProxy(namespace, timeAspectStr);
                break;
            case "SYSTEM":
                String systemStr = "System";
                parentConcept = makeConceptProxy(namespace, systemStr);
                break;
            case "SCALE":
                String scaleStr = "Scale";
                parentConcept = makeConceptProxy(namespace, scaleStr);
                break;
            case "METHOD":
                String methodStr = "Method";
                parentConcept = makeConceptProxy(namespace, methodStr);
                break;
            default:
                parentConcept = null;
                break;
        }
        return parentConcept;
    }

    public static String buildOwlExpression(UUID namespace, String loincNum, String component, String property,
                                     String timeAspect, String system,
                                     String scaleType, String methodType) {
        EntityProxy.Concept loinNumConcept = makeConceptProxy(namespace, loincNum);

        String obsEntityStr = "Observable Entity";
        EntityProxy.Concept observableEntityConcept = makeConceptProxy(namespace, obsEntityStr);

        String componentStr = "Component";
        EntityProxy.Concept componentConcept = makeConceptProxy(namespace, componentStr);

        String propertyStr = "Property";
        EntityProxy.Concept propertyConcept = makeConceptProxy(namespace, propertyStr);

        String timeAspectStr = "Time Aspect";
        EntityProxy.Concept timeAspectConcept = makeConceptProxy(namespace, timeAspectStr);

        String systemStr = "System";
        EntityProxy.Concept systemConcept = makeConceptProxy(namespace, systemStr);

        String scaleStr = "Scale";
        EntityProxy.Concept scaleConcept = makeConceptProxy(namespace, scaleStr);

        String methodStr = "Method";
        EntityProxy.Concept methodConcept = makeConceptProxy(namespace, methodStr);

        EntityProxy.Concept componentValueConcept = makeConceptProxy(namespace, component);
        EntityProxy.Concept propertyValueConcept = makeConceptProxy(namespace, property);
        EntityProxy.Concept timeAspectValueConcept = makeConceptProxy(namespace, timeAspect);
        EntityProxy.Concept systemValueConcept = makeConceptProxy(namespace, system);
        EntityProxy.Concept scaleTypeValueConcept = makeConceptProxy(namespace, scaleType);
        EntityProxy.Concept methodTypeValueConcept = makeConceptProxy(namespace, methodType);

        String owlExpression =
                "EquivalentClasses(" +
                        ":["+  loinNumConcept.publicId().asUuidArray()[0] +"]" +
                        " ObjectIntersectionOf(" +
                        ":["+  observableEntityConcept.publicId().asUuidArray()[0] + "]" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + componentConcept.publicId().asUuidArray()[0]+ "]" +
                        " :[" + componentValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + propertyConcept.publicId().asUuidArray()[0] + "]" +
                        " :[" + propertyValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + timeAspectConcept.publicId().asUuidArray()[0] + "]" +
                        " :[" + timeAspectValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + systemConcept.publicId().asUuidArray()[0] + "]" +
                        " :[" + systemValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + scaleConcept.publicId().asUuidArray()[0] + "]" +
                        " :[" + scaleTypeValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        " ObjectSomeValuesFrom(" +
                        ":["+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"]" +
                        " ObjectSomeValuesFrom(" +
                        ":[" + methodConcept.publicId().asUuidArray()[0] + "]" +
                        " :[" + methodTypeValueConcept.publicId().asUuidArray()[0] + "]" +
                        ")" +
                        ")" +
                        ")" +
                        ")";
        return owlExpression;
    }

}
