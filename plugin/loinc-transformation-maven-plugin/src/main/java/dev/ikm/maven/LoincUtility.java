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

    private static EntityProxy.Pattern makePatternProxy(UUID namespace, String description) {
        return EntityProxy.Pattern.make(description, UuidT5Generator.get(namespace, description));
    }

    private static EntityProxy.Concept makeConceptProxy(UUID namespace, String description) {
        return EntityProxy.Concept.make(description, UuidT5Generator.get(namespace, description));
    }

    public static EntityProxy.Concept getModuleConcept(){
        return TinkarTerm.LOINC_MODULES;
    }

    public static EntityProxy.Concept getPathConcept(){
        return TinkarTerm.MASTER_PATH;
    }

    public static String buildOwlExpression(UUID namespace, String component, String property,
                                     String timeAspect, String system,
                                     String scaleType, String methodType) {
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

        String owlExpression =
                "EquivalentClasses(\n" +
                        "    :LOINC_NUM column\n" +
                        "    ObjectIntersectionOf(\n" +
                        "        :"+  observableEntityConcept.publicId().asUuidArray()[0] + "\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + componentConcept.publicId().asUuidArray()[0]+ "\n" +
                        "                :" + component + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + propertyConcept.publicId().asUuidArray()[0] + "\n" +
                        "                :" + property + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + timeAspectConcept.publicId().asUuidArray()[0] + "\n" +
                        "                :" + timeAspect + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + systemConcept.publicId().asUuidArray()[0] + "\n" +
                        "                :" + system + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + scaleConcept.publicId().asUuidArray()[0] + "\n" +
                        "                :" + scaleType + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "        ObjectSomeValuesFrom(\n" +
                        "            :"+ TinkarTerm.ROLE_GROUP.publicId().asUuidArray()[0] +"\n" +
                        "            ObjectSomeValuesFrom(\n" +
                        "                :" + methodConcept.publicId().asUuidArray()[0] + "\n" +
                        "                :" + methodType + "\n" +
                        "            )\n" +
                        "        )\n" +
                        "    )\n" +
                        ")";
        return owlExpression;
    }

}
