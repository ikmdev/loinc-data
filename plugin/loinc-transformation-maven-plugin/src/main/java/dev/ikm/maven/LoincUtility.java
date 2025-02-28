package dev.ikm.maven;

import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.TinkarTerm;

import java.util.UUID;

public class LoincUtility {
    private static final String loincAuthorStr = "Regenstrief Institute, Inc. Author";

    /**
     * retrieves user concept
     * @return the snomed author
     */
    public static EntityProxy.Concept getAuthorConcept(UUID namespace){
        EntityProxy.Concept loincAuthor = EntityProxy.Concept.make(loincAuthorStr, UuidT5Generator.get(namespace,(loincAuthorStr)));
        return loincAuthor;
    }

    public static EntityProxy.Concept getModuleConcept(UUID namespace){
        return TinkarTerm.LOINC_MODULES;
    }

    public static EntityProxy.Concept getPathConcept(UUID namespace){
        return TinkarTerm.MASTER_PATH;
    }
}
