package org.apache.jackrabbit.oak.query.ast;

import java.util.Set;

/**
 * A nodetype info mechanism.
 */
public interface NodeTypeInfo {
    
    /**
     * Check whether the nodetype exists.
     * 
     * @return true if it exists
     */
    boolean exists();

    /**
     * Get the name of the nodetype.
     * 
     * @return the fully qualified name
     */
    String getNodeTypeName();

    /**
     * Get the set of supertypes.
     * 
     * @return the set
     */
    Set<String> getSuperTypes();

    /**
     * Get the set of primary subtypes.
     * 
     * @return the set
     */
    Set<String> getPrimarySubTypes();

    /**
     * Get the set of mixin subtypes.
     * 
     * @return the set
     */
    Set<String> getMixinSubTypes();

    /**
     * Check whether this is a mixin.
     * 
     * @return true if it is a mixin, false if it is a primary type
     */
    boolean isMixin();

    /**
     * Get the names of all single-valued properties.
     * 
     * @return the names
     */
    Iterable<String> getNamesSingleValuesProperties();

}