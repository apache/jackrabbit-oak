package org.apache.jackrabbit.oak.query.ast;

/**
 * A nodetype info mechanism.
 */
public interface NodeTypeInfoProvider {
    
    /**
     * Verify that the given nodetype exists.
     * 
     * @param nodeTypeName the fully qualified nodetype name
     * @return the information
     */
    NodeTypeInfo getNodeTypeInfo(String nodeTypeName);

}