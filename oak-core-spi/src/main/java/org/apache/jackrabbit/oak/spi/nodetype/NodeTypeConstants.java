/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.nodetype;

import org.apache.jackrabbit.JcrConstants;

/**
 * NodeTypeConstants... TODO
 */
public interface NodeTypeConstants extends JcrConstants {

    String JCR_NODE_TYPES = "jcr:nodeTypes";
    String NODE_TYPES_PATH = '/' + JcrConstants.JCR_SYSTEM + '/' + JCR_NODE_TYPES;

    String JCR_IS_ABSTRACT = "jcr:isAbstract";
    String JCR_IS_QUERYABLE = "jcr:isQueryable";
    String JCR_IS_FULLTEXT_SEARCHABLE = "jcr:isFullTextSearchable";
    String JCR_IS_QUERY_ORDERABLE = "jcr:isQueryOrderable";
    String JCR_AVAILABLE_QUERY_OPERATORS = "jcr:availableQueryOperators";

    /**
     * Constants for built-in repository defined node type names
     */
    String NT_REP_ROOT = "rep:root";
    String NT_REP_SYSTEM = "rep:system";
    String NT_REP_NODE_TYPES = "rep:nodeTypes";
    /**
     * @since oak 1.0
     */
    String NT_REP_UNSTRUCTURED = "rep:Unstructured";
    /**
     * @since oak 1.0
     */
    String NT_OAK_UNSTRUCTURED = "oak:Unstructured";
    /**
     * @since oak 1.5.7
     */
    String NT_OAK_RESOURCE = "oak:Resource";

    String NT_REP_NODE_TYPE = "rep:NodeType";
    String NT_REP_NAMED_PROPERTY_DEFINITIONS = "rep:NamedPropertyDefinitions";
    String NT_REP_PROPERTY_DEFINITIONS = "rep:PropertyDefinitions";
    String NT_REP_PROPERTY_DEFINITION = "rep:PropertyDefinition";
    String NT_REP_NAMED_CHILD_NODE_DEFINITIONS = "rep:NamedChildNodeDefinitions";
    String NT_REP_CHILD_NODE_DEFINITIONS = "rep:ChildNodeDefinitions";
    String NT_REP_CHILD_NODE_DEFINITION = "rep:ChildNodeDefinition";

    /**
     * Additional name constants not present in JcrConstants
     */
    String JCR_CREATEDBY = "jcr:createdBy";
    String JCR_LASTMODIFIEDBY = "jcr:lastModifiedBy";
    String MIX_CREATED = "mix:created";
    String MIX_LASTMODIFIED = "mix:lastModified";

    /**
     * Merge conflict handling
     */
    String MIX_REP_MERGE_CONFLICT = "rep:MergeConflict";
    String REP_OURS = "rep:ours";

    String RESIDUAL_NAME = "*";

    // Pre-compiled Oak type information fields
    String REP_SUPERTYPES = "rep:supertypes";
    String REP_PRIMARY_SUBTYPES = "rep:primarySubtypes";
    String REP_MIXIN_SUBTYPES = "rep:mixinSubtypes";
    String REP_MANDATORY_PROPERTIES = "rep:mandatoryProperties";
    String REP_MANDATORY_CHILD_NODES = "rep:mandatoryChildNodes";
    String REP_PROTECTED_PROPERTIES = "rep:protectedProperties";
    String REP_PROTECTED_CHILD_NODES = "rep:protectedChildNodes";
    String REP_HAS_PROTECTED_RESIDUAL_PROPERTIES = "rep:hasProtectedResidualProperties";
    String REP_HAS_PROTECTED_RESIDUAL_CHILD_NODES = "rep:hasProtectedResidualChildNodes";
    String REP_NAMED_SINGLE_VALUED_PROPERTIES = "rep:namedSingleValuedProperties";
    String REP_RESIDUAL_CHILD_NODE_DEFINITIONS = "rep:residualChildNodeDefinitions";
    String REP_NAMED_CHILD_NODE_DEFINITIONS = "rep:namedChildNodeDefinitions";
    String REP_RESIDUAL_PROPERTY_DEFINITIONS = "rep:residualPropertyDefinitions";
    String REP_NAMED_PROPERTY_DEFINITIONS = "rep:namedPropertyDefinitions";
    String REP_DECLARING_NODE_TYPE = "rep:declaringNodeType";
    String REP_PRIMARY_TYPE = "rep:primaryType";
    String REP_MIXIN_TYPES = "rep:mixinTypes";
    String REP_UUID = "rep:uuid";
    
    /**
     * mixin to enable the AtomicCounterEditor.
     */
    String MIX_ATOMIC_COUNTER = "mix:atomicCounter";
    
    /**
     * adding such mixin will allow the {@link JcrConstants#NT_UNSTRUCTURED} type under restricting
     * nodes such {@link JcrConstants#NT_FOLDER}
     */
    String MIX_INDEXABLE = "mix:indexable";
}
