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
package org.apache.jackrabbit.oak.plugins.nodetype;

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
    String OAK_SUPERTYPES = "oak:supertypes";
    String OAK_PRIMARY_SUBTYPES = "oak:primarySubtypes";
    String OAK_MIXIN_SUBTYPES = "oak:mixinSubtypes";
    String OAK_MANDATORY_PROPERTIES = "oak:mandatoryProperties";
    String OAK_MANDATORY_CHILD_NODES = "oak:mandatoryChildNodes";
    String OAK_PROTECTED_PROPERTIES = "oak:protectedProperties";
    String OAK_PROTECTED_CHILD_NODES = "oak:protectedChildNodes";
    String OAK_HAS_PROTECTED_RESIDUAL_PROPERTIES = "oak:hasProtectedResidualProperties";
    String OAK_HAS_PROTECTED_RESIDUAL_CHILD_NODES = "oak:hasProtectedResidualChildNodes";
    String OAK_NAMED_SINGLE_VALUED_PROPERTIES = "oak:namedSingleValuedProperties";
    String OAK_RESIDUAL_CHILD_NODE_DEFINITIONS = "oak:residualChildNodeDefinitions";
    String OAK_NAMED_CHILD_NODE_DEFINITIONS = "oak:namedChildNodeDefinitions";
    String OAK_RESIDUAL_PROPERTY_DEFINITIONS = "oak:residualPropertyDefinitions";
    String OAK_NAMED_PROPERTY_DEFINITIONS = "oak:namedPropertyDefinitions";
    String OAK_PROPERTY_DEFINITIONS = "oak:propertyDefinitions";
    String OAK_PROPERTY_DEFINITION = "oak:propertyDefinition";
    String OAK_CHILD_NODE_DEFINITIONS = "oak:childNodeDefinitions";
    String OAK_CHILD_NODE_DEFINITION = "oak:childNodeDefinition";
    String OAK_DECLARING_NODE_TYPE = "oak:declaringNodeType";
    String OAK_PRIMARY_TYPE = "oak:primaryType";
    String OAK_MIXIN_TYPES = "oak:mixinTypes";
    String OAK_UUID = "oak:uuid";

}
