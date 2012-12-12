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

    /**
     * Additinal name constants not present in JcrConstants
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
    String ADD_EXISTING = "addExisting";
    String CHANGE_DELETED = "changeDeleted";
    String CHANGE_CHANGED = "changeChanged";
    String DELETE_CHANGED = "deleteChanged";
    String DELETE_DELETED = "deleteDeleted";

    String RESIDUAL_NAME = "*";
}