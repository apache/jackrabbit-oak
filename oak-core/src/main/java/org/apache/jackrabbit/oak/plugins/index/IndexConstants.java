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
package org.apache.jackrabbit.oak.plugins.index;

/**
 * TODO document
 */
public interface IndexConstants {

    String INDEX_DEFINITIONS_NODE_TYPE = "oak:QueryIndexDefinition";

    String INDEX_DEFINITIONS_NAME = "oak:index";

    String TYPE_PROPERTY_NAME = "type";

    String TYPE_UNKNOWN = "unknown";

    String REINDEX_PROPERTY_NAME = "reindex";

    String REINDEX_COUNT = "reindexCount";

    String REINDEX_ASYNC_PROPERTY_NAME = "reindex-async";

    String INDEXING_MODE_SYNC = "sync";

    String INDEXING_MODE_NRT = "nrt";

    String ASYNC_PROPERTY_NAME = "async";

    String ASYNC_REINDEX_VALUE = "async-reindex";

    String ENTRY_COUNT_PROPERTY_NAME = "entryCount";

    String KEY_COUNT_PROPERTY_NAME = "keyCount";
    
    /**
     * The regular expression pattern of the values to be indexes.
     */
    String VALUE_PATTERN = "valuePattern";

    /**
     * A list of prefixes to be excluded from the index.
     */
    String VALUE_EXCLUDED_PREFIXES = "valueExcludedPrefixes";
    
    /**
     * A list of prefixes to be included from the index.
     * Include has higher priority than exclude.
     */
    String VALUE_INCLUDED_PREFIXES = "valueIncludedPrefixes";

    /**
     * Marks a unique property index.
     */
    String UNIQUE_PROPERTY_NAME = "unique";

    /**
     * Defines the names of the properties that are covered by a specific
     * property index definition.
     */
    String PROPERTY_NAMES = "propertyNames";

    /**
     * Defines the property name of the "declaringNodeTypes" property with
     * allows to restrict a given index definition to specific node types.
     */
    String DECLARING_NODE_TYPES = "declaringNodeTypes";

    String INDEX_CONTENT_NODE_NAME = ":index";

    /**
     * MVP to define the paths for which the index can be used to perform
     * queries. Defaults to ['/'].
     */
    String QUERY_PATHS = "queryPaths";

    /**
     * Property name for indicating that given index is corrupt and should be excluded
     * from further indexing. Its value is the date when this index was marked as
     * corrupt
     */
    String CORRUPT_PROPERTY_NAME = "corrupt";

    /**
     * CommitInfo attribute name which refers to the time at which
     * async index checkpoint is created i.e. time upto which repository
     * state is being indexed in given indexing cycle.
     *
     * The time is in string for as per Type.DATE
     */
    String CHECKPOINT_CREATION_TIME = "indexingCheckpointTime";
    
    /**
     * The index tag hint (when using "option(index tagged x, y)", this is IN("x", "y"))
     */
    String INDEX_TAG_OPTION = ":indexTag";

    /**
     * The tags property in the index definition.
     */
    String INDEX_TAGS = "tags";

    /**
     * The index name hint (when using "option(index abc)", this is "abc")
     */
    String INDEX_NAME_OPTION = ":indexName";

    /**
     * Boolean property on any index node indicating that such a node should not be
     * removed during reindex
     */
    String REINDEX_RETAIN = "retainNodeInReindex";

    /**
     * Index type for disabled indexes
     */
    String TYPE_DISABLED = "disabled";

    /**
     * Multi value property referring to index paths which current index supersedes
     */
    String SUPERSEDED_INDEX_PATHS = "supersedes";

    /**
     * Boolean flag indicating that old indexes need to be disabled
     */
    String DISABLE_INDEXES_ON_NEXT_CYCLE = ":disableIndexesOnNextCycle";

}
