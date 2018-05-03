/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.search;


public interface FulltextIndexConstants {

    enum IndexingMode {
        SYNC,
        NRT,
        ASYNC;

        public String asyncValueName(){
            return name().toLowerCase();
        }

        public static IndexingMode from(String indexingMode){
            return valueOf(indexingMode.toUpperCase());
        }
    }

    String INDEX_DATA_CHILD_NAME = ":data";

    /**
     * include only certain property types in the index
     */
    String INCLUDE_PROPERTY_TYPES = "includePropertyTypes";

    /**
     * exclude certain properties by name
     */
    String EXCLUDE_PROPERTY_NAMES = "excludePropertyNames";

    String PERSISTENCE_NAME = "persistence";

    String PERSISTENCE_OAK = "repository";

    String PERSISTENCE_FILE = "file";

    String PERSISTENCE_PATH = "path";

    /**
     * Experimental flag to control storage behavior: 'null' or 'true' means the content is stored
     */
    String EXPERIMENTAL_STORAGE = "oak.experimental.storage";

    /**
     * Determines if full text indexing is enabled for this index definition.
     * Default is true
     */
    String FULL_TEXT_ENABLED = "fulltextEnabled";

    /**
     * Only include properties with name in this set. If this property is defined
     * then {@code excludePropertyNames} would be ignored
     */
    String INCLUDE_PROPERTY_NAMES = "includePropertyNames";

    /**
     * Type of the property being indexed defined as part of property definition
     * under the given index definition. Refer to {@link javax.jcr.PropertyType}
     * contants for the possible values
     */
    String PROP_TYPE = "type";

    /**
     * Defines properties which would be used for ordering. If range queries are to
     * be performed with same property then it must be part of include list also
     */
    String ORDERED_PROP_NAMES = "orderedProps";

    /**
     * Size in bytes used for splitting the index files when storing them in NodeStore
     */
    String BLOB_SIZE = "blobSize";

    /**
     * Native function name associated with this index definition. Any query can
     * use this as the function name to ensure that this index gets used for invoking
     * the index
     */
    String FUNC_NAME = "functionName";

    /**
     * Child node name under which property details are provided
     */
    String PROP_NODE = "properties";

    String INDEX_RULES = "indexRules";

    /**
     * Field boost factor
     */
    String FIELD_BOOST = "boost";

    /**
     * Property name defined explicitly. Mostly used in case of relative property names
     */
    String PROP_NAME = "name";

    String PROP_IS_REGEX = "isRegexp";

    String PROP_INDEX = "index";

    String PROP_USE_IN_EXCERPT = "useInExcerpt";

    String EXCERPT_NODE_FIELD_NAME = ".";

    String PROP_NODE_SCOPE_INDEX = "nodeScopeIndex";

    String PROP_PROPERTY_INDEX = "propertyIndex";

    String PROP_ANALYZED = "analyzed";

    String RULE_INHERITED = "inherited";

    String PROP_ORDERED = "ordered";

    String PROP_SCORER_PROVIDER = "scorerProviderName";

    String PROP_WEIGHT = "weight";

    /**
     * Boolean property in property definition to mark sync properties
     */
    String PROP_SYNC = "sync";

    /**
     * Boolean property in property definition to mark unique properties
     */
    String PROP_UNIQUE = "unique";


    String EVALUATE_PATH_RESTRICTION = "evaluatePathRestrictions";

    /**
     * Experimental config to restrict which property type gets indexed at
     * property definition level. Mostly index rule level #INCLUDE_PROPERTY_TYPES
     * should be sufficient
     */
    String PROP_INCLUDED_TYPE = "oak.experimental.includePropertyTypes";

    /**
     * Regex to allow inclusion of all immediate properties of the node
     */
    String REGEX_ALL_PROPS = "^[^\\/]*$";

    /**
     * Node name storing the aggregate rules
     */
    String AGGREGATES = "aggregates";

    String AGG_PRIMARY_TYPE = "primaryType";

    /**
     * Name of property which stores the aggregate include pattern like <code>jcr:content/metadata</code>
     */
    String AGG_PATH = "path";

    /**
     * Limit for maximum number of reaggregates allowed. For example if there is an aggregate of nt:folder
     * and it also includes nt:folder then aggregation would traverse down untill this limit is hit
     */
    String AGG_RECURSIVE_LIMIT = "reaggregateLimit";

    /**
     * Boolean property indicating that separate fulltext field should be created for
     * node represented by this pattern
     */
    String AGG_RELATIVE_NODE = "relativeNode";

    String COST_PER_ENTRY = "costPerEntry";

    String COST_PER_EXECUTION = "costPerExecution";


    /**
     * Config node which include Tika related configuration
     */
    String TIKA = "tika";

    /**
     * nt:file node under 'tika' node which refers to the config xml file
     */
    String TIKA_CONFIG = "config.xml";

    String TIKA_MAX_EXTRACT_LENGTH = "maxExtractLength";

    /**
     *  Config node under tika which defines mime type mappings
     */
    String TIKA_MIME_TYPES = "mimeTypes";

    /**
     * Property name within the mime type structure which defines a mime type mapping
     */
    String TIKA_MAPPED_TYPE = "mappedType";

    /**
     * The maximum number of terms that will be indexed for a single field in a
     * document.  This limits the amount of memory required for indexing, so that
     * collections with very large files will not crash the indexing process by
     * running out of memory.
     * <p>
     * Note that this effectively truncates large documents, excluding from the
     * index terms that occur further in the document.  If you know your source
     * documents are large, be sure to set this value high enough to accommodate
     * the expected size.  If you set it to Integer.MAX_VALUE, then the only limit
     * is your memory, but you should anticipate an OutOfMemoryError.
     * <p>
     * By default, no more than 10,000 terms will be indexed for a field.
     */
    String MAX_FIELD_LENGTH = "maxFieldLength";

    /**
     * whether use this property values for suggestions
     */
    String PROP_USE_IN_SUGGEST = "useInSuggest";

    /**
     * subnode holding configuration for suggestions
     */
    String SUGGESTION_CONFIG = "suggestion";

    /**
     * update frequency of the suggester in minutes
     */
    String SUGGEST_UPDATE_FREQUENCY_MINUTES = "suggestUpdateFrequencyMinutes";

    /**
     * whether use this property values for spellchecking
     */
    String PROP_USE_IN_SPELLCHECK = "useInSpellcheck";

    /**
     * Property definition config indicating that null check support should be
     * enabled for this property
     */
    String PROP_NULL_CHECK_ENABLED = "nullCheckEnabled";

    /**
     * Property definition config indicating that this property would be used with
     * 'IS NOT NULL' constraint
     */
    String PROP_NOT_NULL_CHECK_ENABLED = "notNullCheckEnabled";

    /**
     * IndexRule level config to indicate that Node name should also be index
     * to support fn:name() queries
     */
    String INDEX_NODE_NAME = "indexNodeName";

    /**
     * Property definition name to indicate indexing node name
     */
    String PROPDEF_PROP_NODE_NAME = ":nodeName";


    /**
     * Optional subnode holding configuration for facets.
     */
    String FACETS = "facets";

    /**
     * Optional property to set the suggest field to be analyzed and therefore allow more fine
     * grained and flexible suggestions.
     */
    String SUGGEST_ANALYZED = "suggestAnalyzed";

    /**
     * Integer property indicating that LuceneIndex should be
     * used in compat mode to specific version
     */
    String COMPAT_MODE = "compatVersion";

    /**
     * Optional (index definition) property indicating max number of facets that will be retrieved
     * in query
     * Default is {@link IndexDefinition#DEFAULT_FACET_COUNT}
     */
    String PROP_FACETS_TOP_CHILDREN = "topChildren";

    /**
     * Optional (property definition) property indicating whether facets should be created
     * for this property
     */
    String PROP_FACETS = "facets";

    /**
     * Boolean property indicate that property should not be included in aggregation
     */
    String PROP_EXCLUDE_FROM_AGGREGATE = "excludeFromAggregation";

    /**
     * String property: the function to index, for function-based index
     */
    String PROP_FUNCTION = "function";

    /**
     * Boolean property which signal LuceneIndexEditor to refresh the stored index definition
     */
    String PROP_REFRESH_DEFN = "refresh";

    /**
     * Boolean property to indicate that nodes nodetype matching indexRule name
     * should be indexed
     */
    String PROP_INDEX_NODE_TYPE = "nodeTypeIndex";
}
