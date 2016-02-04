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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

public interface LuceneIndexConstants {

    String TYPE_LUCENE = "lucene";

    String INDEX_DATA_CHILD_NAME = ":data";

    Version VERSION = Version.LUCENE_47;

    Analyzer ANALYZER = new OakAnalyzer(VERSION);

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

    String INDEX_DATA_CHILD_NAME_FS = "data";

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
     * Name of the codec to be used for indexing
     */
    String CODEC_NAME = "codec";

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

    String PROP_NODE_SCOPE_INDEX = "nodeScopeIndex";

    String PROP_PROPERTY_INDEX = "propertyIndex";

    String PROP_ANALYZED = "analyzed";

    String RULE_INHERITED = "inherited";

    String PROP_ORDERED = "ordered";

    /**
     * Integer property indicating that LuceneIndex should be
     * used in compat mode to specific version
     */
    String COMPAT_MODE = "compatVersion";

    /**
     * Boolean property to indicate that LuceneIndex is being used in testMode
     * and it should participate in every test
     */
    String TEST_MODE = "testMode";

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

    /**
     * Property definition config indicating that null check support should be
     * enabled for this property
     */
    String PROP_NULL_CHECK_ENABLED = "nullCheckEnabled";

    /**
     * whether use this property values for spellchecking
     */
    String PROP_USE_IN_SPELLCHECK = "useInSpellcheck";

    /**
     * whether use this property values for suggestions
     */
    String PROP_USE_IN_SUGGEST = "useInSuggest";

    /**
     * update frequency of the suggester in minutes
     */
    String SUGGEST_UPDATE_FREQUENCY_MINUTES = "suggestUpdateFrequencyMinutes";

    /**
     * IndexRule level config to indicate that Node name should also be index
     * to support fn:name() queries
     */
    String INDEX_NODE_NAME = "indexNodeName";

    /**
     * Boolean property indicating that Lucene directory content
     * should be saved as part of NodeState itself as a multi value property
     * to allow faster reads (OAK-2809)
     */
    String SAVE_DIR_LISTING = "saveDirectoryListing";

    /**
     * Optional  Property to store the path of index in the repository. Path at which index
     * definition is defined is not known to IndexEditor. To make use of CopyOnWrite
     * feature its required to know the indexPath to optimize the lookup and read of
     * existing index files
     */
    String INDEX_PATH = "indexPath";

    /**
     * Boolean property indicate that property should not be included in aggregation
     */
    String PROP_EXCLUDE_FROM_AGGREGATE = "excludeFromAggregation";
}
