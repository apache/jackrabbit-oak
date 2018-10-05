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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.AbstractAnalysisFactory;
import org.apache.lucene.util.Version;

/**
 * Constants used internally in Lucene indexes.
 */
public interface LuceneIndexConstants extends FulltextIndexConstants {

    String TYPE_LUCENE = "lucene";

    String SUGGEST_DATA_CHILD_NAME = ":suggest-data";

    String TRASH_CHILD_NAME = ":trash";

    Version VERSION = Version.LUCENE_47;

    Analyzer ANALYZER = new OakAnalyzer(VERSION);

    /**
     * Name of the codec to be used for indexing
     */
    String CODEC_NAME = "codec";

    /**
     * Name of the merge policy to be used while indexing
     */
    String MERGE_POLICY_NAME = "mergePolicy";

    /**
     * Boolean property to indicate that LuceneIndex is being used in testMode
     * and it should participate in every test
     */
    String TEST_MODE = "testMode";

    /**
     * Boolean property indicating if in-built analyzer should preserve original term
     * (i.e. use
     * {@link org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter#PRESERVE_ORIGINAL}
     * flag)
     */
    String INDEX_ORIGINAL_TERM = "indexOriginalTerm";

    /**
     * Node name under which various analyzers are configured
     */
    String ANALYZERS = "analyzers";

    /**
     * Name of the default analyzer definition node under 'analyzers' node
     */
    String ANL_DEFAULT = "default";
    String ANL_FILTERS = "filters";
    String ANL_STOPWORDS = "stopwords";
    String ANL_TOKENIZER = "tokenizer";
    String ANL_CHAR_FILTERS = "charFilters";
    String ANL_CLASS = "class";
    String ANL_NAME = "name";
    String ANL_LUCENE_MATCH_VERSION = AbstractAnalysisFactory.LUCENE_MATCH_VERSION_PARAM;

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
     * whether use this property values for similarity
     */
    String PROP_USE_IN_SIMILARITY = "useInSimilarity";

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
     * Its value should match {@link FieldNames#NODE_NAME}
     */
    String PROPDEF_PROP_NODE_NAME = ":nodeName";

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
     *
     * @deprecated With OAK-4152 no need to explicitly define indexPath property
     */
    @Deprecated
    String INDEX_PATH = "indexPath";

}
