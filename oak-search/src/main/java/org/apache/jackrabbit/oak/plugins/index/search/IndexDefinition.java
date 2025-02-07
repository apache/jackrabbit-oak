/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.primitives.Ints;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.index.search.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeTypeIterator;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_SCORE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DYNAMIC_BOOST_LITE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_NRT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_SYNC;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.COMPAT_MODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EVALUATE_PATH_RESTRICTION;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FIELD_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_SIMILARITY_BINARIES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_SIMILARITY_STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_FACETS_TOP_CHILDREN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_QUERY_FILTER_REGEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_RANDOM_SEED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_JVM_PARAM;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_SECURE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_VALUE_REGEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.TIKA;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.TIKA_CONFIG;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.TIKA_MAPPED_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.TIKA_MIME_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition.DEFAULT_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValues;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

/**
 * Represents a configuration of an index.
 */
public class IndexDefinition implements Aggregate.AggregateMapper {
    /**
     * Name of the internal property that contains the child order defined in
     * org.apache.jackrabbit.oak.plugins.tree.TreeConstants
     */
    private static final String OAK_CHILD_ORDER = ":childOrder";

    private static final Logger log = LoggerFactory.getLogger(IndexDefinition.class);

    private static boolean disableStoredIndexDefinition;

    /**
     * Blob size to use by default. To avoid issues in OAK-2105 the size should not
     * be power of 2.
     */
    public static final int DEFAULT_BLOB_SIZE = 1024 * 1024 - 1024;

    /**
     * Default entry count to keep estimated entry count low.
     */
    public static final long DEFAULT_ENTRY_COUNT = 1000;

    /**
     * Default value for property {@link #maxFieldLength}.
     */
    public static final int DEFAULT_MAX_FIELD_LENGTH = 10000;

    public static final int DEFAULT_MAX_EXTRACT_LENGTH = -10;

    /**
     * System managed hidden property to record the current index version
     */
    public static final String INDEX_VERSION = ":version";

    /**
     * Hidden node under index definition which is used to store the index definition
     * nodestate as it was at time of reindexing
     */
    public static final String INDEX_DEFINITION_NODE = ":index-definition";

    /**
     * Hidden node under index definition which is used to store meta info
     */
    public static final String STATUS_NODE = ":status";

    /**
     * Hidden node under index definition that contains indexed data for read only
     * part of composite node store.
     */
    public static final String HIDDEN_OAK_MOUNT_PREFIX = ":oak:mount-";

    /**
     * Node name under which all property indexes are created
     */
    public static final String PROPERTY_INDEX = ":property-index";

    /**
     * Property on status node which refers to the date when the index was lastUpdated
     * This may not be the same time as when index was closed but the time of checkpoint
     * upto which index is upto date (OAK-6194)
     */
    public static final String STATUS_LAST_UPDATED = "lastUpdated";

    public static final String CREATION_TIMESTAMP = "creationTimestamp";
    public static final String REINDEX_COMPLETION_TIMESTAMP = "reindexCompletionTimestamp";
    /**
     * Property to store paths for documents failed during index updates.
     */
    public static final String FAILED_DOC_PATHS = "failedDocPaths";

    /**
     * Meta property which provides the unique id
     */
    public static final String PROP_UID = "uid";

    private static final String TYPES_ALLOW_ALL_NAME = "all";

    static final int TYPES_ALLOW_NONE = PropertyType.UNDEFINED;

    static final int TYPES_ALLOW_ALL = -1;

    /**
     * Default suggesterUpdateFrequencyMinutes
     */
    public static final int DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES = 10;

    /**
     * Default no. of facets retrieved
     */
    static final int DEFAULT_FACET_COUNT = 10;

    /**
     * native sort order
     */
    public static final OrderEntry NATIVE_SORT_ORDER = new OrderEntry(JCR_SCORE, Type.UNDEFINED,
            OrderEntry.Order.DESCENDING);

    private final boolean indexSimilarityBinaries;
    private final boolean indexSimilarityStrings;

    /**
     * Dynamic boost uses index time boosting. This requires to have a separate field for each unique term that needs to
     * be boosted. With a high number of terms (thousands), this could result in a sparse index that requires extra disk
     * space. Usually, dynamicBoost and similarityTags are configured on the same field. In this case, similarityTags
     * work on the same set of terms without the need to store boost values. This does not affect the index size. With
     * oak.search.dynamicBoostLite=lucene no index time boosting is used for lucene types indexes. The terms will affect
     * the query match clause but the scores won't be the same. In summary, in lite mode the query will have the same
     * recall but lower precision.
     * <p>
     * WARNING: dynamicBoostLite needs similarityTags. In case there are no similarityTags, the query won't return the
     * expected results.
     */
    private final static String DYNAMIC_BOOST_LITE_NAME = "oak.search.dynamicBoostLite";
    protected final static List<String> DYNAMIC_BOOST_LITE =
            List.of(System.getProperty(DYNAMIC_BOOST_LITE_NAME, "").split(","));

    protected final boolean fullTextEnabled;

    protected final NodeState definition;

    private final NodeState root;

    private final IndexFormatVersion version;

    private final String funcName;

    private final int blobSize;

    /**
     * Defines the maximum estimated entry count configured.
     * Defaults to {#DEFAULT_ENTRY_COUNT}
     */
    private final long entryCount;

    private final boolean entryCountDefined;

    private final double costPerEntry;

    private final double costPerExecution;

    /**
     * The {@link IndexingRule}s inside this configuration. Keys being the NodeType names
     */
    private final Map<String, List<IndexingRule>> indexRules;

    private final List<IndexingRule> definedRules;

    private final String indexName;

    private final boolean evaluatePathRestrictions;

    private final Map<String, Aggregate> aggregates;

    private final boolean hasCustomTikaConfig;

    private final Map<String, String> customTikaMimeTypeMappings;

    private final int maxFieldLength;

    private final int maxExtractLength;

    private final int suggesterUpdateFrequencyMinutes;

    private final long reindexCount;

    private final PathFilter pathFilter;

    @Nullable
    private final String[] queryPaths;

    private final boolean suggestAnalyzed;

    private final SecureFacetConfiguration secureFacets;
    private final long randomSeed;

    private final int numberOfTopFacets;

    private final boolean suggestEnabled;

    private final boolean spellcheckEnabled;

    private final String indexPath;

    private final boolean nrtIndexMode;
    private final boolean syncIndexMode;

    private final boolean nodeTypeIndex;

    @Nullable
    private final String uid;

    @Nullable
    private final String[] indexTags;

    @Nullable
    private final String indexSelectionPolicy;

    private final boolean syncPropertyIndexes;

    private final String useIfExists;

    private final boolean deprecated;

    private final boolean testMode;

    private final boolean dynamicBoostLite;

    /**
     * See {@link FulltextIndexConstants#PROP_VALUE_REGEX}
     */
    private final Pattern propertyRegex;

    /**
     * See {@link FulltextIndexConstants#PROP_QUERY_FILTER_REGEX}
     */
    private final Pattern queryFilterRegex;

    public boolean isTestMode() {
        return testMode;
    }

    //~--------------------------------------------------------< Builder >

    // TODO - this method should be removed after tests don't use it anymore
    public static Builder newBuilder(NodeState root, NodeState defn, String indexPath) {
        return new Builder().root(root).defn(defn).indexPath(indexPath);
    }

    public static class Builder {
        /**
         * Default unique id used when no existing uid is defined
         * and index is not populated
         */
        private static final String DEFAULT_UID = "0";
        protected NodeState root;
        private NodeState defn;
        protected String indexPath;
        protected String uid;
        private boolean reindexMode;
        protected IndexFormatVersion version;

        public Builder root(NodeState root) {
            this.root = requireNonNull(root);
            return this;
        }

        public Builder defn(NodeState defn) {
            this.defn = requireNonNull(defn);
            return this;
        }

        public Builder indexPath(String indexPath) {
            this.indexPath = requireNonNull(indexPath);
            return this;
        }

        public Builder uid(String uid) {
            this.uid = uid;
            return this;
        }

        public Builder version(IndexFormatVersion version) {
            this.version = version;
            return this;
        }


        public Builder reindex() {
            this.reindexMode = true;
            return this;
        }

        public IndexDefinition build() {
            if (version == null) {
                version = determineIndexFormatVersion(defn);
            }
            if (uid == null) {
                uid = determineUniqueId(defn);
                if (uid == null && !IndexDefinition.hasPersistedIndex(defn)) {
                    uid = DEFAULT_UID;
                }
            }

            NodeState indexDefnStateToUse = defn;
            if (!reindexMode) {
                indexDefnStateToUse = getIndexDefinitionState(defn);
            }
            return createInstance(indexDefnStateToUse);
        }

        // TODO: This method should be abstract... to be done later after tests are updated so that they compile
        protected IndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new IndexDefinition(root, indexDefnStateToUse, version, uid, indexPath);
        }
    }

    public IndexDefinition(NodeState root, NodeState defn, String indexPath) {
        this(root, getIndexDefinitionState(defn), determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
    }

    protected IndexDefinition(NodeState root, NodeState defn, IndexFormatVersion version, String uid, String indexPath) {
        try {
            this.root = root;
            this.version = requireNonNull(version);
            this.uid = uid;
            this.definition = defn;
            this.indexPath = requireNonNull(indexPath);
            this.indexName = indexPath;
            this.indexTags = getOptionalValues(defn, IndexConstants.INDEX_TAGS, Type.STRINGS, String.class);
            this.indexSelectionPolicy
                    = getOptionalValue(defn, IndexConstants.INDEX_SELECTION_POLICY, null);
            this.nodeTypeIndex = getOptionalValue(defn, FulltextIndexConstants.PROP_INDEX_NODE_TYPE, false);
            this.blobSize = Math.max(1024, getOptionalValue(defn, BLOB_SIZE, DEFAULT_BLOB_SIZE));

            this.aggregates = nodeTypeIndex ? Map.of() : collectAggregates(defn);

            NodeState rulesState = defn.getChildNode(FulltextIndexConstants.INDEX_RULES);
            if (!rulesState.exists()) {
                rulesState = createIndexRules(defn).getNodeState();
            }

            this.testMode = getOptionalValue(defn, FulltextIndexConstants.TEST_MODE, false);
            List<IndexingRule> definedIndexRules = new ArrayList<>();
            this.indexRules = collectIndexRules(rulesState, definedIndexRules);
            this.definedRules = List.copyOf(definedIndexRules);

            this.fullTextEnabled = hasFulltextEnabledIndexRule(definedIndexRules);
            this.evaluatePathRestrictions = getOptionalValue(defn, EVALUATE_PATH_RESTRICTION, false);
            if (defn.hasProperty(PROP_VALUE_REGEX)) {
                this.propertyRegex = Pattern.compile(getOptionalValue(defn, PROP_VALUE_REGEX, ""));
            } else {
                this.propertyRegex = null;
            }
            if (defn.hasProperty(PROP_QUERY_FILTER_REGEX)) {
                this.queryFilterRegex = Pattern.compile(getOptionalValue(defn, PROP_QUERY_FILTER_REGEX, ""));
            } else {
                this.queryFilterRegex = null;
            }
            String functionName = getOptionalValue(defn, FulltextIndexConstants.FUNC_NAME, null);
            if (fullTextEnabled && functionName == null) {
                functionName = getDefaultFunctionName();
            }
            this.funcName = functionName != null ? "native*" + functionName : null;

            if (defn.hasProperty(ENTRY_COUNT_PROPERTY_NAME)) {
                this.entryCountDefined = true;
                this.entryCount = getOptionalValue(defn, ENTRY_COUNT_PROPERTY_NAME, DEFAULT_ENTRY_COUNT);
            } else {
                this.entryCountDefined = false;
                this.entryCount = DEFAULT_ENTRY_COUNT;
            }

            this.maxFieldLength = getOptionalValue(defn, FulltextIndexConstants.MAX_FIELD_LENGTH, DEFAULT_MAX_FIELD_LENGTH);
            this.costPerEntry = getOptionalValue(defn, FulltextIndexConstants.COST_PER_ENTRY, getDefaultCostPerEntry(version));
            this.costPerExecution = getOptionalValue(defn, FulltextIndexConstants.COST_PER_EXECUTION, 1.0);
            this.hasCustomTikaConfig = getTikaConfigNode().exists();
            this.customTikaMimeTypeMappings = buildMimeTypeMap(definition.getChildNode(TIKA).getChildNode(TIKA_MIME_TYPES));
            this.maxExtractLength = determineMaxExtractLength();
            this.suggesterUpdateFrequencyMinutes = evaluateSuggesterUpdateFrequencyMinutes(defn,
                    DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES);
            this.reindexCount = getOptionalValue(defn, REINDEX_COUNT, 0);
            this.pathFilter = PathFilter.from(new ReadOnlyBuilder(defn));
            this.queryPaths = getOptionalValues(defn, IndexConstants.QUERY_PATHS, Type.STRINGS, String.class);
            this.suggestAnalyzed = evaluateSuggestAnalyzed(defn, false);

            {
                PropertyState randomPS = defn.getProperty(PROP_RANDOM_SEED);
                if (randomPS != null && randomPS.getType() == Type.LONG) {
                    randomSeed = randomPS.getValue(Type.LONG);
                } else {
                    // create a random number
                    randomSeed = UUID.randomUUID().getMostSignificantBits();
                }
            }
            if (defn.hasChildNode(FACETS)) {
                NodeState facetsConfig = defn.getChildNode(FACETS);
                this.secureFacets = SecureFacetConfiguration.getInstance(randomSeed, facetsConfig);
                this.numberOfTopFacets = getOptionalValue(facetsConfig, PROP_FACETS_TOP_CHILDREN, DEFAULT_FACET_COUNT);
            } else {
                this.secureFacets = SecureFacetConfiguration.getInstance(randomSeed, null);
                this.numberOfTopFacets = DEFAULT_FACET_COUNT;
            }

            this.suggestEnabled = evaluateSuggestionEnabled();
            this.spellcheckEnabled = evaluateSpellcheckEnabled();
            this.nrtIndexMode = supportsNRTIndexing(defn);
            this.syncIndexMode = supportsSyncIndexing(defn);
            this.syncPropertyIndexes = definedRules.stream().anyMatch(ir -> !ir.syncProps.isEmpty());
            this.useIfExists = getOptionalValue(defn, IndexConstants.USE_IF_EXISTS, null);
            this.deprecated = getOptionalValue(defn, IndexConstants.INDEX_DEPRECATED, false);
            this.dynamicBoostLite = getOptionalValue(defn, DYNAMIC_BOOST_LITE_PROPERTY_NAME,
                    defn.getProperty(TYPE_PROPERTY_NAME) != null &&
                            DYNAMIC_BOOST_LITE.contains(defn.getProperty(TYPE_PROPERTY_NAME).getValue(Type.STRING))
            );
            this.indexSimilarityBinaries = getSimilarityDefaultValue(defn, INDEX_SIMILARITY_BINARIES);
            this.indexSimilarityStrings = getSimilarityDefaultValue(defn, INDEX_SIMILARITY_STRINGS);
        } catch (IllegalStateException e) {
            log.error("Config error for index definition at {} . Please correct the index definition "
                    + "and reindex after correction. Additional Info : {}", indexPath, e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private boolean getSimilarityDefaultValue(NodeState defn, String propertyKey) {
        return defn.getProperty(propertyKey) == null // = true in case this property is not defined
                || (defn.getProperty(TYPE_PROPERTY_NAME) != null && isPresent(defn.getProperty(TYPE_PROPERTY_NAME).getValue(Type.STRING), defn.getProperty(propertyKey).getValue(Type.STRINGS).iterator()));
    }

    private <T> boolean isPresent(T key, Iterator<T> iterator) {
        while (iterator.hasNext()) {
            if (key.equals(iterator.next())) {
                return true;
            }
        }
        return false;
    }

    public NodeState getDefinitionNodeState() {
        return definition;
    }

    public boolean isEnabled() {
        if (useIfExists == null) {
            return true;
        }
        if (!PathUtils.isValid(useIfExists)) {
            return false;
        }
        NodeState nodeState = root;
        for (String element : PathUtils.elements(useIfExists)) {
            if (element.startsWith("@")) {
                return nodeState.hasProperty(element.substring(1));
            }
            nodeState = nodeState.getChildNode(element);
            if (!nodeState.exists()) {
                return false;
            }
        }
        return true;
    }

    public boolean shouldIndexSimilarityBinaries() {
        return indexSimilarityBinaries;
    }

    public boolean shouldIndexSimilarityStrings() {
        return indexSimilarityStrings;
    }

    public boolean isDynamicBoostLiteEnabled() {
        return dynamicBoostLite;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public String getFunctionName() {
        return funcName;
    }

    //TODO Should this method be abstract?
    protected String getDefaultFunctionName() {
        return "fulltext";//TODO Should this be FulltextIndexConstants.FUNC_NAME
    }

    public boolean hasFunctionDefined() {
        return funcName != null;
    }

    /**
     * Size in bytes for the blobs created while storing the index content
     *
     * @return size in bytes
     */
    public int getBlobSize() {
        return blobSize;
    }

    public long getReindexCount() {
        return reindexCount;
    }

    public long getEntryCount() {
        return entryCount;
    }

    private int evaluateSuggesterUpdateFrequencyMinutes(NodeState defn, int defaultValue) {
        NodeState suggestionConfig = defn.getChildNode(FulltextIndexConstants.SUGGESTION_CONFIG);

        if (!suggestionConfig.exists()) {
            //handle backward compatibility
            return getOptionalValue(defn, FulltextIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES, defaultValue);
        }

        return getOptionalValue(suggestionConfig, FulltextIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES, defaultValue);
    }

    public int getSuggesterUpdateFrequencyMinutes() {
        return suggesterUpdateFrequencyMinutes;
    }

    public boolean isEntryCountDefined() {
        return entryCountDefined;
    }

    public double getCostPerEntry() {
        return costPerEntry;
    }

    protected double getDefaultCostPerEntry(IndexFormatVersion version) {
        return 1.0;
    }

    public double getCostPerExecution() {
        return costPerExecution;
    }

    public long getFulltextEntryCount(long numOfDocs) {
        if (isEntryCountDefined()) {
            return Math.min(getEntryCount(), numOfDocs);
        }
        return numOfDocs;
    }

    public boolean isDeprecated() {
        return deprecated;
    }

    public IndexFormatVersion getVersion() {
        return version;
    }


    public boolean isOfOldFormat() {
        return !hasIndexingRules(definition);
    }

    public boolean evaluatePathRestrictions() {
        return evaluatePathRestrictions;
    }

    public boolean hasCustomTikaConfig() {
        return hasCustomTikaConfig;
    }

    public InputStream getTikaConfig() {
        return ConfigUtil.getBlob(getTikaConfigNode(), TIKA_CONFIG).getNewStream();
    }

    public String getTikaMappedMimeType(String type) {
        return customTikaMimeTypeMappings.getOrDefault(type, type);
    }

    public String getIndexName() {
        return indexName;
    }

    public String[] getIndexTags() {
        return indexTags;
    }

    public String getIndexSelectionPolicy() {
        return indexSelectionPolicy;
    }

    public int getMaxExtractLength() {
        return maxExtractLength;
    }

    public PathFilter getPathFilter() {
        return pathFilter;
    }

    @Nullable
    public String[] getQueryPaths() {
        return queryPaths;
    }

    @Nullable
    public String getUniqueId() {
        return uid;
    }

    public boolean isNRTIndexingEnabled() {
        return nrtIndexMode;
    }

    public boolean isSyncIndexingEnabled() {
        return syncIndexMode;
    }

    public boolean hasSyncPropertyDefinitions() {
        return syncPropertyIndexes;
    }

    public boolean isPureNodeTypeIndex() {
        return nodeTypeIndex;
    }

    /**
     * Check if the index definition is fresh, or (some) indexing has occurred.
     * <p>
     * WARNING: If there is _any_ hidden node, then it is assumed that
     * no reindex is needed. Even if the hidden node is completely unrelated
     * and doesn't contain index data (for example the node ":status").
     * See also OAK-7991.
     *
     * @param definition nodestate for Index Definition
     * @return true if index has some indexed content
     */
    public static boolean hasPersistedIndex(NodeState definition) {
        for (String rm : definition.getChildNodeNames()) {
            if (NodeStateUtils.isHidden(rm)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isDisableStoredIndexDefinition() {
        return disableStoredIndexDefinition;
    }

    public static void setDisableStoredIndexDefinition(boolean disableStoredIndexDefinitionDefault) {
        IndexDefinition.disableStoredIndexDefinition = disableStoredIndexDefinitionDefault;
    }

    public Set<String> getRelativeNodeNames() {
        //Can be computed lazily as required only for oak-run indexing for now
        Set<String> names = new HashSet<>();
        for (IndexingRule r : definedRules) {
            for (Aggregate.Include i : r.aggregate.getIncludes()) {
                for (int d = 0; d < i.maxDepth(); d++) {
                    if (!i.isPattern(d)) {
                        names.add(i.getElementNameIfNotAPattern(d));
                    }
                }
            }
        }
        return names;
    }

    public boolean indexesRelativeNodes() {
        for (IndexingRule r : definedRules) {
            if (!r.aggregate.getIncludes().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Fulltext Index : " + indexName;
    }


    //~---------------------------------------------------< Aggregates >

    @Nullable
    public Aggregate getAggregate(String nodeType) {
        return aggregates.get(nodeType);
    }

    @Nullable
    public Map<String, Aggregate> getAggregates() {
        return aggregates;
    }

    private Map<String, Aggregate> collectAggregates(NodeState defn) {
        Map<String, Aggregate> aggregateMap = new HashMap<>();

        for (ChildNodeEntry cne : defn.getChildNode(FulltextIndexConstants.AGGREGATES).getChildNodeEntries()) {
            String nodeType = cne.getName();
            int recursionLimit = getOptionalValue(cne.getNodeState(), FulltextIndexConstants.AGG_RECURSIVE_LIMIT,
                    Aggregate.RECURSIVE_AGGREGATION_LIMIT_DEFAULT);

            List<Aggregate.Include> includes = new ArrayList<>();
            for (ChildNodeEntry include : cne.getNodeState().getChildNodeEntries()) {
                NodeState is = include.getNodeState();
                String primaryType = is.getString(FulltextIndexConstants.AGG_PRIMARY_TYPE);
                String path = is.getString(FulltextIndexConstants.AGG_PATH);
                boolean relativeNode = getOptionalValue(is, FulltextIndexConstants.AGG_RELATIVE_NODE, false);
                if (path == null) {
                    log.warn("Aggregate pattern in {} does not have required property [{}]. {} aggregate rule would " +
                            "be ignored", this, FulltextIndexConstants.AGG_PATH, include.getName());
                    continue;
                }
                includes.add(new Aggregate.NodeInclude(this, primaryType, path, relativeNode));
            }
            aggregateMap.put(nodeType, new Aggregate(nodeType, includes, recursionLimit));
        }

        return Map.copyOf(aggregateMap);
    }

    //~---------------------------------------------------< IndexRule >

    public boolean hasMatchingNodeTypeReg(NodeState root) {
        try {
            return this.root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES)
                    .equals(root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES));
        } catch (IllegalRepositoryStateException e) {
            // the root might be so old that a SegmentNotFoundException
            // is thrown - in which case we can't be sure
            // that the node type registry wasn't changed
            log.warn("Possibly old root: {}", e.toString());
            log.debug("Possibly old root", e);
            return false;
        }
    }


    public List<IndexingRule> getDefinedRules() {
        return definedRules;
    }

    @Nullable
    public IndexingRule getApplicableIndexingRule(String primaryNodeType) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(primaryNodeType);
        if (r != null) {
            rules = new ArrayList<>(r);
        }

        if (rules != null) {
            for (IndexingRule rule : rules) {
                if (rule.appliesTo(primaryNodeType)) {
                    return rule;
                }
            }
        }

        // no applicable rule
        return null;
    }

    /**
     * Returns the first indexing rule that applies to the given node
     * <code>state</code>.
     *
     * @param state a node state.
     * @return the indexing rule or <code>null</code> if none applies.
     */
    @Nullable
    public IndexingRule getApplicableIndexingRule(NodeState state) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(getPrimaryTypeName(state));
        if (r != null) {
            rules = new ArrayList<>(r);
        }

        for (String name : getMixinTypeNames(state)) {
            r = indexRules.get(name);
            if (r != null) {
                if (rules == null) {
                    rules = new ArrayList<>();
                }
                rules.addAll(r);
            }
        }

        if (rules != null) {
            for (IndexingRule rule : rules) {
                if (rule.appliesTo(state)) {
                    return rule;
                }
            }
        }

        // no applicable rule
        return null;
    }

    private Map<String, List<IndexingRule>> collectIndexRules(NodeState indexRules,
                                                              List<IndexingRule> definedIndexRules) {
        //TODO if a rule is defined for nt:base then this map would have entry for each
        //registered nodeType in the system

        if (!indexRules.exists()) {
            return Map.of();
        }

        if (!hasOrderableChildren(indexRules)) {
            log.warn("IndexRule node does not have orderable children in [{}]", IndexDefinition.this);
        }

        Map<String, List<IndexingRule>> nt2rules = new HashMap<>();
        ReadOnlyNodeTypeManager ntReg = createNodeTypeManager(RootFactory.createReadOnlyRoot(root));

        //Use Tree API to read ordered child nodes
        Tree ruleTree = TreeFactory.createReadOnlyTree(indexRules);
        final List<String> allNames = getAllNodeTypes(ntReg);
        for (Tree ruleEntry : ruleTree.getChildren()) {
            IndexingRule rule = new IndexingRule(ruleEntry.getName(), indexRules.getChildNode(ruleEntry.getName()));
            definedIndexRules.add(rule);

            // register under node type and all its sub types
            log.trace("Found rule '{}' for NodeType '{}'", rule, rule.getNodeTypeName());

            List<String> ntNames = allNames;
            if (!rule.inherited) {
                //Trim the list to rule's nodeType so that inheritance check
                //is not performed for other nodeTypes
                ntNames = List.of(rule.getNodeTypeName());
            }

            for (String ntName : ntNames) {
                if (ntReg.isNodeType(ntName, rule.getNodeTypeName())) {
                    List<IndexingRule> perNtConfig = nt2rules.computeIfAbsent(ntName, k -> new ArrayList<>());
                    log.trace("Registering rule '{}' for name '{}'", rule, ntName);
                    perNtConfig.add(new IndexingRule(rule, ntName));
                }
            }
        }

        for (Map.Entry<String, List<IndexingRule>> e : nt2rules.entrySet()) {
            e.setValue(List.copyOf(e.getValue()));
        }

        return Map.copyOf(nt2rules);
    }

    private boolean evaluateSuggestionEnabled() {
        for (IndexingRule indexingRule : definedRules) {
            for (PropertyDefinition propertyDefinition : indexingRule.propConfigs.values()) {
                if (propertyDefinition.useInSuggest) {
                    return true;
                }
            }
            for (NamePattern np : indexingRule.namePatterns) {
                if (np.getConfig().useInSuggest) {
                    return true;
                }
            }
        }
        return false;
    }

    public Pattern getPropertyRegex() {
        return propertyRegex;
    }

    public Pattern getQueryFilterRegex() {
        return queryFilterRegex;
    }

    public boolean isSuggestEnabled() {
        return suggestEnabled;
    }

    private boolean evaluateSpellcheckEnabled() {
        for (IndexingRule indexingRule : definedRules) {
            for (PropertyDefinition propertyDefinition : indexingRule.propConfigs.values()) {
                if (propertyDefinition.useInSpellcheck) {
                    return true;
                }
            }
            for (NamePattern np : indexingRule.namePatterns) {
                if (np.getConfig().useInSpellcheck) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isSpellcheckEnabled() {
        return spellcheckEnabled;
    }

    public String getIndexPath() {
        return indexPath;
    }

    private boolean evaluateSuggestAnalyzed(NodeState defn, boolean defaultValue) {
        NodeState suggestionConfig = defn.getChildNode(FulltextIndexConstants.SUGGESTION_CONFIG);

        if (!suggestionConfig.exists()) {
            //handle backward compatibility
            return getOptionalValue(defn, FulltextIndexConstants.SUGGEST_ANALYZED, defaultValue);
        }

        return getOptionalValue(suggestionConfig, FulltextIndexConstants.SUGGEST_ANALYZED, defaultValue);
    }

    public boolean isSuggestAnalyzed() {
        return suggestAnalyzed;
    }

    public SecureFacetConfiguration getSecureFacetConfiguration() {
        return secureFacets;
    }

    public int getNumberOfTopFacets() {
        return numberOfTopFacets;
    }

    public class IndexingRule {
        private final String baseNodeType;
        private final String nodeTypeName;
        /**
         * Case-insensitive map of lower cased propertyName to propertyConfigs
         */
        private final Map<String, PropertyDefinition> propConfigs;
        /**
         * List of {@code NamePattern}s configured for this rule
         */
        private final List<NamePattern> namePatterns;
        private final List<PropertyDefinition> nullCheckEnabledProperties;
        private final List<PropertyDefinition> functionRestrictions;
        private final List<PropertyDefinition> notNullCheckEnabledProperties;
        private final List<PropertyDefinition> nodeScopeAnalyzedProps;
        private final List<PropertyDefinition> syncProps;
        private final List<PropertyDefinition> similarityProperties;
        private final boolean indexesAllNodesOfMatchingType;
        private final boolean nodeNameIndexed;

        public final float boost;
        final boolean inherited;
        public final int propertyTypes;
        final boolean fulltextEnabled;
        public final boolean propertyIndexEnabled;
        final boolean nodeFullTextIndexed;

        final Aggregate aggregate;
        final Aggregate propAggregate;

        IndexingRule(String nodeTypeName, NodeState config) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = nodeTypeName;
            this.boost = getOptionalValue(config, FIELD_BOOST, DEFAULT_BOOST);
            this.inherited = getOptionalValue(config, FulltextIndexConstants.RULE_INHERITED, true);
            this.propertyTypes = getSupportedTypes(config, INCLUDE_PROPERTY_TYPES, TYPES_ALLOW_ALL);

            List<NamePattern> namePatterns = new ArrayList<>();
            List<PropertyDefinition> nonExistentProperties = new ArrayList<>();
            List<PropertyDefinition> functionRestrictions = new ArrayList<>();
            List<PropertyDefinition> existentProperties = new ArrayList<>();
            List<PropertyDefinition> nodeScopeAnalyzedProps = new ArrayList<>();
            List<PropertyDefinition> syncProps = new ArrayList<>();
            List<PropertyDefinition> similarityProperties = new ArrayList<>();
            List<Aggregate.Include> propIncludes = new ArrayList<>();
            this.propConfigs = collectPropConfigs(config, namePatterns, propIncludes, nonExistentProperties,
                    existentProperties, nodeScopeAnalyzedProps, functionRestrictions, syncProps, similarityProperties);
            this.propAggregate = new Aggregate(nodeTypeName, propIncludes);
            this.aggregate = combine(propAggregate, nodeTypeName);

            this.namePatterns = List.copyOf(namePatterns);
            this.nodeScopeAnalyzedProps = List.copyOf(nodeScopeAnalyzedProps);
            this.nullCheckEnabledProperties = List.copyOf(nonExistentProperties);
            this.functionRestrictions = List.copyOf(functionRestrictions);
            this.notNullCheckEnabledProperties = List.copyOf(existentProperties);
            this.similarityProperties = List.copyOf(similarityProperties);
            this.fulltextEnabled = aggregate.hasNodeAggregates() || hasAnyFullTextEnabledProperty();
            this.nodeFullTextIndexed = aggregate.hasNodeAggregates() || anyNodeScopeIndexedProperty();
            this.propertyIndexEnabled = hasAnyPropertyIndexConfigured();
            this.indexesAllNodesOfMatchingType = areAlMatchingNodeByTypeIndexed();
            this.nodeNameIndexed = evaluateNodeNameIndexed(config);
            this.syncProps = List.copyOf(syncProps);
            validateRuleDefinition();
        }

        /**
         * Creates a new indexing rule base on an existing one, but for a
         * different node type name.
         *
         * @param original     the existing rule.
         * @param nodeTypeName the node type name for the rule.
         */
        IndexingRule(IndexingRule original, String nodeTypeName) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = original.getNodeTypeName();
            this.propConfigs = original.propConfigs;
            this.namePatterns = original.namePatterns;
            this.boost = original.boost;
            this.inherited = original.inherited;
            this.propertyTypes = original.propertyTypes;
            this.propertyIndexEnabled = original.propertyIndexEnabled;
            this.propAggregate = original.propAggregate;
            this.nullCheckEnabledProperties = original.nullCheckEnabledProperties;
            this.notNullCheckEnabledProperties = original.notNullCheckEnabledProperties;
            this.functionRestrictions = original.functionRestrictions;
            this.nodeScopeAnalyzedProps = original.nodeScopeAnalyzedProps;
            this.aggregate = combine(propAggregate, nodeTypeName);
            this.fulltextEnabled = aggregate.hasNodeAggregates() || original.fulltextEnabled;
            this.nodeFullTextIndexed = aggregate.hasNodeAggregates() || original.nodeFullTextIndexed;
            this.indexesAllNodesOfMatchingType = areAlMatchingNodeByTypeIndexed();
            this.nodeNameIndexed = original.nodeNameIndexed;
            this.syncProps = original.syncProps;
            this.similarityProperties = original.similarityProperties;
        }

        /**
         * Returns <code>true</code> if the property with the given name is
         * indexed according to this rule.
         *
         * @param propertyName the name of a property.
         * @return <code>true</code> if the property is indexed;
         * <code>false</code> otherwise.
         */
        public boolean isIndexed(String propertyName) {
            return getConfig(propertyName) != null;
        }


        /**
         * Returns the name of the node type where this rule applies to.
         *
         * @return name of the node type.
         */
        public String getNodeTypeName() {
            return nodeTypeName;
        }

        /**
         * @return name of the base node type.
         */
        public String getBaseNodeType() {
            return baseNodeType;
        }

        /**
         * Returns all the configured {@code PropertyDefinition}s for this {@code IndexRule}.
         * <p>
         * In case of a pure nodetype index we just return primaryType and mixins.
         *
         * @return an {@code Iterable} of {@code PropertyDefinition}s.
         * @see IndexDefinition#isPureNodeTypeIndex()
         */
        public Iterable<PropertyDefinition> getProperties() {
            return propConfigs.values();
        }

        public List<PropertyDefinition> getNullCheckEnabledProperties() {
            return nullCheckEnabledProperties;
        }

        public List<PropertyDefinition> getFunctionRestrictions() {
            return functionRestrictions;
        }

        public List<PropertyDefinition> getNotNullCheckEnabledProperties() {
            return notNullCheckEnabledProperties;
        }

        public List<PropertyDefinition> getNodeScopeAnalyzedProps() {
            return nodeScopeAnalyzedProps;
        }

        public List<PropertyDefinition> getSimilarityProperties() {
            return similarityProperties;
        }

        public Stream<PropertyDefinition> getNamePatternsProperties() {
            return namePatterns.stream().map(NamePattern::getConfig);
        }

        @Override
        public String toString() {
            String str = "IndexRule: " + nodeTypeName;
            if (!baseNodeType.equals(nodeTypeName)) {
                str += "(" + baseNodeType + ")";
            }
            return str;
        }

        public boolean isAggregated(String nodePath) {
            return aggregate.hasRelativeNodeInclude(nodePath);
        }

        /**
         * Returns <code>true</code> if this rule applies to the given node
         * <code>state</code>.
         *
         * @param state the state to check.
         * @return <code>true</code> the rule applies to the given node;
         * <code>false</code> otherwise.
         */
        public boolean appliesTo(NodeState state) {
            for (String mixinName : getMixinTypeNames(state)) {
                if (nodeTypeName.equals(mixinName)) {
                    return true;
                }
            }

            return nodeTypeName.equals(getPrimaryTypeName(state));
        }

        public boolean appliesTo(String nodeTypeName) {
            return this.nodeTypeName.equals(nodeTypeName);

        }

        public boolean isNodeNameIndexed() {
            return nodeNameIndexed;
        }

        /**
         * Returns true if the rule has any property definition which enables
         * evaluation of fulltext related clauses
         */
        public boolean isFulltextEnabled() {
            return fulltextEnabled;
        }

        /**
         * Returns true if a fulltext field for the node would be created
         * for any node matching the current rule.
         */
        public boolean isNodeFullTextIndexed() {
            return nodeFullTextIndexed;
        }

        /**
         * @param propertyName name of a property.
         * @return the property configuration or <code>null</code> if this
         * indexing rule does not contain a configuration for the given
         * property.
         */
        @Nullable
        public PropertyDefinition getConfig(String propertyName) {
            PropertyDefinition config = propConfigs.get(propertyName.toLowerCase(Locale.ENGLISH));
            if (config != null) {
                return config;
            } else if (namePatterns.size() > 0) {
                // check patterns
                for (NamePattern np : namePatterns) {
                    if (np.matches(propertyName)) {
                        return np.getConfig();
                    }
                }
            }
            return null;
        }

        public boolean includePropertyType(int type) {
            return IndexDefinition.includePropertyType(propertyTypes, type);
        }

        public Aggregate getAggregate() {
            return aggregate;
        }

        /**
         * Flag to determine weather current index rule definition allows indexing of all
         * node of type as covered by the current rule. For example if the rule only indexes
         * certain property 'foo' for node type 'app:Asset' then index would only have
         * entries for those assets where foo is defined. Such an index cannot claim that
         * it has entries for all assets.
         *
         * @return true in case all matching node types are covered by this rule
         */
        public boolean indexesAllNodesOfMatchingType() {
            return indexesAllNodesOfMatchingType;
        }

        public boolean isBasedOnNtBase() {
            return JcrConstants.NT_BASE.equals(baseNodeType);
        }

        private Map<String, PropertyDefinition> collectPropConfigs(NodeState config,
                                                                   List<NamePattern> patterns,
                                                                   List<Aggregate.Include> propAggregate,
                                                                   List<PropertyDefinition> nonExistentProperties,
                                                                   List<PropertyDefinition> existentProperties,
                                                                   List<PropertyDefinition> nodeScopeAnalyzedProps,
                                                                   List<PropertyDefinition> functionRestrictions,
                                                                   List<PropertyDefinition> syncProps,
                                                                   List<PropertyDefinition> similarityProperties) {
            Map<String, PropertyDefinition> propDefns = new HashMap<>();
            NodeState propNode = config.getChildNode(FulltextIndexConstants.PROP_NODE);

            if (propNode.exists() && !hasOrderableChildren(propNode)) {
                log.warn("Properties node for [{}] does not have orderable " +
                        "children in [{}]", this, IndexDefinition.this);
            }

            //In case of a pure nodetype index we just index primaryType and mixins
            //and ignore any other property definition
            if (nodeTypeIndex) {
                boolean sync = getOptionalValue(config, FulltextIndexConstants.PROP_SYNC, false);
                PropertyDefinition pdpt = createNodeTypeDefinition(this, JcrConstants.JCR_PRIMARYTYPE, sync);
                PropertyDefinition pdmixin = createNodeTypeDefinition(this, JcrConstants.JCR_MIXINTYPES, sync);

                propDefns.put(pdpt.name.toLowerCase(Locale.ENGLISH), pdpt);
                propDefns.put(pdmixin.name.toLowerCase(Locale.ENGLISH), pdmixin);

                if (sync) {
                    syncProps.add(pdpt);
                    syncProps.add(pdmixin);
                }

                if (propNode.getChildNodeCount(1) > 0) {
                    log.warn("Index at [{}] has {} enabled and cannot support other property " +
                            "definitions", indexPath, FulltextIndexConstants.PROP_INDEX_NODE_TYPE);
                }

                return Map.copyOf(propDefns);
            }

            //Include all immediate child nodes to 'properties' node by default
            Tree propTree = TreeFactory.createReadOnlyTree(propNode);
            for (Tree prop : propTree.getChildren()) {
                String propName = prop.getName();
                NodeState propDefnNode = propNode.getChildNode(propName);
                if (propDefnNode.exists() && !propDefns.containsKey(propName)) {
                    PropertyDefinition pd = createPropertyDefinition(this, propName, propDefnNode);
                    if (pd.function != null) {
                        functionRestrictions.add(pd);
                        String[] properties = FunctionIndexProcessor.getProperties(pd.functionCode);
                        for (String p : properties) {
                            if (PathUtils.getDepth(p) > 1) {
                                PropertyDefinition pd2 = createPropertyDefinition(this, p, propDefnNode);
                                propAggregate.add(new Aggregate.FunctionInclude(pd2));
                            }
                        }
                        // a function index has no other options
                        continue;
                    }
                    if (pd.isRegexp) {
                        patterns.add(new NamePattern(pd.name, pd));
                    } else {
                        propDefns.put(pd.name.toLowerCase(Locale.ENGLISH), pd);
                    }

                    if (pd.relative) {
                        propAggregate.add(new Aggregate.PropertyInclude(pd));
                    }

                    if (pd.nullCheckEnabled) {
                        nonExistentProperties.add(pd);
                    }

                    if (pd.notNullCheckEnabled) {
                        existentProperties.add(pd);
                    }

                    //Include props with name, boosted and nodeScopeIndex
                    if (pd.nodeScopeIndex
                            && pd.analyzed
                            && !pd.isRegexp) {
                        nodeScopeAnalyzedProps.add(pd);
                    }

                    if (pd.sync) {
                        syncProps.add(pd);
                    }
                    if (pd.useInSimilarity) {
                        similarityProperties.add(pd);
                    }
                }
            }
            ensureNodeTypeIndexingIsConsistent(propDefns, syncProps);
            return Map.copyOf(propDefns);
        }

        /**
         * If jcr:primaryType is indexed but jcr:mixinTypes is not indexed
         * then ensure that jcr:mixinTypes is also indexed
         */
        private void ensureNodeTypeIndexingIsConsistent(Map<String, PropertyDefinition> propDefns,
                                                        List<PropertyDefinition> syncProps) {
            PropertyDefinition pd_pr = propDefns.get(JcrConstants.JCR_PRIMARYTYPE.toLowerCase(Locale.ENGLISH));
            PropertyDefinition pd_mixin = propDefns.get(JcrConstants.JCR_MIXINTYPES.toLowerCase(Locale.ENGLISH));

            if (pd_pr != null && pd_pr.propertyIndex && pd_mixin == null) {
                pd_mixin = createNodeTypeDefinition(this, JcrConstants.JCR_MIXINTYPES, pd_pr.sync);
                syncProps.add(pd_mixin);
                propDefns.put(JcrConstants.JCR_MIXINTYPES.toLowerCase(Locale.ENGLISH), pd_mixin);
            }
        }

        private boolean hasAnyFullTextEnabledProperty() {
            for (PropertyDefinition pd : propConfigs.values()) {
                if (pd.fulltextEnabled()) {
                    return true;
                }
            }

            for (NamePattern np : namePatterns) {
                if (np.getConfig().fulltextEnabled()) {
                    return true;
                }
            }
            return false;
        }

        private boolean hasAnyPropertyIndexConfigured() {
            for (PropertyDefinition pd : propConfigs.values()) {
                if (pd.propertyIndex) {
                    return true;
                }
            }

            for (NamePattern np : namePatterns) {
                if (np.getConfig().propertyIndex) {
                    return true;
                }
            }
            return false;
        }

        private boolean anyNodeScopeIndexedProperty() {
            //Check if there is any nodeScope indexed property as
            //for such case a node would always be indexed
            for (PropertyDefinition pd : propConfigs.values()) {
                if (pd.nodeScopeIndex) {
                    return true;
                }
            }

            for (NamePattern np : namePatterns) {
                if (np.getConfig().nodeScopeIndex) {
                    return true;
                }
            }

            return false;
        }

        private boolean areAlMatchingNodeByTypeIndexed() {
            if (nodeTypeIndex) {
                return true;
            }

            if (nodeFullTextIndexed) {
                return true;
            }

            //If there is nullCheckEnabled property which is not relative then
            //all nodes would be indexed. relativeProperty with nullCheckEnabled might
            //not ensure that (OAK-1085)
            for (PropertyDefinition pd : nullCheckEnabledProperties) {
                if (!pd.relative) {
                    return true;
                }
            }

            //jcr:primaryType is present on all node. So if such a property
            //is indexed then it would mean all nodes covered by this index rule
            //are indexed
            return getConfig(JcrConstants.JCR_PRIMARYTYPE) != null;

        }

        private boolean evaluateNodeNameIndexed(NodeState config) {
            //check global config first
            if (getOptionalValue(config, FulltextIndexConstants.INDEX_NODE_NAME, false)) {
                return true;
            }

            //iterate over property definitions
            for (PropertyDefinition pd : propConfigs.values()) {
                if (FulltextIndexConstants.PROPDEF_PROP_NODE_NAME.equals(pd.name)) {
                    return true;
                }
            }
            return false;
        }

        private Aggregate combine(Aggregate propAggregate, String nodeTypeName) {
            Aggregate nodeTypeAgg = IndexDefinition.this.getAggregate(nodeTypeName);
            List<Aggregate.Include> includes = new ArrayList<>(propAggregate.getIncludes());
            if (nodeTypeAgg != null) {
                includes.addAll(nodeTypeAgg.getIncludes());
            }
            return new Aggregate(nodeTypeName, includes);
        }

        private void validateRuleDefinition() {
            if (!nullCheckEnabledProperties.isEmpty() && isBasedOnNtBase()) {
                throw new IllegalStateException("nt:base based rule cannot have a " +
                        "PropertyDefinition with nullCheckEnabled");
            }
        }
    }

    /**
     * A property name pattern.
     */
    private static final class NamePattern {
        private final String parentPath;
        /**
         * The pattern to match.
         */
        private final Pattern pattern;

        /**
         * The associated configuration.
         */
        private final PropertyDefinition config;

        /**
         * Creates a new name pattern.
         *
         * @param pattern the pattern as defined by the property definition
         * @param config  the associated configuration.
         */
        private NamePattern(String pattern, PropertyDefinition config) {

            //Special handling for all props regex as its already being used
            //and use of '/' in regex would confuse the parent path calculation
            //logic
            if (FulltextIndexConstants.REGEX_ALL_PROPS.equals(pattern)) {
                this.parentPath = "";
                this.pattern = Pattern.compile(pattern);
            } else {
                this.parentPath = getParentPath(pattern);
                this.pattern = Pattern.compile(PathUtils.getName(pattern));
            }
            this.config = config;
        }

        /**
         * @param propertyPath property name to match
         * @return <code>true</code> if <code>property name</code> matches this name
         * pattern; <code>false</code> otherwise.
         */
        boolean matches(String propertyPath) {
            String parentPath = getParentPath(propertyPath);
            if (!this.parentPath.equals(parentPath)) {
                return false;
            }
            String propertyName = PathUtils.getName(propertyPath);
            return pattern.matcher(propertyName).matches();
        }

        PropertyDefinition getConfig() {
            return config;
        }
    }

    //~---------------------------------------------< compatibility >

    public static NodeBuilder updateDefinition(NodeBuilder indexDefn) {
        return updateDefinition(indexDefn, "unknown");
    }

    public static NodeBuilder updateDefinition(NodeBuilder indexDefn, String indexPath) {
        NodeState defn = indexDefn.getBaseState();
        if (!hasIndexingRules(defn)) {
            NodeState rulesState = createIndexRules(defn).getNodeState();
            indexDefn.setChildNode(FulltextIndexConstants.INDEX_RULES, rulesState);
            indexDefn.setProperty(INDEX_VERSION, determineIndexFormatVersion(defn).getVersion());

            indexDefn.removeProperty(DECLARING_NODE_TYPES);
            indexDefn.removeProperty(INCLUDE_PROPERTY_NAMES);
            indexDefn.removeProperty(EXCLUDE_PROPERTY_NAMES);
            indexDefn.removeProperty(ORDERED_PROP_NAMES);
            indexDefn.removeProperty(FULL_TEXT_ENABLED);
            indexDefn.child(PROP_NODE).remove();
            log.info("Updated index definition for {}", indexPath);
        }
        return indexDefn;
    }

    /**
     * Constructs IndexingRule based on earlier format of index configuration
     */
    private static NodeBuilder createIndexRules(NodeState defn) {
        NodeBuilder builder = EMPTY_NODE.builder();
        Set<String> declaringNodeTypes = getMultiProperty(defn, DECLARING_NODE_TYPES);
        Set<String> includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        Set<String> excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        Set<String> orderedProps = getMultiProperty(defn, ORDERED_PROP_NAMES);
        boolean fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        boolean storageEnabled = getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);
        NodeState propNodeState = defn.getChildNode(FulltextIndexConstants.PROP_NODE);

        //If no explicit nodeType defined then all config applies for nt:base
        if (declaringNodeTypes.isEmpty()) {
            declaringNodeTypes = Set.of(NT_BASE);
        }

        Set<String> propNamesSet = new HashSet<>();
        propNamesSet.addAll(includes);
        propNamesSet.addAll(excludes);
        propNamesSet.addAll(orderedProps);

        //Also include all immediate leaf propNode names
        for (ChildNodeEntry cne : propNodeState.getChildNodeEntries()) {
            if (!propNamesSet.contains(cne.getName())
                    && Iterables.isEmpty(cne.getNodeState().getChildNodeNames())) {
                propNamesSet.add(cne.getName());
            }
        }

        List<String> propNames = new ArrayList<>(propNamesSet);

        final String includeAllProp = FulltextIndexConstants.REGEX_ALL_PROPS;
        if (fullTextEnabled && includes.isEmpty()) {
            //Add the regEx for including all properties at the end
            //for fulltext index and when no explicit includes are defined
            propNames.add(includeAllProp);
        }

        for (String typeName : declaringNodeTypes) {
            NodeBuilder rule = builder.child(typeName);
            markAsNtUnstructured(rule);
            List<String> propNodeNames = new ArrayList<>(propNamesSet.size());
            NodeBuilder propNodes = rule.child(PROP_NODE);
            int i = 0;
            for (String propName : propNames) {
                String propNodeName = propName;

                //For proper propName use the propName as childNode name
                if (PropertyDefinition.isRelativeProperty(propName)
                        || propName.equals(includeAllProp)) {
                    propNodeName = "prop" + i++;
                }
                propNodeNames.add(propNodeName);

                NodeBuilder prop = propNodes.child(propNodeName);
                markAsNtUnstructured(prop);
                prop.setProperty(FulltextIndexConstants.PROP_NAME, propName);

                if (excludes.contains(propName)) {
                    prop.setProperty(FulltextIndexConstants.PROP_INDEX, false);
                } else if (fullTextEnabled) {
                    prop.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
                    prop.setProperty(FulltextIndexConstants.PROP_NODE_SCOPE_INDEX, true);
                    prop.setProperty(FulltextIndexConstants.PROP_USE_IN_EXCERPT, storageEnabled);
                    prop.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, false);
                } else {
                    prop.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);

                    if (orderedProps.contains(propName)) {
                        prop.setProperty(FulltextIndexConstants.PROP_ORDERED, true);
                    }
                }

                if (propName.equals(includeAllProp)) {
                    prop.setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
                } else {
                    //Copy over the property configuration
                    NodeState propDefNode = getPropDefnNode(defn, propName);
                    if (propDefNode != null) {
                        for (PropertyState ps : propDefNode.getProperties()) {
                            prop.setProperty(ps);
                        }
                    }
                }
            }

            //If no propertyType defined then default to UNKNOWN such that none
            //of the properties get indexed
            PropertyState supportedTypes = defn.getProperty(INCLUDE_PROPERTY_TYPES);
            if (supportedTypes == null) {
                supportedTypes = PropertyStates.createProperty(INCLUDE_PROPERTY_TYPES, TYPES_ALLOW_ALL_NAME);
            }
            rule.setProperty(supportedTypes);

            if (!NT_BASE.equals(typeName)) {
                rule.setProperty(FulltextIndexConstants.RULE_INHERITED, false);
            }

            propNodes.setProperty(OAK_CHILD_ORDER, propNodeNames, NAMES);
            markAsNtUnstructured(propNodes);
        }

        markAsNtUnstructured(builder);
        builder.setProperty(OAK_CHILD_ORDER, declaringNodeTypes, NAMES);
        return builder;
    }

    private static NodeState getPropDefnNode(NodeState defn, String propName) {
        NodeState propNode = defn.getChildNode(FulltextIndexConstants.PROP_NODE);
        NodeState propDefNode;
        if (PropertyDefinition.isRelativeProperty(propName)) {
            NodeState result = propNode;
            for (String name : PathUtils.elements(propName)) {
                result = result.getChildNode(name);
            }
            propDefNode = result;
        } else {
            propDefNode = propNode.getChildNode(propName);
        }
        return propDefNode.exists() ? propDefNode : null;
    }

    //~---------------------------------------------< utility >

    private int determineMaxExtractLength() {
        int length = getOptionalValue(definition.getChildNode(TIKA), FulltextIndexConstants.TIKA_MAX_EXTRACT_LENGTH,
                DEFAULT_MAX_EXTRACT_LENGTH);
        if (length < 0) {
            return -length * maxFieldLength;
        }
        return length;
    }

    private NodeState getTikaConfigNode() {
        return definition.getChildNode(TIKA).getChildNode(TIKA_CONFIG);
    }

    private static Set<String> getMultiProperty(NodeState definition, String propName) {
        PropertyState pse = definition.getProperty(propName);
        return pse != null ? ImmutableSet.copyOf(pse.getValue(Type.STRINGS)) : Set.of();
    }

    private static Set<String> toLowerCase(Set<String> values) {
        Set<String> result = new HashSet<>();
        for (String val : values) {
            result.add(val.toLowerCase(Locale.ENGLISH));
        }
        return Set.copyOf(result);
    }

    private static List<String> getAllNodeTypes(ReadOnlyNodeTypeManager ntReg) {
        try {
            List<String> typeNames = new ArrayList<>();
            NodeTypeIterator ntItr = ntReg.getAllNodeTypes();
            while (ntItr.hasNext()) {
                typeNames.add(ntItr.nextNodeType().getName());
            }
            return typeNames;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    private static ReadOnlyNodeTypeManager createNodeTypeManager(final Root root) {
        return new ReadOnlyNodeTypeManager() {
            @NotNull
            @Override
            protected Tree getTypes() {
                return root.getTree(NODE_TYPES_PATH);
            }

            @NotNull
            @Override
            protected NamePathMapper getNamePathMapper() {
                return NamePathMapper.DEFAULT;
            }
        };
    }

    private static String getPrimaryTypeName(NodeState state) {
        String primaryType = state.getName(JcrConstants.JCR_PRIMARYTYPE);

        //To ensure compatibility with previous Tree based usage look based on string also
        if (primaryType == null) {
            primaryType = state.getString(JcrConstants.JCR_PRIMARYTYPE);
        }
        //In case not a proper JCR assume nt:base TODO return null and ignore indexing such nodes
        //at all
        return primaryType != null ? primaryType : "nt:base";
    }

    private static Iterable<String> getMixinTypeNames(NodeState state) {
        PropertyState property = state.getProperty(JcrConstants.JCR_MIXINTYPES);
        return property != null ? property.getValue(NAMES) : List.of();
    }

    private static boolean hasOrderableChildren(NodeState state) {
        return state.hasProperty(OAK_CHILD_ORDER);
    }

    static int getSupportedTypes(NodeState defn, String typePropertyName, int defaultVal) {
        PropertyState pst = defn.getProperty(typePropertyName);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                if (TYPES_ALLOW_ALL_NAME.equals(inc)) {
                    return TYPES_ALLOW_ALL;
                }

                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: {}", inc);
                }
            }
            return types;
        }
        return defaultVal;
    }

    static boolean includePropertyType(int includedPropertyTypes, int type) {
        if (includedPropertyTypes == TYPES_ALLOW_ALL) {
            return true;
        }

        if (includedPropertyTypes == TYPES_ALLOW_NONE) {
            return false;
        }

        return (includedPropertyTypes & (1 << type)) != 0;
    }

    private static boolean hasFulltextEnabledIndexRule(List<IndexingRule> rules) {
        for (IndexingRule rule : rules) {
            if (rule.isFulltextEnabled()) {
                return true;
            }
        }
        return false;
    }

    private static void markAsNtUnstructured(NodeBuilder nb) {
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
    }

    public static IndexFormatVersion determineIndexFormatVersion(NodeState defn) {
        //Compat mode version if specified has highest priority
        if (defn.hasProperty(COMPAT_MODE)) {
            return versionFrom(defn.getProperty(COMPAT_MODE));
        }

        if (defn.hasProperty(INDEX_VERSION)) {
            return versionFrom(defn.getProperty(INDEX_VERSION));
        }

        //No existing index data i.e. reindex or fresh index
        if (!defn.getChildNode(INDEX_DATA_CHILD_NAME).exists()) {
            return determineVersionForFreshIndex(defn);
        }

        boolean fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);

        //A fulltext index with old indexing format confirms to V1. However
        //a propertyIndex with old indexing format confirms to V2
        return fullTextEnabled ? IndexFormatVersion.V1 : IndexFormatVersion.V2;
    }

    static IndexFormatVersion determineVersionForFreshIndex(NodeState defn) {
        return determineVersionForFreshIndex(defn.getProperty(FULL_TEXT_ENABLED),
                defn.getProperty(COMPAT_MODE), defn.getProperty(INDEX_VERSION));
    }

    static IndexFormatVersion determineVersionForFreshIndex(NodeBuilder defnb) {
        return determineVersionForFreshIndex(defnb.getProperty(FULL_TEXT_ENABLED),
                defnb.getProperty(COMPAT_MODE), defnb.getProperty(INDEX_VERSION));
    }

    private static IndexFormatVersion determineVersionForFreshIndex(PropertyState fulltext,
                                                                    PropertyState compat,
                                                                    PropertyState version) {
        if (compat != null) {
            return versionFrom(compat);
        }

        IndexFormatVersion defaultToUse = IndexFormatVersion.getDefault();
        IndexFormatVersion existing = version != null ? versionFrom(version) : null;

        //As per OAK-2290 current might be less than current used version. So
        //set to current only if it is greater than existing

        //Per setting use default configured
        IndexFormatVersion result = defaultToUse;

        //If default configured is lesser than existing then prefer existing
        if (existing != null) {
            result = IndexFormatVersion.max(result, existing);
        }

        //Check if fulltext is false which indicates it's a property index and
        //hence confirm to V2 or above
        if (fulltext != null && !fulltext.getValue(Type.BOOLEAN)) {
            return IndexFormatVersion.max(result, IndexFormatVersion.V2);
        }

        return result;
    }

    private static IndexFormatVersion versionFrom(PropertyState ps) {
        return IndexFormatVersion.getVersion(Ints.checkedCast(ps.getValue(Type.LONG)));
    }

    private static boolean hasIndexingRules(NodeState defn) {
        return defn.getChildNode(FulltextIndexConstants.INDEX_RULES).exists();
    }

    @Nullable
    protected static String determineUniqueId(NodeState defn) {
        return defn.getChildNode(STATUS_NODE).getString(PROP_UID);
    }

    private static boolean supportsNRTIndexing(NodeState defn) {
        return supportsIndexingMode(new ReadOnlyBuilder(defn), INDEXING_MODE_NRT);
    }

    private static boolean supportsSyncIndexing(NodeState defn) {
        return supportsIndexingMode(new ReadOnlyBuilder(defn), INDEXING_MODE_SYNC);
    }

    public static boolean supportsSyncOrNRTIndexing(NodeBuilder defn) {
        return supportsIndexingMode(defn, INDEXING_MODE_NRT) || supportsIndexingMode(defn, INDEXING_MODE_SYNC);
    }

    private static boolean supportsIndexingMode(NodeBuilder defn, String mode) {
        PropertyState async = defn.getProperty(IndexConstants.ASYNC_PROPERTY_NAME);
        if (async == null) {
            return false;
        }
        return Iterables.contains(async.getValue(Type.STRINGS), mode);
    }

    protected static NodeState getIndexDefinitionState(NodeState defn) {
        if (isDisableStoredIndexDefinition()) {
            return defn;
        }
        NodeState storedState = defn.getChildNode(INDEX_DEFINITION_NODE);
        return storedState.exists() ? storedState : defn;
    }

    protected PropertyDefinition createPropertyDefinition(IndexingRule rule, String name, NodeState nodeState) {
        return new PropertyDefinition(rule, name, nodeState);
    }

    private static Map<String, String> buildMimeTypeMap(NodeState node) {
        Map<String, String> map = new HashMap<>();
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            for (ChildNodeEntry subChild : child.getNodeState().getChildNodeEntries()) {
                StringBuilder typeBuilder = new StringBuilder(child.getName())
                        .append('/')
                        .append(subChild.getName());
                PropertyState property = subChild.getNodeState().getProperty(TIKA_MAPPED_TYPE);
                if (property != null) {
                    map.put(typeBuilder.toString(), property.getValue(Type.STRING));
                }
            }
        }
        return Collections.unmodifiableMap(map);
    }

    private PropertyDefinition createNodeTypeDefinition(IndexingRule rule, String name, boolean sync) {
        NodeBuilder builder = EMPTY_NODE.builder();
        //A nodetype index just required propertyIndex and sync flags to be set
        builder.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        if (sync) {
            builder.setProperty(FulltextIndexConstants.PROP_SYNC, sync);
        }
        builder.setProperty(FulltextIndexConstants.PROP_NAME, name);
        return createPropertyDefinition(rule, name, builder.getNodeState());
    }

    public static class SecureFacetConfiguration {
        public enum MODE {
            SECURE,
            STATISTICAL,
            INSECURE
        }

        private final long randomSeed;
        private final MODE mode;
        private final int statisticalFacetSampleSize;

        SecureFacetConfiguration(long randomSeed, MODE mode, int statisticalFacetSampleSize) {
            this.randomSeed = randomSeed;
            this.mode = mode;
            this.statisticalFacetSampleSize = statisticalFacetSampleSize;
        }

        public MODE getMode() {
            return mode;
        }

        public int getStatisticalFacetSampleSize() {
            return statisticalFacetSampleSize;
        }

        public long getRandomSeed() {
            return randomSeed;
        }

        static SecureFacetConfiguration getInstance(long randomSeed, NodeState facetConfigRoot) {
            if (facetConfigRoot == null) {
                facetConfigRoot = EMPTY_NODE;
            }

            MODE mode = getMode(facetConfigRoot);
            int statisticalFacetSampleSize;

            if (mode == MODE.STATISTICAL) {
                statisticalFacetSampleSize = getStatisticalFacetSampleSize(facetConfigRoot);
            } else {
                statisticalFacetSampleSize = -1;
            }

            return new SecureFacetConfiguration(randomSeed, mode, statisticalFacetSampleSize);
        }

        static MODE getMode(@NotNull final NodeState facetConfigRoot) {
            PropertyState securePS = facetConfigRoot.getProperty(PROP_SECURE_FACETS);
            String modeString;

            if (securePS != null) {
                if (securePS.getType() == Type.BOOLEAN) {
                    // legacy secure config
                    boolean secure = securePS.getValue(Type.BOOLEAN);
                    return secure ? MODE.SECURE : MODE.INSECURE;
                } else {
                    modeString = securePS.getValue(Type.STRING);
                }
            } else {
                // use "default" just as placeholder. default case would do the actual default eval
                modeString = System.getProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "default");
            }

            switch (modeString) {
                case PROP_SECURE_FACETS_VALUE_INSECURE:
                    return MODE.INSECURE;
                case PROP_SECURE_FACETS_VALUE_STATISTICAL:
                    return MODE.STATISTICAL;
                case PROP_SECURE_FACETS_VALUE_SECURE:
                default:
                    return MODE.SECURE;
            }
        }

        static int getStatisticalFacetSampleSize(@NotNull final NodeState facetConfigRoot) {
            int statisticalFacetSampleSize = Integer.getInteger(
                    STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM,
                    STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT);
            int statisticalFacetSampleSizePV = getOptionalValue(facetConfigRoot,
                    PROP_STATISTICAL_FACET_SAMPLE_SIZE,
                    statisticalFacetSampleSize);

            if (statisticalFacetSampleSizePV > 0) {
                statisticalFacetSampleSize = statisticalFacetSampleSizePV;
            }

            if (statisticalFacetSampleSize < 0) {
                statisticalFacetSampleSize = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
            }

            return statisticalFacetSampleSize;
        }
    }
}
