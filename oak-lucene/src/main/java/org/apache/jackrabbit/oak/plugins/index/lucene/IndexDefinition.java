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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeTypeIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.TokenizerChain;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.CommitMitigatingTieredMergePolicy;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.path.PathHierarchyTokenizerFactory;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_SCORE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_NRT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEXING_MODE_SYNC;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition.DEFAULT_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getOptionalValue;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;

public final class IndexDefinition implements Aggregate.AggregateMapper {
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
    static final long DEFAULT_ENTRY_COUNT = 1000;

    /**
     * Default value for property {@link #maxFieldLength}.
     */
    public static final int DEFAULT_MAX_FIELD_LENGTH = 10000;

    static final int DEFAULT_MAX_EXTRACT_LENGTH = -10;

    /**
     * System managed hidden property to record the current index version
     */
    static final String INDEX_VERSION = ":version";

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
     * Property on status node which refers to the date when the index was lastUpdated
     * This may not be the same time as when index was closed but the time of checkpoint
     * upto which index is upto date (OAK-6194)
     */
    static final String STATUS_LAST_UPDATED = "lastUpdated";

    /**
     * Meta property which provides the unique id
     */
    public static final String PROP_UID = "uid";

    private static String TYPES_ALLOW_ALL_NAME = "all";

    static final int TYPES_ALLOW_NONE = PropertyType.UNDEFINED;

    static final int TYPES_ALLOW_ALL = -1;

    /**
     * Default suggesterUpdateFrequencyMinutes
     */
    static final int DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES = 10;

    /**
     * Default no. of facets retrieved
     */
    static final int DEFAULT_FACET_COUNT = 10;

    /**
     * native sort order
     */
    static final OrderEntry NATIVE_SORT_ORDER = new OrderEntry(JCR_SCORE, Type.UNDEFINED,
        OrderEntry.Order.DESCENDING);

    private final boolean fullTextEnabled;

    private final NodeState definition;

    private final NodeState root;

    private final String funcName;

    private final int blobSize;

    private final Codec codec;

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

    private final boolean testMode;

    private final boolean evaluatePathRestrictions;

    private final IndexFormatVersion version;

    private final Map<String, Aggregate> aggregates;

    private final boolean indexesAllTypes;

    private final Analyzer analyzer;

    private final String scorerProviderName;

    private final Map<String, Analyzer> analyzers;

    private final boolean hasCustomTikaConfig;

    private final Map<String, String> customTikaMimeTypeMappings;

    private final int maxFieldLength;

    private final int maxExtractLength;

    private final int suggesterUpdateFrequencyMinutes;

    private final long reindexCount;

    private final PathFilter pathFilter;

    @Nullable
    private final String[] queryPaths;

    private final boolean saveDirListing;

    private final boolean suggestAnalyzed;

    private final boolean secureFacets;

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

    private final boolean syncPropertyIndexes;

    //~--------------------------------------------------------< Builder >

    public static Builder newBuilder(NodeState root, NodeState defn, String indexPath){
        return new Builder(root, defn, indexPath);
    }

    public static class Builder {
        /**
         * Default unique id used when no existing uid is defined
         * and index is not populated
         */
        private static final String DEFAULT_UID = "0";
        private final NodeState root;
        private final NodeState defn;
        private final String indexPath;
        private IndexFormatVersion version;
        private String uid;
        private boolean reindexMode;

        public Builder(NodeState root, NodeState defn, String indexPath) {
            this.root = checkNotNull(root);
            this.defn = checkNotNull(defn);
            this.indexPath = checkNotNull(indexPath);
        }

        public Builder version(IndexFormatVersion version){
            this.version = version;
            return this;
        }

        public Builder uid(String uid){
            this.uid = uid;
            return this;
        }

        public Builder reindex(){
            this.reindexMode = true;
            return this;
        }

        public IndexDefinition build(){
            if (version == null){
                version = determineIndexFormatVersion(defn);
            }
            if (uid == null){
                uid = determineUniqueId(defn);
                if (uid == null && !IndexDefinition.hasPersistedIndex(defn)){
                    uid = DEFAULT_UID;
                }
            }

            NodeState indexDefnStateToUse = defn;
            if (!reindexMode){
                indexDefnStateToUse = getIndexDefinitionState(defn);
            }
            return new IndexDefinition(root, indexDefnStateToUse, version, uid, indexPath);
        }
    }

    public IndexDefinition(NodeState root, NodeState defn, String indexPath) {
        this(root, getIndexDefinitionState(defn), determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
    }

    private IndexDefinition(NodeState root, NodeState defn, IndexFormatVersion version, String uid, String indexPath) {
        this.root = root;
        this.version = checkNotNull(version);
        this.uid = uid;
        this.definition = defn;
        this.indexPath = checkNotNull(indexPath);
        this.indexName = indexPath;
        this.indexTags = getOptionalStrings(defn, IndexConstants.INDEX_TAGS);
        this.nodeTypeIndex = getOptionalValue(defn, LuceneIndexConstants.PROP_INDEX_NODE_TYPE, false);

        this.blobSize = getOptionalValue(defn, BLOB_SIZE, DEFAULT_BLOB_SIZE);
        this.testMode = getOptionalValue(defn, LuceneIndexConstants.TEST_MODE, false);

        this.aggregates = nodeTypeIndex ? Collections.emptyMap() : collectAggregates(defn);

        NodeState rulesState = defn.getChildNode(LuceneIndexConstants.INDEX_RULES);
        if (!rulesState.exists()){
            rulesState = createIndexRules(defn).getNodeState();
        }

        List<IndexingRule> definedIndexRules = newArrayList();
        this.indexRules = collectIndexRules(rulesState, definedIndexRules);
        this.definedRules = ImmutableList.copyOf(definedIndexRules);

        this.fullTextEnabled = hasFulltextEnabledIndexRule(definedIndexRules);
        this.evaluatePathRestrictions = getOptionalValue(defn, EVALUATE_PATH_RESTRICTION, false);

        String functionName = getOptionalValue(defn, LuceneIndexConstants.FUNC_NAME, null);
        if (fullTextEnabled && functionName == null){
            functionName = "lucene";
        }
        this.funcName = functionName != null ? "native*" + functionName : null;

        this.codec = createCodec();

        if (defn.hasProperty(ENTRY_COUNT_PROPERTY_NAME)) {
            this.entryCountDefined = true;
            this.entryCount = defn.getProperty(ENTRY_COUNT_PROPERTY_NAME).getValue(Type.LONG);
        } else {
            this.entryCountDefined = false;
            this.entryCount = DEFAULT_ENTRY_COUNT;
        }

        this.maxFieldLength = getOptionalValue(defn, LuceneIndexConstants.MAX_FIELD_LENGTH, DEFAULT_MAX_FIELD_LENGTH);
        this.costPerEntry = getOptionalValue(defn, LuceneIndexConstants.COST_PER_ENTRY, getDefaultCostPerEntry(version));
        this.costPerExecution = getOptionalValue(defn, LuceneIndexConstants.COST_PER_EXECUTION, 1.0);
        this.indexesAllTypes = areAllTypesIndexed();
        this.analyzers = collectAnalyzers(defn);
        this.analyzer = createAnalyzer();
        this.hasCustomTikaConfig = getTikaConfigNode().exists();
        this.customTikaMimeTypeMappings = buildMimeTypeMap(definition.getChildNode(TIKA).getChildNode(TIKA_MIME_TYPES));
        this.maxExtractLength = determineMaxExtractLength();
        this.suggesterUpdateFrequencyMinutes = evaluateSuggesterUpdateFrequencyMinutes(defn,
                DEFAULT_SUGGESTER_UPDATE_FREQUENCY_MINUTES);
        this.scorerProviderName = getOptionalValue(defn, LuceneIndexConstants.PROP_SCORER_PROVIDER, null);
        this.reindexCount = getOptionalValue(defn, REINDEX_COUNT, 0);
        this.pathFilter = PathFilter.from(new ReadOnlyBuilder(defn));
        this.queryPaths = getOptionalStrings(defn, IndexConstants.QUERY_PATHS);
        this.saveDirListing = getOptionalValue(defn, LuceneIndexConstants.SAVE_DIR_LISTING, true);
        this.suggestAnalyzed = evaluateSuggestAnalyzed(defn, false);

        if (defn.hasChildNode(FACETS)) {
            NodeState facetsConfig =  defn.getChildNode(FACETS);
            this.secureFacets = getOptionalValue(facetsConfig, PROP_SECURE_FACETS, true);
            this.numberOfTopFacets = getOptionalValue(facetsConfig, PROP_FACETS_TOP_CHILDREN, DEFAULT_FACET_COUNT);
        } else {
            this.secureFacets = true;
            this.numberOfTopFacets = DEFAULT_FACET_COUNT;
        }

        this.suggestEnabled = evaluateSuggestionEnabled();
        this.spellcheckEnabled = evaluateSpellcheckEnabled();
        this.nrtIndexMode = supportsNRTIndexing(defn);
        this.syncIndexMode = supportsSyncIndexing(defn);
        this.syncPropertyIndexes = definedRules.stream().anyMatch(ir -> !ir.syncProps.isEmpty());
    }

    public NodeState getDefinitionNodeState() {
        return definition;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public String getFunctionName(){
        return funcName;
    }

    public boolean hasFunctionDefined(){
        return funcName != null;
    }

    /**
     * Size in bytes for the blobs created while storing the index content
     * @return size in bytes
     */
    public int getBlobSize() {
        return blobSize;
    }

    @CheckForNull
    public Codec getCodec() {
        return codec;
    }

    @Nonnull
    public MergePolicy getMergePolicy() {
        // MP is not cached to avoid complaining about multiple IWs with multiplexing writers
        return createMergePolicy();
    }

    public long getReindexCount(){
        return reindexCount;
    }

    public long getEntryCount() {
        return entryCount;
    }

    private int evaluateSuggesterUpdateFrequencyMinutes(NodeState defn, int defaultValue) {
        NodeState suggestionConfig = defn.getChildNode(LuceneIndexConstants.SUGGESTION_CONFIG);

        if (!suggestionConfig.exists()) {
            //handle backward compatibility
            return getOptionalValue(defn, LuceneIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES, defaultValue);
        }

        return getOptionalValue(suggestionConfig, LuceneIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES, defaultValue);
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

    public double getCostPerExecution() {
        return costPerExecution;
    }

    public long getFulltextEntryCount(long numOfDocs){
        if (isEntryCountDefined()){
            return Math.min(getEntryCount(), numOfDocs);
        }
        return numOfDocs;
    }

    public IndexFormatVersion getVersion() {
        return version;
    }

    public boolean isOfOldFormat(){
        return !hasIndexingRules(definition);
    }

    public boolean isTestMode() {
        return testMode;
    }

    public boolean evaluatePathRestrictions() {
        return evaluatePathRestrictions;
    }

    public Analyzer getAnalyzer(){
        return analyzer;
    }

    public boolean hasCustomTikaConfig(){
        return hasCustomTikaConfig;
    }

    public InputStream getTikaConfig(){
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

    public int getMaxExtractLength() {
        return maxExtractLength;
    }

    public String getScorerProviderName() {
        return scorerProviderName;
    }

    public boolean saveDirListing() {
        return saveDirListing;
    }

    public PathFilter getPathFilter() {
        return pathFilter;
    }

    @Nullable
    public String[] getQueryPaths() {
        return queryPaths;
    }

    @CheckForNull
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
     * Check if the index definition is fresh of some index has happened
     *
     * @param definition nodestate for Index Definition
     * @return true if index has some indexed content
     */
    public static boolean hasPersistedIndex(NodeState definition){
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

    public Set<String> getRelativeNodeNames(){
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

    public boolean indexesRelativeNodes(){
        for (IndexingRule r : definedRules) {
            if (!r.aggregate.getIncludes().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Lucene Index : " + indexName;
    }

    //~---------------------------------------------------< Analyzer >

    private Analyzer createAnalyzer() {
        Analyzer result;
        Analyzer defaultAnalyzer = LuceneIndexConstants.ANALYZER;
        if (analyzers.containsKey(LuceneIndexConstants.ANL_DEFAULT)){
            defaultAnalyzer = analyzers.get(LuceneIndexConstants.ANL_DEFAULT);
        }
        if (!evaluatePathRestrictions()){
            result = defaultAnalyzer;
        } else {
            Map<String, Analyzer> analyzerMap = ImmutableMap.<String, Analyzer>builder()
                    .put(FieldNames.ANCESTORS,
                            new TokenizerChain(new PathHierarchyTokenizerFactory(Collections.<String, String>emptyMap())))
                    .build();
            result = new PerFieldAnalyzerWrapper(defaultAnalyzer, analyzerMap);
        }

        //In case of negative value no limits would be applied
        if (maxFieldLength < 0){
            return result;
        }
        return new LimitTokenCountAnalyzer(result, maxFieldLength);
    }

    private static Map<String, Analyzer> collectAnalyzers(NodeState defn) {
        Map<String, Analyzer> analyzerMap = newHashMap();
        NodeStateAnalyzerFactory factory = new NodeStateAnalyzerFactory(LuceneIndexConstants.VERSION);
        NodeState analyzersTree = defn.getChildNode(LuceneIndexConstants.ANALYZERS);
        for (ChildNodeEntry cne : analyzersTree.getChildNodeEntries()) {
            Analyzer a = factory.createInstance(cne.getNodeState());
            analyzerMap.put(cne.getName(), a);
        }

        if (getOptionalValue(analyzersTree, INDEX_ORIGINAL_TERM, false) && !analyzerMap.containsKey(ANL_DEFAULT)) {
            analyzerMap.put(ANL_DEFAULT, new OakAnalyzer(VERSION, true));
        }

        return ImmutableMap.copyOf(analyzerMap);
    }

    //~---------------------------------------------------< Aggregates >

    @CheckForNull
    public Aggregate getAggregate(String nodeType){
        Aggregate agg = aggregates.get(nodeType);
        return agg != null ? agg : null;
    }

    private Map<String, Aggregate> collectAggregates(NodeState defn) {
        Map<String, Aggregate> aggregateMap = newHashMap();

        for (ChildNodeEntry cne : defn.getChildNode(LuceneIndexConstants.AGGREGATES).getChildNodeEntries()) {
            String nodeType = cne.getName();
            int recursionLimit = getOptionalValue(cne.getNodeState(), LuceneIndexConstants.AGG_RECURSIVE_LIMIT,
                    Aggregate.RECURSIVE_AGGREGATION_LIMIT_DEFAULT);

            List<Aggregate.Include> includes = newArrayList();
            for (ChildNodeEntry include : cne.getNodeState().getChildNodeEntries()) {
                NodeState is = include.getNodeState();
                String primaryType = is.getString(LuceneIndexConstants.AGG_PRIMARY_TYPE);
                String path = is.getString(LuceneIndexConstants.AGG_PATH);
                boolean relativeNode = getOptionalValue(is, LuceneIndexConstants.AGG_RELATIVE_NODE, false);
                if (path == null) {
                    log.warn("Aggregate pattern in {} does not have required property [{}]. {} aggregate rule would " +
                            "be ignored", this, LuceneIndexConstants.AGG_PATH, include.getName());
                    continue;
                }
                includes.add(new Aggregate.NodeInclude(this, primaryType, path, relativeNode));
            }
            aggregateMap.put(nodeType, new Aggregate(nodeType, includes, recursionLimit));
        }

        return ImmutableMap.copyOf(aggregateMap);
    }

    //~---------------------------------------------------< IndexRule >

    public boolean hasMatchingNodeTypeReg(NodeState root){
        return this.root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES)
                .equals(root.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES));
    }


    public List<IndexingRule> getDefinedRules() {
        return definedRules;
    }

    @CheckForNull
    public IndexingRule getApplicableIndexingRule(String primaryNodeType) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(primaryNodeType);
        if (r != null) {
            rules = new ArrayList<IndexingRule>();
            rules.addAll(r);
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
    @CheckForNull
    public IndexingRule getApplicableIndexingRule(NodeState state) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(getPrimaryTypeName(state));
        if (r != null) {
            rules = new ArrayList<IndexingRule>();
            rules.addAll(r);
        }

        for (String name : getMixinTypeNames(state)) {
            r = indexRules.get(name);
            if (r != null) {
                if (rules == null) {
                    rules = new ArrayList<IndexingRule>();
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
                                                              List<IndexingRule> definedIndexRules){
        //TODO if a rule is defined for nt:base then this map would have entry for each
        //registered nodeType in the system

        if (!indexRules.exists()) {
            return Collections.emptyMap();
        }

        if (!hasOrderableChildren(indexRules)){
            log.warn("IndexRule node does not have orderable children in [{}]", IndexDefinition.this);
        }

        Map<String, List<IndexingRule>> nt2rules = newHashMap();
        ReadOnlyNodeTypeManager ntReg = createNodeTypeManager(TreeFactory.createReadOnlyTree(root));

        //Use Tree API to read ordered child nodes
        Tree ruleTree = TreeFactory.createReadOnlyTree(indexRules);
        final List<String> allNames = getAllNodeTypes(ntReg);
        for (Tree ruleEntry : ruleTree.getChildren()) {
            IndexingRule rule = new IndexingRule(ruleEntry.getName(), indexRules.getChildNode(ruleEntry.getName()));
            definedIndexRules.add(rule);

            // register under node type and all its sub types
            log.trace("Found rule '{}' for NodeType '{}'", rule, rule.getNodeTypeName());

            List<String> ntNames = allNames;
            if (!rule.inherited){
                //Trim the list to rule's nodeType so that inheritance check
                //is not performed for other nodeTypes
                ntNames = Collections.singletonList(rule.getNodeTypeName());
            }

            for (String ntName : ntNames) {
                if (ntReg.isNodeType(ntName, rule.getNodeTypeName())) {
                    List<IndexingRule> perNtConfig = nt2rules.get(ntName);
                    if (perNtConfig == null) {
                        perNtConfig = new ArrayList<IndexingRule>();
                        nt2rules.put(ntName, perNtConfig);
                    }
                    log.trace("Registering rule '{}' for name '{}'", rule, ntName);
                    perNtConfig.add(new IndexingRule(rule, ntName));
                }
            }
        }

        for (Map.Entry<String, List<IndexingRule>> e : nt2rules.entrySet()){
            e.setValue(ImmutableList.copyOf(e.getValue()));
        }

        return ImmutableMap.copyOf(nt2rules);
    }

    private boolean areAllTypesIndexed() {
        IndexingRule ntBaseRule = getApplicableIndexingRule(NT_BASE);
        return ntBaseRule != null;
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
        NodeState suggestionConfig = defn.getChildNode(LuceneIndexConstants.SUGGESTION_CONFIG);

        if (!suggestionConfig.exists()) {
            //handle backward compatibility
            return getOptionalValue(defn, LuceneIndexConstants.SUGGEST_ANALYZED, defaultValue);
        }

        return getOptionalValue(suggestionConfig, LuceneIndexConstants.SUGGEST_ANALYZED, defaultValue);
    }

    public boolean isSuggestAnalyzed() {
        return suggestAnalyzed;
    }

    public boolean isSecureFacets() {
        return secureFacets;
    }

    public int getNumberOfTopFacets() {
        return numberOfTopFacets;
    }

    public class IndexingRule {
        private final String baseNodeType;
        private final String nodeTypeName;
        /**
         * Case insensitive map of lower cased propertyName to propertyConfigs
         */
        private final Map<String, PropertyDefinition> propConfigs;
        private final List<NamePattern> namePatterns;
        private final List<PropertyDefinition> nullCheckEnabledProperties;
        private final List<PropertyDefinition> functionRestrictions;
        private final List<PropertyDefinition> notNullCheckEnabledProperties;
        private final List<PropertyDefinition> nodeScopeAnalyzedProps;
        private final List<PropertyDefinition> syncProps;
        private final boolean indexesAllNodesOfMatchingType;
        private final boolean nodeNameIndexed;

        final float boost;
        final boolean inherited;
        final int propertyTypes;
        final boolean fulltextEnabled;
        final boolean propertyIndexEnabled;
        final boolean nodeFullTextIndexed;

        final Aggregate aggregate;
        final Aggregate propAggregate;


        IndexingRule(String nodeTypeName, NodeState config) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = nodeTypeName;
            this.boost = getOptionalValue(config, FIELD_BOOST, DEFAULT_BOOST);
            this.inherited = getOptionalValue(config, LuceneIndexConstants.RULE_INHERITED, true);
            this.propertyTypes = getSupportedTypes(config, INCLUDE_PROPERTY_TYPES, TYPES_ALLOW_ALL);

            List<NamePattern> namePatterns = newArrayList();
            List<PropertyDefinition> nonExistentProperties = newArrayList();
            List<PropertyDefinition> functionRestrictions = newArrayList();
            List<PropertyDefinition> existentProperties = newArrayList();
            List<PropertyDefinition> nodeScopeAnalyzedProps = newArrayList();
            List<PropertyDefinition> syncProps = newArrayList();
            List<Aggregate.Include> propIncludes = newArrayList();
            this.propConfigs = collectPropConfigs(config, namePatterns, propIncludes, nonExistentProperties,
                    existentProperties, nodeScopeAnalyzedProps, functionRestrictions, syncProps);
            this.propAggregate = new Aggregate(nodeTypeName, propIncludes);
            this.aggregate = combine(propAggregate, nodeTypeName);

            this.namePatterns = ImmutableList.copyOf(namePatterns);
            this.nodeScopeAnalyzedProps = ImmutableList.copyOf(nodeScopeAnalyzedProps);
            this.nullCheckEnabledProperties = ImmutableList.copyOf(nonExistentProperties);
            this.functionRestrictions = ImmutableList.copyOf(functionRestrictions);
            this.notNullCheckEnabledProperties = ImmutableList.copyOf(existentProperties);
            this.fulltextEnabled = aggregate.hasNodeAggregates() || hasAnyFullTextEnabledProperty();
            this.nodeFullTextIndexed = aggregate.hasNodeAggregates() || anyNodeScopeIndexedProperty();
            this.propertyIndexEnabled = hasAnyPropertyIndexConfigured();
            this.indexesAllNodesOfMatchingType = areAlMatchingNodeByTypeIndexed();
            this.nodeNameIndexed = evaluateNodeNameIndexed(config);
            this.syncProps = ImmutableList.copyOf(syncProps);
            validateRuleDefinition();
        }

        /**
         * Creates a new indexing rule base on an existing one, but for a
         * different node type name.
         *
         * @param original the existing rule.
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
        }

        /**
         * Returns <code>true</code> if the property with the given name is
         * indexed according to this rule.
         *
         * @param propertyName the name of a property.
         * @return <code>true</code> if the property is indexed;
         *         <code>false</code> otherwise.
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

        @Override
        public String toString() {
            String str = "IndexRule: "+ nodeTypeName;
            if (!baseNodeType.equals(nodeTypeName)){
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
         *         <code>false</code> otherwise.
         */
        public boolean appliesTo(NodeState state) {
            for (String mixinName : getMixinTypeNames(state)){
                if (nodeTypeName.equals(mixinName)){
                    return true;
                }
            }

            if (!nodeTypeName.equals(getPrimaryTypeName(state))) {
                return false;
            }
            //TODO Add support for condition
            //return condition == null || condition.evaluate(state);
            return true;
        }

        public boolean appliesTo(String nodeTypeName) {
            if (!this.nodeTypeName.equals(nodeTypeName)) {
                return false;
            }

            //TODO Once condition support is done return false
            //return condition == null || condition.evaluate(state);
            return true;

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
         *         indexing rule does not contain a configuration for the given
         *         property.
         */
        @CheckForNull
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

        public boolean includePropertyType(int type){
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

         * @return true in case all matching node types are covered by this rule
         */
        public boolean indexesAllNodesOfMatchingType() {
            return indexesAllNodesOfMatchingType;
        }

        public boolean isBasedOnNtBase(){
            return JcrConstants.NT_BASE.equals(baseNodeType);
        }

        private Map<String, PropertyDefinition> collectPropConfigs(NodeState config,
                                                                   List<NamePattern> patterns,
                                                                   List<Aggregate.Include> propAggregate,
                                                                   List<PropertyDefinition> nonExistentProperties,
                                                                   List<PropertyDefinition> existentProperties,
                                                                   List<PropertyDefinition> nodeScopeAnalyzedProps,
                                                                   List<PropertyDefinition> functionRestrictions,
                                                                   List<PropertyDefinition> syncProps) {
            Map<String, PropertyDefinition> propDefns = newHashMap();
            NodeState propNode = config.getChildNode(LuceneIndexConstants.PROP_NODE);

            if (propNode.exists() && !hasOrderableChildren(propNode)){
                log.warn("Properties node for [{}] does not have orderable " +
                "children in [{}]", this, IndexDefinition.this);
            }

            //In case of a pure nodetype index we just index primaryType and mixins
            //and ignore any other property definition
            if (nodeTypeIndex) {
                boolean sync = getOptionalValue(config, LuceneIndexConstants.PROP_SYNC, false);
                PropertyDefinition pdpt =  createNodeTypeDefinition(this, JcrConstants.JCR_PRIMARYTYPE, sync);
                PropertyDefinition pdmixin =  createNodeTypeDefinition(this, JcrConstants.JCR_MIXINTYPES, sync);

                propDefns.put(pdpt.name.toLowerCase(Locale.ENGLISH), pdpt);
                propDefns.put(pdmixin.name.toLowerCase(Locale.ENGLISH), pdmixin);

                if (sync) {
                    syncProps.add(pdpt);
                    syncProps.add(pdmixin);
                }

                if (propNode.getChildNodeCount(1) > 0) {
                    log.warn("Index at [{}] has {} enabled and cannot support other property " +
                            "definitions", indexPath, LuceneIndexConstants.PROP_INDEX_NODE_TYPE);
                }

                return ImmutableMap.copyOf(propDefns);
            }

            //Include all immediate child nodes to 'properties' node by default
            Tree propTree = TreeFactory.createReadOnlyTree(propNode);
            for (Tree prop : propTree.getChildren()) {
                String propName = prop.getName();
                NodeState propDefnNode = propNode.getChildNode(propName);
                if (propDefnNode.exists() && !propDefns.containsKey(propName)) {
                    PropertyDefinition pd = new PropertyDefinition(this, propName, propDefnNode);
                    if (pd.function != null) {
                        functionRestrictions.add(pd);
                        String[] properties = FunctionIndexProcessor.getProperties(pd.functionCode);
                        for (String p : properties) {
                            if (PathUtils.getDepth(p) > 1) {
                                PropertyDefinition pd2 = new PropertyDefinition(this, p, propDefnNode);
                                propAggregate.add(new Aggregate.PropertyInclude(pd2));
                            }
                        }
                        // a function index has no other options
                        continue;
                    }
                    if(pd.isRegexp){
                        patterns.add(new NamePattern(pd.name, pd));
                    } else {
                        propDefns.put(pd.name.toLowerCase(Locale.ENGLISH), pd);
                    }

                    if (pd.relative){
                        propAggregate.add(new Aggregate.PropertyInclude(pd));
                    }

                    if (pd.nullCheckEnabled){
                        nonExistentProperties.add(pd);
                    }

                    if (pd.notNullCheckEnabled){
                        existentProperties.add(pd);
                    }

                    //Include props with name, boosted and nodeScopeIndex
                    if (pd.nodeScopeIndex
                            && pd.analyzed
                            && !pd.isRegexp){
                        nodeScopeAnalyzedProps.add(pd);
                    }

                    if (pd.sync) {
                        syncProps.add(pd);
                    }
                }
            }
            ensureNodeTypeIndexingIsConsistent(propDefns, syncProps);
            return ImmutableMap.copyOf(propDefns);
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
                pd_mixin =  createNodeTypeDefinition(this, JcrConstants.JCR_MIXINTYPES, pd_pr.sync);
                syncProps.add(pd_mixin);
                propDefns.put(JcrConstants.JCR_MIXINTYPES.toLowerCase(Locale.ENGLISH), pd_mixin);
            }
        }

        private boolean hasAnyFullTextEnabledProperty() {
            for (PropertyDefinition pd : propConfigs.values()){
                if (pd.fulltextEnabled()){
                    return true;
                }
            }

            for (NamePattern np : namePatterns){
                if (np.getConfig().fulltextEnabled()){
                    return true;
                }
            }
            return false;
        }

        private boolean hasAnyPropertyIndexConfigured() {
            for (PropertyDefinition pd : propConfigs.values()){
                if (pd.propertyIndex){
                    return true;
                }
            }

            for (NamePattern np : namePatterns){
                if (np.getConfig().propertyIndex){
                    return true;
                }
            }
            return false;
        }

        private boolean anyNodeScopeIndexedProperty(){
            //Check if there is any nodeScope indexed property as
            //for such case a node would always be indexed
            for (PropertyDefinition pd : propConfigs.values()){
                if (pd.nodeScopeIndex){
                    return true;
                }
            }

            for (NamePattern np : namePatterns){
                if (np.getConfig().nodeScopeIndex){
                    return true;
                }
            }

            return false;
        }

        private boolean areAlMatchingNodeByTypeIndexed(){
            if (nodeTypeIndex) {
                return true;
            }

            if (nodeFullTextIndexed){
                return true;
            }

            //If there is nullCheckEnabled property which is not relative then
            //all nodes would be indexed. relativeProperty with nullCheckEnabled might
            //not ensure that (OAK-1085)
            for (PropertyDefinition pd : nullCheckEnabledProperties){
                if (!pd.relative) {
                    return true;
                }
            }

            //jcr:primaryType is present on all node. So if such a property
            //is indexed then it would mean all nodes covered by this index rule
            //are indexed
            if (getConfig(JcrConstants.JCR_PRIMARYTYPE) != null){
                return true;
            }

            return false;
        }

        private boolean evaluateNodeNameIndexed(NodeState config) {
            //check global config first
            if (getOptionalValue(config, LuceneIndexConstants.INDEX_NODE_NAME, false)) {
                return true;
            }

            //iterate over property definitions
            for (PropertyDefinition pd : propConfigs.values()){
                if (LuceneIndexConstants.PROPDEF_PROP_NODE_NAME.equals(pd.name)){
                    return true;
                }
            }
            return false;
        }

        private Aggregate combine(Aggregate propAggregate, String nodeTypeName){
            Aggregate nodeTypeAgg = IndexDefinition.this.getAggregate(nodeTypeName);
            List<Aggregate.Include> includes = newArrayList();
            includes.addAll(propAggregate.getIncludes());
            if (nodeTypeAgg != null){
                includes.addAll(nodeTypeAgg.getIncludes());
            }
            return new Aggregate(nodeTypeName, includes);
        }

        private void validateRuleDefinition() {
            if (!nullCheckEnabledProperties.isEmpty() && isBasedOnNtBase()){
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
         * @param config the associated configuration.
         */
        private NamePattern(String pattern,
                            PropertyDefinition config){

            //Special handling for all props regex as its already being used
            //and use of '/' in regex would confuse the parent path calculation
            //logic
            if (LuceneIndexConstants.REGEX_ALL_PROPS.equals(pattern)){
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
         *         pattern; <code>false</code> otherwise.
         */
        boolean matches(String propertyPath) {
            String parentPath = getParentPath(propertyPath);
            String propertyName = PathUtils.getName(propertyPath);
            if (!this.parentPath.equals(parentPath)) {
                return false;
            }
            return pattern.matcher(propertyName).matches();
        }

        PropertyDefinition getConfig() {
            return config;
        }
    }

    //~---------------------------------------------< compatibility >

    public static NodeBuilder updateDefinition(NodeBuilder indexDefn){
        return updateDefinition(indexDefn, "unknown");
    }

    public static NodeBuilder updateDefinition(NodeBuilder indexDefn, String indexPath){
        NodeState defn = indexDefn.getBaseState();
        if (!hasIndexingRules(defn)){
            NodeState rulesState = createIndexRules(defn).getNodeState();
            indexDefn.setChildNode(LuceneIndexConstants.INDEX_RULES, rulesState);
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
    private static NodeBuilder createIndexRules(NodeState defn){
        NodeBuilder builder = EMPTY_NODE.builder();
        Set<String> declaringNodeTypes = getMultiProperty(defn, DECLARING_NODE_TYPES);
        Set<String> includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        Set<String> excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        Set<String> orderedProps = getMultiProperty(defn, ORDERED_PROP_NAMES);
        boolean fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        boolean storageEnabled = getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);
        NodeState propNodeState = defn.getChildNode(LuceneIndexConstants.PROP_NODE);

        //If no explicit nodeType defined then all config applies for nt:base
        if (declaringNodeTypes.isEmpty()){
            declaringNodeTypes = Collections.singleton(NT_BASE);
        }

        Set<String> propNamesSet = Sets.newHashSet();
        propNamesSet.addAll(includes);
        propNamesSet.addAll(excludes);
        propNamesSet.addAll(orderedProps);

        //Also include all immediate leaf propNode names
        for (ChildNodeEntry cne : propNodeState.getChildNodeEntries()){
            if (!propNamesSet.contains(cne.getName())
                    && Iterables.isEmpty(cne.getNodeState().getChildNodeNames())){
                propNamesSet.add(cne.getName());
            }
        }

        List<String> propNames = new ArrayList<String>(propNamesSet);

        final String includeAllProp = LuceneIndexConstants.REGEX_ALL_PROPS;
        if (fullTextEnabled
                && includes.isEmpty()){
            //Add the regEx for including all properties at the end
            //for fulltext index and when no explicit includes are defined
            propNames.add(includeAllProp);
        }

        for (String typeName : declaringNodeTypes){
            NodeBuilder rule = builder.child(typeName);
            markAsNtUnstructured(rule);
            List<String> propNodeNames = newArrayListWithCapacity(propNamesSet.size());
            NodeBuilder propNodes = rule.child(PROP_NODE);
            int i = 0;
            for (String propName : propNames){
                String propNodeName = propName;

                //For proper propName use the propName as childNode name
                if(PropertyDefinition.isRelativeProperty(propName)
                        || propName.equals(includeAllProp)){
                    propNodeName = "prop" + i++;
                }
                propNodeNames.add(propNodeName);

                NodeBuilder prop = propNodes.child(propNodeName);
                markAsNtUnstructured(prop);
                prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);

                if (excludes.contains(propName)){
                    prop.setProperty(LuceneIndexConstants.PROP_INDEX, false);
                } else if (fullTextEnabled){
                    prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
                    prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
                    prop.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, storageEnabled);
                    prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, false);
                } else {
                    prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

                    if (orderedProps.contains(propName)){
                        prop.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
                    }
                }

                if (propName.equals(includeAllProp)){
                    prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
                }

                //Copy over the property configuration
                NodeState propDefNode  = getPropDefnNode(defn, propName);
                if (propDefNode != null){
                    for (PropertyState ps : propDefNode.getProperties()){
                        prop.setProperty(ps);
                    }
                }
            }

            //If no propertyType defined then default to UNKNOWN such that none
            //of the properties get indexed
            PropertyState supportedTypes = defn.getProperty(INCLUDE_PROPERTY_TYPES);
            if (supportedTypes == null){
                supportedTypes = PropertyStates.createProperty(INCLUDE_PROPERTY_TYPES, TYPES_ALLOW_ALL_NAME);
            }
            rule.setProperty(supportedTypes);

            if (!NT_BASE.equals(typeName)) {
                rule.setProperty(LuceneIndexConstants.RULE_INHERITED, false);
            }

            propNodes.setProperty(OAK_CHILD_ORDER, propNodeNames ,NAMES);
            markAsNtUnstructured(propNodes);
        }

        markAsNtUnstructured(builder);
        builder.setProperty(OAK_CHILD_ORDER, declaringNodeTypes ,NAMES);
        return builder;
    }

    private static NodeState getPropDefnNode(NodeState defn, String propName){
        NodeState propNode = defn.getChildNode(LuceneIndexConstants.PROP_NODE);
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
        int length = getOptionalValue(definition.getChildNode(TIKA), LuceneIndexConstants.TIKA_MAX_EXTRACT_LENGTH,
                DEFAULT_MAX_EXTRACT_LENGTH);
        if (length < 0){
            return - length * maxFieldLength;
        }
        return length;
    }

    private NodeState getTikaConfigNode() {
        return definition.getChildNode(TIKA).getChildNode(TIKA_CONFIG);
    }

    private Codec createCodec() {
        String codecName = getOptionalValue(definition, LuceneIndexConstants.CODEC_NAME, null);
        Codec codec = null;
        if (codecName != null) {
            // prevent LUCENE-6482
            // (also done in LuceneIndexProviderService, just to be save)
            OakCodec ensureLucene46CodecLoaded = new OakCodec();
            // to ensure the JVM doesn't optimize away object creation
            // (probably not really needed; just to be save)
            log.debug("Lucene46Codec is loaded: {}", ensureLucene46CodecLoaded);
            codec = Codec.forName(codecName);
            log.debug("Codec is loaded: {}", codecName);
        } else if (fullTextEnabled) {
            codec = new OakCodec();
        }
        return codec;
    }

    private MergePolicy createMergePolicy() {
        String mmp = System.getProperty("oak.lucene.cmmp");
        if (mmp != null) {
            return new CommitMitigatingTieredMergePolicy();
        }

        String mergePolicyName = getOptionalValue(definition, LuceneIndexConstants.MERGE_POLICY_NAME, null);
        MergePolicy mergePolicy = null;
        if (mergePolicyName != null) {
            if (mergePolicyName.equalsIgnoreCase("no")) {
                mergePolicy = NoMergePolicy.COMPOUND_FILES;
            } else if (mergePolicyName.equalsIgnoreCase("mitigated")) {
                mergePolicy = new CommitMitigatingTieredMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("tiered") || mergePolicyName.equalsIgnoreCase("default")) {
                mergePolicy = new TieredMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("logbyte")) {
                mergePolicy = new LogByteSizeMergePolicy();
            } else if (mergePolicyName.equalsIgnoreCase("logdoc")) {
                mergePolicy = new LogDocMergePolicy();
            }
        }
        if (mergePolicy == null) {
            mergePolicy = new TieredMergePolicy();
        }
        return mergePolicy;
    }

    private static Set<String> getMultiProperty(NodeState definition, String propName){
        PropertyState pse = definition.getProperty(propName);
        return pse != null ? ImmutableSet.copyOf(pse.getValue(Type.STRINGS)) : Collections.<String>emptySet();
    }

    private static Set<String> toLowerCase(Set<String> values){
        Set<String> result = newHashSet();
        for(String val : values){
            result.add(val.toLowerCase());
        }
        return ImmutableSet.copyOf(result);
    }

    private static List<String> getAllNodeTypes(ReadOnlyNodeTypeManager ntReg) {
        try {
            List<String> typeNames = newArrayList();
            NodeTypeIterator ntItr = ntReg.getAllNodeTypes();
            while (ntItr.hasNext()){
                typeNames.add(ntItr.nextNodeType().getName());
            }
            return typeNames;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    private static ReadOnlyNodeTypeManager createNodeTypeManager(final Tree root) {
        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return TreeUtil.getTree(root,NODE_TYPES_PATH);
            }

            @Nonnull
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
        return property != null ? property.getValue(Type.NAMES) : Collections.<String>emptyList();
    }

    private static boolean hasOrderableChildren(NodeState state){
        return state.hasProperty(OAK_CHILD_ORDER);
    }

    static int getSupportedTypes(NodeState defn, String typePropertyName, int defaultVal) {
        PropertyState pst = defn.getProperty(typePropertyName);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                if (TYPES_ALLOW_ALL_NAME.equals(inc)){
                    return TYPES_ALLOW_ALL;
                }

                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            return types;
        }
        return defaultVal;
    }

    static boolean includePropertyType(int includedPropertyTypes, int type){
        if(includedPropertyTypes == TYPES_ALLOW_ALL){
            return true;
        }

        if (includedPropertyTypes == TYPES_ALLOW_NONE){
            return false;
        }

        return (includedPropertyTypes & (1 << type)) != 0;
    }

    private static boolean hasFulltextEnabledIndexRule(List<IndexingRule> rules) {
        for (IndexingRule rule : rules){
            if (rule.isFulltextEnabled()){
                return true;
            }
        }
        return false;
    }

    private static void markAsNtUnstructured(NodeBuilder nb){
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
    }

    private static IndexFormatVersion determineIndexFormatVersion(NodeState defn) {
        //Compat mode version if specified has highest priority
        if (defn.hasProperty(COMPAT_MODE)){
            return versionFrom(defn.getProperty(COMPAT_MODE));
        }

        if (defn.hasProperty(INDEX_VERSION)){
            return versionFrom(defn.getProperty(INDEX_VERSION));
        }

        //No existing index data i.e. reindex or fresh index
        if (!defn.getChildNode(INDEX_DATA_CHILD_NAME).exists()){
            return determineVersionForFreshIndex(defn);
        }

        boolean fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);

        //A fulltext index with old indexing format confirms to V1. However
        //a propertyIndex with old indexing format confirms to V2
        return fullTextEnabled ? IndexFormatVersion.V1 : IndexFormatVersion.V2;
    }

    static IndexFormatVersion determineVersionForFreshIndex(NodeState defn){
        return determineVersionForFreshIndex(defn.getProperty(FULL_TEXT_ENABLED),
                defn.getProperty(COMPAT_MODE), defn.getProperty(INDEX_VERSION));
    }

    static IndexFormatVersion determineVersionForFreshIndex(NodeBuilder defnb){
        return determineVersionForFreshIndex(defnb.getProperty(FULL_TEXT_ENABLED),
                defnb.getProperty(COMPAT_MODE), defnb.getProperty(INDEX_VERSION));
    }

    private static IndexFormatVersion determineVersionForFreshIndex(PropertyState fulltext,
                                                                    PropertyState compat,
                                                                    PropertyState version){
        if (compat != null){
            return versionFrom(compat);
        }

        IndexFormatVersion defaultToUse = IndexFormatVersion.getDefault();
        IndexFormatVersion existing = version != null ? versionFrom(version) : null;

        //As per OAK-2290 current might be less than current used version. So
        //set to current only if it is greater than existing

        //Per setting use default configured
        IndexFormatVersion result = defaultToUse;

        //If default configured is lesser than existing then prefer existing
        if (existing != null){
            result = IndexFormatVersion.max(result,existing);
        }

        //Check if fulltext is false which indicates its a property index and
        //hence confirm to V2 or above
        if (fulltext != null && !fulltext.getValue(Type.BOOLEAN)){
            return IndexFormatVersion.max(result,IndexFormatVersion.V2);
        }

        return result;
    }

    private static String[] getOptionalStrings(NodeState defn, String propertyName) {
        PropertyState ps = defn.getProperty(propertyName);
        if (ps != null) {
            return Iterables.toArray(ps.getValue(Type.STRINGS), String.class);
        }
        return null;
    }

    private static IndexFormatVersion versionFrom(PropertyState ps){
        return IndexFormatVersion.getVersion(Ints.checkedCast(ps.getValue(Type.LONG)));
    }

    private static boolean hasIndexingRules(NodeState defn) {
        return defn.getChildNode(LuceneIndexConstants.INDEX_RULES).exists();
    }

    @CheckForNull
    private static String determineUniqueId(NodeState defn) {
        return defn.getChildNode(STATUS_NODE).getString(PROP_UID);
    }

    private static double getDefaultCostPerEntry(IndexFormatVersion version) {
        //For older format cost per entry would be higher as it does a runtime
        //aggregation
        return version == IndexFormatVersion.V1 ?  1.5 : 1.0;
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
        if (async == null){
            return false;
        }
        return Iterables.contains(async.getValue(Type.STRINGS), mode);
    }

    private static NodeState getIndexDefinitionState(NodeState defn) {
        if (isDisableStoredIndexDefinition()){
            return defn;
        }
        NodeState storedState = defn.getChildNode(INDEX_DEFINITION_NODE);
        return storedState.exists() ? storedState : defn;
    }

    private static Map<String, String> buildMimeTypeMap(NodeState node) {
        ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
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
        return map.build();
    }

    private static PropertyDefinition createNodeTypeDefinition(IndexingRule rule, String name, boolean sync) {
        NodeBuilder builder = EMPTY_NODE.builder();
        //A nodetype index just required propertyIndex and sync flags to be set
        builder.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        if (sync) {
            builder.setProperty(LuceneIndexConstants.PROP_SYNC, sync);
        }
        builder.setProperty(LuceneIndexConstants.PROP_NAME, name);
        return new PropertyDefinition(rule, name, builder.getNodeState());
    }

}
