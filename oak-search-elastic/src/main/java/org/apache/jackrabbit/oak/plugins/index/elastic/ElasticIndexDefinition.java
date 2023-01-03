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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValues;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

public class ElasticIndexDefinition extends IndexDefinition {

    public static final String TYPE_ELASTICSEARCH = "elasticsearch";

    public static final String BULK_ACTIONS = "bulkActions";
    public static final int BULK_ACTIONS_DEFAULT = 250;

    public static final String BULK_SIZE_BYTES = "bulkSizeBytes";
    public static final long BULK_SIZE_BYTES_DEFAULT = 2 * 1024 * 1024; // 2MB

    public static final String BULK_FLUSH_INTERVAL_MS = "bulkFlushIntervalMs";
    public static final long BULK_FLUSH_INTERVAL_MS_DEFAULT = 3000;

    public static final String BULK_RETRIES = "bulkRetries";
    public static final int BULK_RETRIES_DEFAULT = 3;

    public static final String BULK_RETRIES_BACKOFF = "bulkRetriesBackoff";
    public static final long BULK_RETRIES_BACKOFF_DEFAULT = 200;

    public static final String NUMBER_OF_SHARDS = "numberOfShards";
    public static final int NUMBER_OF_SHARDS_DEFAULT = 1;

    public static final String NUMBER_OF_REPLICAS = "numberOfReplicas";
    public static final int NUMBER_OF_REPLICAS_DEFAULT = 1;

    public static final String QUERY_FETCH_SIZES = "queryFetchSizes";
    public static final Long[] QUERY_FETCH_SIZES_DEFAULT = new Long[]{100L, 1000L};

    public static final String TRACK_TOTAL_HITS = "trackTotalHits";
    public static final Integer TRACK_TOTAL_HITS_DEFAULT = 10000;

    /**
     * Hidden property for storing a seed value to be used as suffix in remote index name.
     */
    public static final String PROP_INDEX_NAME_SEED = ":nameSeed";

    /**
     * Hidden property to store similarity tags
     */
    public static final String SIMILARITY_TAGS = ":simTags";

    /**
     * Node name under which various analyzers are configured
     */
    private static final String ANALYZERS = "analyzers";

    /**
     * Boolean property indicating if in-built analyzer should preserve original term
     */
    public static final String INDEX_ORIGINAL_TERM = "indexOriginalTerm";

    public static final String SPLIT_ON_CASE_CHANGE = "splitOnCaseChange";
    public static final String SPLIT_ON_NUMERICS = "splitOnNumerics";

    public static final String ELASTIKNN = "elastiknn";

    private static final String SIMILARITY_TAGS_ENABLED = "similarityTagsEnabled";
    private static final boolean SIMILARITY_TAGS_ENABLED_DEFAULT = true;

    private static final String SIMILARITY_TAGS_BOOST = "similarityTagsBoost";
    private static final float SIMILARITY_TAGS_BOOST_DEFAULT = 0.5f;

    private static final Function<Integer, Boolean> isAnalyzable;

    static {
        int[] NOT_ANALYZED_TYPES = new int[] {
                Type.BINARY.tag(), Type.LONG.tag(), Type.DOUBLE.tag(), Type.DECIMAL.tag(), Type.DATE.tag(), Type.BOOLEAN.tag()
        };
        Arrays.sort(NOT_ANALYZED_TYPES); // need for binary search
        isAnalyzable = type -> Arrays.binarySearch(NOT_ANALYZED_TYPES, type) < 0;
    }

    private final String indexPrefix;
    private final String indexAlias;
    public final int bulkActions;
    public final long bulkSizeBytes;
    public final long bulkFlushIntervalMs;
    public final int bulkRetries;
    public final long bulkRetriesBackoff;
    private final boolean similarityTagsEnabled;
    private final float similarityTagsBoost;
    public final int numberOfShards;
    public final int numberOfReplicas;
    public final int[] queryFetchSizes;
    public final Integer trackTotalHits;

    private final Map<String, List<PropertyDefinition>> propertiesByName;
    private final List<PropertyDefinition> dynamicBoostProperties;
    private final List<PropertyDefinition> similarityProperties;

    public ElasticIndexDefinition(NodeState root, NodeState defn, String indexPath, String indexPrefix) {
        super(root, defn, determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
        this.indexPrefix = indexPrefix;
        this.indexAlias = ElasticIndexNameHelper.getElasticSafeIndexName(indexPrefix, getIndexPath());
        this.bulkActions = getOptionalValue(defn, BULK_ACTIONS, BULK_ACTIONS_DEFAULT);
        this.bulkSizeBytes = getOptionalValue(defn, BULK_SIZE_BYTES, BULK_SIZE_BYTES_DEFAULT);
        this.bulkFlushIntervalMs = getOptionalValue(defn, BULK_FLUSH_INTERVAL_MS, BULK_FLUSH_INTERVAL_MS_DEFAULT);
        this.bulkRetries = getOptionalValue(defn, BULK_RETRIES, BULK_RETRIES_DEFAULT);
        this.bulkRetriesBackoff = getOptionalValue(defn, BULK_RETRIES_BACKOFF, BULK_RETRIES_BACKOFF_DEFAULT);
        this.numberOfShards = getOptionalValue(defn, NUMBER_OF_SHARDS, NUMBER_OF_SHARDS_DEFAULT);
        this.numberOfReplicas = getOptionalValue(defn, NUMBER_OF_REPLICAS, NUMBER_OF_REPLICAS_DEFAULT);
        this.similarityTagsEnabled = getOptionalValue(defn, SIMILARITY_TAGS_ENABLED, SIMILARITY_TAGS_ENABLED_DEFAULT);
        this.similarityTagsBoost = getOptionalValue(defn, SIMILARITY_TAGS_BOOST, SIMILARITY_TAGS_BOOST_DEFAULT);
        this.queryFetchSizes = Arrays.stream(getOptionalValues(defn, QUERY_FETCH_SIZES, Type.LONGS, Long.class, QUERY_FETCH_SIZES_DEFAULT))
                .mapToInt(Long::intValue).toArray();
        this.trackTotalHits = getOptionalValue(defn, TRACK_TOTAL_HITS, TRACK_TOTAL_HITS_DEFAULT);

        this.propertiesByName = getDefinedRules()
                .stream()
                .flatMap(rule -> Stream.concat(StreamSupport.stream(rule.getProperties().spliterator(), false),
                        rule.getFunctionRestrictions().stream()))
                .filter(pd -> pd.index) // keep only properties that can be indexed
                .collect(Collectors.groupingBy(pd -> {
                    if (pd.function != null) {
                        return pd.function;
                    } else {
                        return pd.name;
                    }
                }));

        this.dynamicBoostProperties = getDefinedRules()
                .stream()
                .flatMap(IndexingRule::getNamePatternsProperties)
                .filter(pd -> pd.dynamicBoost)
                .collect(Collectors.toList());

        this.similarityProperties = getDefinedRules()
                .stream()
                .flatMap(rule -> rule.getSimilarityProperties().stream())
                .collect(Collectors.toList());
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    /**
     * Returns the index alias on the Elasticsearch cluster. This alias should be used for any query related operations.
     * The actual index name is used only when a reindex is in progress.
     * @return the Elasticsearch index alias
     */
    public String getIndexAlias() {
        return indexAlias;
    }

    public Map<String, List<PropertyDefinition>> getPropertiesByName() {
        return propertiesByName;
    }

    public List<PropertyDefinition> getDynamicBoostProperties() {
        return dynamicBoostProperties;
    }

    public List<PropertyDefinition> getSimilarityProperties() {
        return similarityProperties;
    }

    public boolean areSimilarityTagsEnabled() {
        return similarityTagsEnabled;
    }

    public float getSimilarityTagsBoost() {
        return similarityTagsBoost;
    }

    /**
     * Returns the keyword field name mapped in Elasticsearch for the specified property name.
     * @param propertyName the property name in the index rules
     * @return the field name identifier in Elasticsearch
     */
    public String getElasticKeyword(String propertyName) {
        List<PropertyDefinition> propertyDefinitions = propertiesByName.get(propertyName);
        if (propertyDefinitions == null) {
            // if there are no property definitions we return the default keyword name
            // this can happen for properties that were not explicitly defined (eg: created with a regex)
            return propertyName + ".keyword";
        }

        String field = propertyName;
        // it's ok to look at the first property since we are sure they all have the same type
        int type = propertyDefinitions.get(0).getType();
        if (isAnalyzable.apply(type) && isAnalyzed(propertyDefinitions)) {
            field += ".keyword";
        }
        return field;
    }

    public boolean isAnalyzed(List<PropertyDefinition> propertyDefinitions) {
        return propertyDefinitions.stream().anyMatch(pd -> pd.analyzed);
    }

    @Override
    protected String getDefaultFunctionName() {
        /*
        This has nothing to do with lucene index. While parsing queries, spellCheck queries are handled
        via PropertyRestriction having native*lucene as key.
         */
        return "lucene";
    }

    /**
     * Returns {@code true} if original terms need to be preserved at indexing analysis phase
     */
    public boolean analyzerConfigIndexOriginalTerms() {
        NodeState analyzersTree = definition.getChildNode(ANALYZERS);
        return getOptionalValue(analyzersTree, INDEX_ORIGINAL_TERM, false);
    }

    public boolean analyzerConfigSplitOnCaseChange() {
        NodeState analyzersTree = definition.getChildNode(ANALYZERS);
        return getOptionalValue(analyzersTree, SPLIT_ON_CASE_CHANGE, false);
    }

    public boolean analyzerConfigSplitOnNumerics() {
        NodeState analyzersTree = definition.getChildNode(ANALYZERS);
        return getOptionalValue(analyzersTree, SPLIT_ON_NUMERICS, false);
    }

    @Override
    protected PropertyDefinition createPropertyDefinition(IndexDefinition.IndexingRule rule, String name, NodeState nodeState) {
        return new ElasticPropertyDefinition(rule, name, nodeState);
    }

    /**
     * Class to help with {@link ElasticIndexDefinition} creation.
     * The built object represents the index definition only without the node structure.
     */
    public static class Builder extends IndexDefinition.Builder {

        private final String indexPrefix;

        public Builder(@NotNull String indexPrefix) {
            this.indexPrefix = indexPrefix;
        }

        @Override
        public ElasticIndexDefinition build() {
            return (ElasticIndexDefinition) super.build();
        }

        @Override
        public Builder reindex() {
            super.reindex();
            return this;
        }

        @Override
        protected IndexDefinition createInstance(NodeState indexDefnStateToUse) {
            return new ElasticIndexDefinition(root, indexDefnStateToUse, indexPath, indexPrefix);
        }
    }
}
