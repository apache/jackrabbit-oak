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

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getOptionalValue;

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

    /**
     * Hidden property for storing a seed value to be used as suffix in remote index name.
     */
    public static final String PROP_INDEX_NAME_SEED = ":nameSeed";

    /**
     * Node name under which various analyzers are configured
     */
    private static final String ANALYZERS = "analyzers";

    /**
     * Boolean property indicating if in-built analyzer should preserve original term
     */
    private static final String INDEX_ORIGINAL_TERM = "indexOriginalTerm";

    private static final Function<Integer, Boolean> isAnalyzable;

    static {
        int[] NOT_ANALYZED_TYPES = new int[] {
                Type.BINARY.tag(), Type.LONG.tag(), Type.DOUBLE.tag(), Type.DECIMAL.tag(), Type.DATE.tag(), Type.BOOLEAN.tag()
        };
        Arrays.sort(NOT_ANALYZED_TYPES); // need for binary search
        isAnalyzable = type -> Arrays.binarySearch(NOT_ANALYZED_TYPES, type) < 0;
    }

    public final int bulkActions;
    public final long bulkSizeBytes;
    public final long bulkFlushIntervalMs;
    public final int bulkRetries;
    public final long bulkRetriesBackoff;
    private final String remoteAlias;

    private final Map<String, List<PropertyDefinition>> propertiesByName;

    public ElasticIndexDefinition(NodeState root, NodeState defn, String indexPath, String indexPrefix) {
        super(root, defn, determineIndexFormatVersion(defn), determineUniqueId(defn), indexPath);
        this.remoteAlias = ElasticIndexNameHelper.getIndexAlias(indexPrefix != null ? indexPrefix : "", getIndexPath());
        this.bulkActions = getOptionalValue(defn, BULK_ACTIONS, BULK_ACTIONS_DEFAULT);
        this.bulkSizeBytes = getOptionalValue(defn, BULK_SIZE_BYTES, BULK_SIZE_BYTES_DEFAULT);
        this.bulkFlushIntervalMs = getOptionalValue(defn, BULK_FLUSH_INTERVAL_MS, BULK_FLUSH_INTERVAL_MS_DEFAULT);
        this.bulkRetries = getOptionalValue(defn, BULK_RETRIES, BULK_RETRIES_DEFAULT);
        this.bulkRetriesBackoff = getOptionalValue(defn, BULK_RETRIES_BACKOFF, BULK_RETRIES_BACKOFF_DEFAULT);

        this.propertiesByName = getDefinedRules()
                .stream()
                .flatMap(rule -> StreamSupport.stream(rule.getProperties().spliterator(), false))
                .filter(pd -> pd.index) // keep only properties that can be indexed
                .collect(Collectors.groupingBy(pd -> pd.name));
    }

    /**
     * Returns the index alias on the Elasticsearch cluster. This alias should be used for any index related operations
     * instead of accessing the index directly.
     * @return the Elasticsearch index alias
     */
    public String getRemoteIndexAlias() {
        return remoteAlias;
    }

    public Map<String, List<PropertyDefinition>> getPropertiesByName() {
        return propertiesByName;
    }

    /**
     * Returns the keyword field name mapped in Elasticsearch for the specified property name.
     * @param propertyName the property name in the index rules
     * @return the field name identifier in Elasticsearch
     * @throws IllegalArgumentException if the specified name is not part of this {@code ElasticIndexDefinition}
     */
    public String getElasticKeyword(String propertyName) {
        List<PropertyDefinition> propertyDefinitions = propertiesByName.get(propertyName);
        if (propertyDefinitions == null) {
            throw new IllegalArgumentException(propertyName + " is not part of this ElasticIndexDefinition");
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
        return propertyDefinitions.stream().anyMatch(pd -> pd.analyzed || pd.fulltextEnabled());
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
    public boolean indexOriginalTerms() {
        NodeState analyzersTree = definition.getChildNode(ANALYZERS);
        return getOptionalValue(analyzersTree, INDEX_ORIGINAL_TERM, false);
    }

    /**
     * Class to help with {@link ElasticIndexDefinition} creation.
     * The built object represents the index definition only without the node structure.
     */
    public static class Builder extends IndexDefinition.Builder {

        private final String indexPrefix;

        public Builder(String indexPrefix) {
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
