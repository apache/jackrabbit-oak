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
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Locale;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public final class MongotIndexDefinition extends IndexDefinition {

    public static final String TYPE_MONGOT = "mongot";
    public static final String SYNONYM_MAPPING_NAME = "oak_synonyms";
    public static final String PROP_COLLECTION_SEED = ":collectionSeed";
    public static final String QUERY_FETCH_SIZES = "queryFetchSizes";
    public static final String QUERY_TIMEOUT_MS = "queryTimeoutMs";
    public static final String STORED_SOURCE = "storedSource";

    private static final int[] DEFAULT_QUERY_FETCH_SIZES = {10, 100, 1000};
    private static final long DEFAULT_QUERY_TIMEOUT_MILLIS = 60_000L;

    private final long collectionSeed;
    private final String collectionName;
    private final String searchIndexName;
    private final int[] queryFetchSizes;
    private final long queryTimeoutMillis;
    private final boolean storedSource;
    private final boolean fullTextStored;

    public MongotIndexDefinition(NodeState root, NodeState definition, String indexPath) {
        super(root, getIndexDefinitionState(definition), IndexFormatVersion.V2,
                determineUniqueId(definition), indexPath);
        this.collectionSeed = definition.getLong(PROP_COLLECTION_SEED);
        this.collectionName = MongotIndexNames.collectionName(indexPath, collectionSeed);
        this.searchIndexName = MongotIndexNames.searchIndexName(indexPath);
        this.queryFetchSizes = queryFetchSizes(definition);
        this.queryTimeoutMillis = definition.hasProperty(QUERY_TIMEOUT_MS)
                ? definition.getLong(QUERY_TIMEOUT_MS)
                : DEFAULT_QUERY_TIMEOUT_MILLIS;
        if (queryTimeoutMillis <= 0) {
            throw new IllegalArgumentException(QUERY_TIMEOUT_MS + " must be positive");
        }
        // Read from the stored definition, which only changes on reindex: Mongot rejects
        // returnStoredSource against a search index that was built without storedSource.
        this.storedSource = getDefinitionNodeState().getBoolean(STORED_SOURCE);
        // Mongot reads a hit's stored fields together, so a stored copy of the large full-text
        // field makes stored-source reads slower than document lookups. Keep it only when
        // excerpts need it for highlighting.
        this.fullTextStored = !storedSource || hasExcerptProperties();
    }

    public String getCollectionName() {
        return collectionName;
    }

    public long getCollectionSeed() {
        return collectionSeed;
    }

    public String getCollectionName(long seed) {
        return MongotIndexNames.collectionName(getIndexPath(), seed);
    }

    public String getSearchIndexName() {
        return searchIndexName;
    }

    public int[] getQueryFetchSizes() {
        return queryFetchSizes.clone();
    }

    public long getQueryTimeoutMillis() {
        return queryTimeoutMillis;
    }

    public boolean isStoredSource() {
        return storedSource;
    }

    public boolean isFullTextStored() {
        return fullTextStored;
    }

    private boolean hasExcerptProperties() {
        return getDefinedRules().stream().anyMatch(rule ->
                StreamSupport.stream(rule.getProperties().spliterator(), false)
                        .anyMatch(property -> property.stored)
                        || rule.getNamePatternsProperties().anyMatch(property -> property.stored));
    }

    public String getSynonymCollectionName() {
        return MongotIndexNames.collectionName(getIndexPath()) + "_synonyms";
    }

    public boolean hasSynonyms() {
        NodeState filters = getDefinitionNodeState()
                .getChildNode(FulltextIndexConstants.ANALYZERS)
                .getChildNode(FulltextIndexConstants.ANL_DEFAULT)
                .getChildNode(FulltextIndexConstants.ANL_FILTERS);
        for (var entry : filters.getChildNodeEntries()) {
            NodeState filter = entry.getNodeState();
            String name = filter.hasProperty(FulltextIndexConstants.ANL_NAME)
                    ? filter.getString(FulltextIndexConstants.ANL_NAME)
                    : entry.getName();
            name = name.replace("Factory", "").replace("-", "")
                    .replace("_", "").toLowerCase(Locale.ROOT);
            if ("synonym".equals(name) || "synonymgraph".equals(name)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasFullTextDynamicBoost() {
        return hasDynamicBoost(true);
    }

    public boolean hasDynamicBoost() {
        return hasDynamicBoost(false);
    }

    private boolean hasDynamicBoost(boolean requireFullTextQuery) {
        NodeState rules = getDefinitionNodeState()
                .getChildNode(FulltextIndexConstants.INDEX_RULES);
        for (NodeState rule : childStates(rules)) {
            NodeState properties = rule.getChildNode(FulltextIndexConstants.PROP_NODE);
            for (NodeState property : childStates(properties)) {
                if (property.getBoolean(FulltextIndexConstants.PROP_DYNAMIC_BOOST)
                        && (!requireFullTextQuery
                        || !property.hasProperty("useInFullTextQuery")
                        || property.getBoolean("useInFullTextQuery"))) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isFullTextStopword(String term) {
        NodeState analyzer = getDefinitionNodeState()
                .getChildNode(FulltextIndexConstants.ANALYZERS)
                .getChildNode(FulltextIndexConstants.ANL_DEFAULT);
        if (!analyzer.exists()) {
            return false;
        }
        NodeState configuredStopwords = analyzer.getChildNode("stopwords");
        if (configuredStopwords.exists()
                && containsLine(configuredStopwords, term, true)) {
            return true;
        }
        NodeState filters = analyzer.getChildNode(FulltextIndexConstants.ANL_FILTERS);
        for (var entry : filters.getChildNodeEntries()) {
            NodeState filter = entry.getNodeState();
            String name = filter.hasProperty(FulltextIndexConstants.ANL_NAME)
                    ? filter.getString(FulltextIndexConstants.ANL_NAME)
                    : entry.getName();
            name = name.replace("Factory", "").replace("-", "")
                    .replace("_", "").toLowerCase(Locale.ROOT);
            if (!("stop".equals(name) || "stopword".equals(name))) {
                continue;
            }
            PropertyState resources = filter.getProperty("words");
            if (resources == null) {
                continue;
            }
            boolean ignoreCase = !filter.hasProperty("ignoreCase")
                    || Boolean.parseBoolean(filter.getProperty("ignoreCase").getValue(Type.STRING));
            boolean snowball = filter.hasProperty("format")
                    && "snowball".equalsIgnoreCase(filter.getString("format"));
            for (String resource : resources.getValue(Type.STRING).split(",")) {
                if (containsLine(filter.getChildNode(resource.trim()), term, ignoreCase, snowball)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean containsLine(NodeState resource, String term, boolean ignoreCase) {
        return containsLine(resource, term, ignoreCase, false);
    }

    private static boolean containsLine(NodeState resource, String term, boolean ignoreCase,
                                        boolean snowball) {
        PropertyState data = resource.getChildNode("jcr:content").getProperty("jcr:data");
        if (data == null) {
            return false;
        }
        for (String line : read(data).split("\\R")) {
            String value = snowball ? line.split("\\|", 2)[0].trim() : line.trim();
            if (!value.isEmpty() && !value.startsWith("#")
                    && (ignoreCase ? value.equalsIgnoreCase(term) : value.equals(term))) {
                return true;
            }
        }
        return false;
    }

    private static String read(PropertyState data) {
        if (data.getType().tag() != Type.BINARY.tag()) {
            return data.getValue(Type.STRING);
        }
        Blob blob = data.getValue(Type.BINARY);
        try (InputStream stream = blob.getNewStream()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read analyzer stopwords", e);
        }
    }

    private static Iterable<NodeState> childStates(NodeState parent) {
        java.util.List<NodeState> children = new java.util.ArrayList<>();
        parent.getChildNodeEntries().forEach(entry -> children.add(entry.getNodeState()));
        return children;
    }

    private static int[] queryFetchSizes(NodeState definition) {
        if (!definition.hasProperty(QUERY_FETCH_SIZES)) {
            return DEFAULT_QUERY_FETCH_SIZES.clone();
        }
        ArrayList<Integer> values = new ArrayList<>();
        for (Long value : definition.getProperty(QUERY_FETCH_SIZES).getValue(Type.LONGS)) {
            if (value <= 0 || value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(QUERY_FETCH_SIZES
                        + " values must be positive integers");
            }
            values.add(value.intValue());
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException(QUERY_FETCH_SIZES + " must not be empty");
        }
        return values.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    protected String getDefaultFunctionName() {
        // Oak models suggest and spellcheck as native*lucene restrictions for every backend.
        return "lucene";
    }

    public static final class Builder extends IndexDefinition.Builder<MongotIndexDefinition> {

        @Override
        protected MongotIndexDefinition createInstance(NodeState definitionState) {
            return new MongotIndexDefinition(root, definitionState, indexPath);
        }
    }
}
