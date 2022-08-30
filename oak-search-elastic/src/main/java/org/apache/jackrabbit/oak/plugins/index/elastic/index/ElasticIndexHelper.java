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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides utility functions around Elasticsearch indexing
 */
class ElasticIndexHelper {

    private static final String ES_DENSE_VECTOR_DIM_PROP = "dims";

    // Unset the refresh interval and disable replicas at index creation to optimize for initial loads
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html
    private static final String INITIAL_REFRESH_INTERVAL = "-1";
    private static final int INITIAL_NUMBER_OF_REPLICAS = 0;

    /**
     * Returns a {@code CreateIndexRequest} with settings and mappings translated from the specified {@code ElasticIndexDefinition}.
     * The returned object can be used to create and index optimized for bulk loads (eg: reindexing) but not for queries.
     * To make it usable, a #enableIndexRequest needs to be performed.
     *
     * @param remoteIndexName the final index name
     * @param indexDefinition the definition used to read settings/mappings
     * @return a {@code CreateIndexRequest}
     * @throws IOException if an error happens while creating the request
     *                     <p>
     *                     TODO: index create cannot be migrated to the ES Java client: it does not support custom mappings/settings needed to configure elastiknn.
     *                     See discussion in https://discuss.elastic.co/t/elasticsearch-java-client-support-for-custom-mappings-settings/303172
     *                     The migration will continue when this roadmap item gets fixed https://github.com/elastic/elasticsearch-java/issues/252
     */
    public static CreateIndexRequest createIndexRequest(String remoteIndexName, ElasticIndexDefinition indexDefinition) throws IOException {
        final CreateIndexRequest request = new CreateIndexRequest(remoteIndexName);

        // provision settings
        request.settings(loadSettings(indexDefinition));

        // provision mappings
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
                mapInternalProperties(mappingBuilder);
                mapIndexRules(indexDefinition, mappingBuilder);
            }
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        request.mapping(mappingBuilder);

        return request;
    }

    /**
     * Returns a {@code UpdateSettingsRequest} to make an index ready to be queried and updated in near real time.
     *
     * @param remoteIndexName the final index name (no alias)
     * @param indexDefinition the definition used to read settings/mappings
     * @return an {@code UpdateSettingsRequest}
     * <p>
     * TODO: migrate to Elasticsearch Java client when the following issue will be fixed
     * <a href="https://github.com/elastic/elasticsearch-java/issues/283">https://github.com/elastic/elasticsearch-java/issues/283</a>
     */
    public static UpdateSettingsRequest enableIndexRequest(String remoteIndexName, ElasticIndexDefinition indexDefinition) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(remoteIndexName);

        Settings.Builder settingsBuilder = Settings.builder()
                .putNull("index.refresh_interval") // null=reset a setting back to the default value
                .put("index.number_of_replicas", indexDefinition.numberOfReplicas);

        return request.settings(settingsBuilder);
    }

    private static XContentBuilder loadSettings(ElasticIndexDefinition indexDefinition) throws IOException {
        final XContentBuilder settingsBuilder = XContentFactory.jsonBuilder();
        settingsBuilder.startObject();
        if (indexDefinition.getSimilarityProperties().size() > 0) {
            settingsBuilder.field("elastiknn", true);
        }
        // static setting: cannot be changed after the index gets created
        settingsBuilder.field("index.number_of_shards", indexDefinition.numberOfShards);

        // dynamic settings: see #enableIndexRequest
        settingsBuilder.field("index.refresh_interval", INITIAL_REFRESH_INTERVAL);
        settingsBuilder.field("index.number_of_replicas", INITIAL_NUMBER_OF_REPLICAS);
        {
            settingsBuilder.startObject("analysis");
            {
                settingsBuilder.startObject("filter");
                {
                    settingsBuilder.startObject("oak_word_delimiter_graph_filter");
                    {
                        settingsBuilder.field("type", "word_delimiter_graph");
                        settingsBuilder.field("generate_word_parts", true);
                        settingsBuilder.field("stem_english_possessive", true);
                        settingsBuilder.field("generate_number_parts", true);
                        settingsBuilder.field("split_on_numerics", indexDefinition.analyzerConfigSplitOnNumerics());
                        settingsBuilder.field("split_on_case_change", indexDefinition.analyzerConfigSplitOnCaseChange());
                        settingsBuilder.field("preserve_original", indexDefinition.analyzerConfigIndexOriginalTerms());
                    }
                    settingsBuilder.endObject();

                    settingsBuilder.startObject("shingle")
                            .field("type", "shingle")
                            .field("min_shingle_size", 2)
                            .field("max_shingle_size", 3)
                            .endObject();
                }
                settingsBuilder.endObject();

                settingsBuilder.startObject("analyzer");
                {
                    settingsBuilder.startObject("oak_analyzer");
                    {
                        settingsBuilder.field("type", "custom");
                        settingsBuilder.field("tokenizer", "standard");
                        settingsBuilder.field("filter", new String[]{"lowercase", "oak_word_delimiter_graph_filter"});
                    }
                    settingsBuilder.endObject();
                    // https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pathhierarchy-tokenizer.html
                    settingsBuilder.startObject("ancestor_analyzer");
                    {
                        settingsBuilder.field("type", "custom");
                        settingsBuilder.field("tokenizer", "path_hierarchy");
                    }
                    settingsBuilder.endObject();

                    settingsBuilder.startObject("trigram")
                            .field("type", "custom")
                            .field("tokenizer", "standard")
                            .array("filter", "lowercase", "shingle")
                            .endObject();
                }
                settingsBuilder.endObject();
            }
            settingsBuilder.endObject();
        }
        settingsBuilder.endObject();
        return settingsBuilder;
    }

    private static void mapInternalProperties(XContentBuilder mappingBuilder) throws IOException {
        mappingBuilder.startObject(FieldNames.PATH)
                .field("type", "keyword")
                .endObject();
        mappingBuilder.startObject(FieldNames.ANCESTORS)
                .field("type", "text")
                .field("analyzer", "ancestor_analyzer")
                .field("search_analyzer", "keyword")
                .field("search_quote_analyzer", "keyword")
                .endObject();
        mappingBuilder.startObject(FieldNames.PATH_DEPTH)
                .field("type", "integer")
                .field("doc_values", false) // no need to sort/aggregate here
                .endObject();
        mappingBuilder.startObject(FieldNames.FULLTEXT)
                .field("type", "text")
                .field("analyzer", "oak_analyzer")
                .endObject();
        // TODO: the mapping below is for features currently not supported. These need to be reviewed
        // mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
        //  .field("type", "keyword")
        //  .endObject();
        // mappingBuilder.startObject(FieldNames.NULL_PROPS)
        // .field("type", "keyword")
        // .endObject();
    }

    private static void mapIndexRules(ElasticIndexDefinition indexDefinition, XContentBuilder mappingBuilder) throws IOException {
        checkIndexRules(indexDefinition);
        boolean useInSuggest = false;
        for (Map.Entry<String, List<PropertyDefinition>> entry : indexDefinition.getPropertiesByName().entrySet()) {
            final String pdName = entry.getKey();
            final List<PropertyDefinition> pds = entry.getValue();
            Type<?> type = null;
            for (PropertyDefinition pd : pds) {
                type = Type.fromTag(pd.getType(), false);
                if (pd.useInSuggest) {
                    useInSuggest = true;
                }
            }

            mappingBuilder.startObject(pdName);
            {
                // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
                if (Type.BINARY.equals(type)) {
                    mappingBuilder.field("type", "binary");
                } else if (Type.LONG.equals(type)) {
                    mappingBuilder.field("type", "long");
                } else if (Type.DOUBLE.equals(type) || Type.DECIMAL.equals(type)) {
                    mappingBuilder.field("type", "double");
                } else if (Type.DATE.equals(type)) {
                    mappingBuilder.field("type", "date");
                } else if (Type.BOOLEAN.equals(type)) {
                    mappingBuilder.field("type", "boolean");
                } else {
                    if (indexDefinition.isAnalyzed(pds)) {
                        mappingBuilder.field("type", "text");
                        mappingBuilder.field("analyzer", "oak_analyzer");
                        // always add keyword for sorting / faceting as sub-field
                        mappingBuilder.startObject("fields");
                        {
                            mappingBuilder.startObject("keyword")
                                    .field("type", "keyword")
                                    .field("ignore_above", 256)
                                    .endObject();
                        }
                        mappingBuilder.endObject();
                    } else {
                        if (indexDefinition.isIndexed(pds)) {
                            mappingBuilder
                                    .field("type", "keyword")
                                    .field("ignore_above", 256);
                        } else {
                            if (indexDefinition.isOrdered(pds) || indexDefinition.isFaceted(pds)) {
                                mappingBuilder
                                        .field("type", "keyword")
                                        .field("ignore_above", 256)
                                        .field("index", false);
                            } else {
                                //TODO uncomment the following tests to enable useInExcerpts
                                /*if ( indexDefinition.useInExcerpts(pds)) {
                                    mappingBuilder
                                            .field("type", "keyword")
                                            .field("ignore_above", 256)
                                            .field("index", false)
                                            .field("doc-values", false);
                                }*/
                            }
                        }
                    }
                }
            }
            mappingBuilder.endObject();
        }

        mappingBuilder.startObject(FieldNames.SPELLCHECK)
                .field("type", "text").field("analyzer", "trigram")
                .endObject();

        if (useInSuggest) {
            mappingBuilder.startObject(FieldNames.SUGGEST);
            {
                mappingBuilder.field("type", "nested");
                mappingBuilder.startObject("properties");
                {
                    // TODO: evaluate https://www.elastic.co/guide/en/elasticsearch/reference/current/faster-prefix-queries.html
                    mappingBuilder.startObject("value")
                            .field("type", "text")
                            .field("analyzer", "oak_analyzer")
                            .endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }

        for (PropertyDefinition pd : indexDefinition.getDynamicBoostProperties()) {
            mappingBuilder.startObject(pd.nodeName);
            {
                mappingBuilder.field("type", "nested");
                mappingBuilder.startObject("properties");
                {
                    mappingBuilder.startObject("value")
                            .field("type", "text")
                            .field("analyzer", "oak_analyzer")
                            .endObject();
                    mappingBuilder.startObject("boost")
                            .field("type", "double")
                            .endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }

        for (PropertyDefinition propertyDefinition : indexDefinition.getSimilarityProperties()) {
            ElasticPropertyDefinition pd = (ElasticPropertyDefinition) propertyDefinition;
            int denseVectorSize = pd.getSimilaritySearchDenseVectorSize();
            mappingBuilder.startObject(FieldNames.createSimilarityFieldName(pd.name));
            {
                mappingBuilder.field("type", "elastiknn_dense_float_vector");
                mappingBuilder.startObject("elastiknn");
                {
                    mappingBuilder.field(ES_DENSE_VECTOR_DIM_PROP, denseVectorSize);
                    mappingBuilder.field("model", "lsh");
                    mappingBuilder.field("similarity", pd.getSimilaritySearchParameters().getIndexTimeSimilarityFunction());
                    mappingBuilder.field("L", pd.getSimilaritySearchParameters().getL());
                    mappingBuilder.field("k", pd.getSimilaritySearchParameters().getK());
                    mappingBuilder.field("w", pd.getSimilaritySearchParameters().getW());
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }

        mappingBuilder.startObject(ElasticIndexDefinition.SIMILARITY_TAGS)
                .field("type", "text")
                .field("analyzer", "oak_analyzer")
                .endObject();
    }

    // we need to check if in the defined rules there are properties with the same name and different types
    private static void checkIndexRules(ElasticIndexDefinition indexDefinition) {
        final List<Map.Entry<String, List<PropertyDefinition>>> multiTypesFields = indexDefinition.getPropertiesByName()
                .entrySet()
                .stream()
                .filter(e -> e.getValue().size() > 1)
                .filter(e -> e.getValue().stream().map(PropertyDefinition::getType).distinct().count() > 1)
                .collect(Collectors.toList());

        if (!multiTypesFields.isEmpty()) {
            String fields = multiTypesFields.stream().map(Map.Entry::getKey).collect(Collectors.joining(", ", "[", "]"));
            throw new IllegalStateException(indexDefinition.getIndexPath() + " has properties with the same name and " +
                    "different types " + fields);
        }
    }
}
