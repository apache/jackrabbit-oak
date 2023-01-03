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

import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides utility functions around Elasticsearch indexing
 */
class ElasticIndexHelper {

    // Unset the refresh interval and disable replicas at index creation to optimize for initial loads
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html
    private static final Time INITIAL_REFRESH_INTERVAL = Time.of(b -> b.time("-1"));
    private static final String INITIAL_NUMBER_OF_REPLICAS = "0";

    /**
     * Returns a {@code CreateIndexRequest} with settings and mappings translated from the specified {@code ElasticIndexDefinition}.
     * The returned object can be used to create and index optimized for bulk loads (eg: reindexing) but not for queries.
     * To make it usable, a #enableIndexRequest needs to be performed.
     *
     * @param remoteIndexName the final index name
     * @param indexDefinition the definition used to read settings/mappings
     * @return a {@code CreateIndexRequest}
     */
    public static CreateIndexRequest createIndexRequest(@NotNull String remoteIndexName,
                                                        @NotNull ElasticIndexDefinition indexDefinition) {
        return new CreateIndexRequest.Builder()
                .index(remoteIndexName)
                .settings(s -> loadSettings(s, indexDefinition))
                .mappings(s -> loadMappings(s, indexDefinition))
                .build();
    }

    private static ObjectBuilder<TypeMapping> loadMappings(@NotNull TypeMapping.Builder builder,
                                                           @NotNull ElasticIndexDefinition indexDefinition) {
        mapInternalProperties(builder);
        mapIndexRules(builder, indexDefinition);
        return builder;
    }

    private static void mapInternalProperties(@NotNull TypeMapping.Builder builder) {
        builder.properties(FieldNames.PATH,
                        b1 -> b1.keyword(builder3 -> builder3))
                .properties(FieldNames.ANCESTORS,
                        b1 -> b1.text(
                                b2 -> b2.analyzer("ancestor_analyzer")
                                        .searchAnalyzer("keyword")
                                        .searchQuoteAnalyzer("keyword")))
                .properties(FieldNames.PATH_DEPTH,
                        b1 -> b1.integer(
                                b2 -> b2.docValues(false)))
                .properties(FieldNames.FULLTEXT,
                        b1 -> b1.text(
                                b2 -> b2.analyzer("oak_analyzer")));
        // TODO: the mapping below is for features currently not supported. These need to be reviewed
        // mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
        //  .field("type", "keyword")
        //  .endObject();
        // mappingBuilder.startObject(FieldNames.NULL_PROPS)
        // .field("type", "keyword")
        // .endObject();
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


    private static ObjectBuilder<IndexSettings> loadSettings(@NotNull IndexSettings.Builder builder,
                                                             @NotNull ElasticIndexDefinition indexDefinition) {
        if (indexDefinition.getSimilarityProperties().size() > 0) {
            builder.otherSettings(ElasticIndexDefinition.ELASTIKNN, JsonData.of(true));
        }
        builder.index(indexBuilder -> indexBuilder
                        // static setting: cannot be changed after the index gets created
                        .numberOfShards(Integer.toString(indexDefinition.numberOfShards))
                        // dynamic settings: see #enableIndexRequest
                        .refreshInterval(INITIAL_REFRESH_INTERVAL)
                        .numberOfReplicas(INITIAL_NUMBER_OF_REPLICAS))
                .analysis(b1 ->
                        b1.filter("oak_word_delimiter_graph_filter",
                                        b2 -> b2.definition(
                                                b3 -> b3.wordDelimiterGraph(
                                                        wdgBuilder -> wdgBuilder.generateWordParts(true)
                                                                .stemEnglishPossessive(true)
                                                                .generateNumberParts(true)
                                                                .splitOnNumerics(indexDefinition.analyzerConfigSplitOnNumerics())
                                                                .splitOnCaseChange(indexDefinition.analyzerConfigSplitOnCaseChange())
                                                                .preserveOriginal(indexDefinition.analyzerConfigIndexOriginalTerms()))
                                        ))
                                .filter("shingle",
                                        b2 -> b2.definition(
                                                b3 -> b3.shingle(
                                                        b4 -> b4.minShingleSize("2")
                                                                .maxShingleSize("3"))))
                                .analyzer("oak_analyzer",
                                        b2 -> b2.custom(
                                                b3 -> b3.tokenizer("standard")
                                                        .filter("lowercase", "oak_word_delimiter_graph_filter")))
                                .analyzer("ancestor_analyzer",
                                        b2 -> b2.custom(
                                                b3 -> b3.tokenizer("path_hierarchy")))
                                .analyzer("trigram",
                                        b2 -> b2.custom(
                                                b3 -> b3.tokenizer("standard")
                                                        .filter("lowercase", "shingle")))

                );

        return builder;
    }

    private static void mapIndexRules(@NotNull TypeMapping.Builder builder,
                                      @NotNull ElasticIndexDefinition indexDefinition) {
        checkIndexRules(indexDefinition);
        boolean useInSuggest = false;
        for (Map.Entry<String, List<PropertyDefinition>> entry : indexDefinition.getPropertiesByName().entrySet()) {
            final String name = entry.getKey();
            final List<PropertyDefinition> propertyDefinitions = entry.getValue();
            Type<?> type = null;
            for (PropertyDefinition pd : propertyDefinitions) {
                type = Type.fromTag(pd.getType(), false);
                if (pd.useInSuggest) {
                    useInSuggest = true;
                }
            }

            Property.Builder pBuilder = new Property.Builder();
            // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
            if (Type.BINARY.equals(type)) {
                pBuilder.binary(b -> b);
            } else if (Type.LONG.equals(type)) {
                pBuilder.long_(b -> b);
            } else if (Type.DOUBLE.equals(type) || Type.DECIMAL.equals(type)) {
                pBuilder.double_(b -> b);
            } else if (Type.DATE.equals(type)) {
                pBuilder.date(b -> b);
            } else if (Type.BOOLEAN.equals(type)) {
                pBuilder.boolean_(b -> b);
            } else {
                if (indexDefinition.isAnalyzed(propertyDefinitions)) {
                    // always add keyword for sorting / faceting as sub-field
                    pBuilder.text(
                            b1 -> b1.analyzer("oak_analyzer")
                                    .fields("keyword",
                                            b2 -> b2.keyword(
                                                    b3 -> b3.ignoreAbove(256))));
                } else {
                    // always add keyword for sorting / faceting
                    pBuilder.keyword(b1 -> b1.ignoreAbove(256));
                }
            }
            builder.properties(name, pBuilder.build());

            builder.properties(FieldNames.SPELLCHECK,
                    b1 -> b1.text(
                            b2 -> b2.analyzer("trigram"))
            );

            if (useInSuggest) {
                builder.properties(FieldNames.SUGGEST,
                        b1 -> b1.nested(
                                // TODO: evaluate https://www.elastic.co/guide/en/elasticsearch/reference/current/faster-prefix-queries.html
                                b2 -> b2.properties("value",
                                        b3 -> b3.text(
                                                b4 -> b4.analyzer("oak_analyzer")
                                        )
                                )
                        )
                );
            }

            for (PropertyDefinition pd : indexDefinition.getDynamicBoostProperties()) {
                builder.properties(pd.nodeName,
                        b1 -> b1.nested(
                                b2 -> b2.properties("value",
                                                b3 -> b3.text(
                                                        b4 -> b4.analyzer("oak_analyzer")))
                                        .properties("boost",
                                                b3 -> b3.double_(f -> f)
                                        )
                        )
                );
            }

            for (PropertyDefinition propertyDefinition : indexDefinition.getSimilarityProperties()) {
                ElasticPropertyDefinition pd = (ElasticPropertyDefinition) propertyDefinition;
                int denseVectorSize = pd.getSimilaritySearchDenseVectorSize();

                Reader eknnConfig = new StringReader(
                        "{" +
                                "  \"type\": \"elastiknn_dense_float_vector\"," +
                                "  \"elastiknn\": {" +
                                "    \"dims\": " + denseVectorSize + "," +
                                "    \"model\": \"lsh\"," +
                                "    \"similarity\": \"" + pd.getSimilaritySearchParameters().getIndexTimeSimilarityFunction() + "\"," +
                                "    \"L\": " + pd.getSimilaritySearchParameters().getL() + "," +
                                "    \"k\": " + pd.getSimilaritySearchParameters().getK() + "," +
                                "    \"w\": " + pd.getSimilaritySearchParameters().getW() + "" +
                                "  }" +
                                "}");

                builder.properties(FieldNames.createSimilarityFieldName(pd.name), b1 -> b1.withJson(eknnConfig));
            }

            builder.properties(ElasticIndexDefinition.SIMILARITY_TAGS,
                    b1 -> b1.text(
                            b2 -> b2.analyzer("oak_analyzer")
                    )
            );
        }
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
