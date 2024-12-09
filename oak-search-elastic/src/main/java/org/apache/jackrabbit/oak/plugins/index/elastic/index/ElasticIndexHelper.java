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

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition.DEFAULT_SIMILARITY_METRIC;

import co.elastic.clients.json.JsonData;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.jetbrains.annotations.NotNull;

import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.mapping.DenseVectorProperty;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.util.ObjectBuilder;

import java.util.Arrays;
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

    private static final String OAK_WORD_DELIMITER_GRAPH_FILTER = "oak_word_delimiter_graph_filter";

    protected static final String SUGGEST_NESTED_VALUE = "value";

    protected static final String DYNAMIC_BOOST_NESTED_VALUE = "value";

    protected static final String DYNAMIC_BOOST_NESTED_BOOST = "boost";

    protected static final String DYNAMIC_PROPERTY_NAME = "name";

    protected static final String DYNAMIC_PROPERTY_VALUE = "value";

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
        builder.dynamic(Arrays
                .stream(DynamicMapping.values())
                .filter(dm -> dm.jsonValue().equals(indexDefinition.dynamicMapping))
                .findFirst().orElse(DynamicMapping.True)
        );
        mapInternalProperties(builder);
        mapIndexRules(builder, indexDefinition);
        if (indexDefinition.inferenceDefinition != null) {
            mapInferenceDefinition(builder, indexDefinition.inferenceDefinition);
        }
        return builder;
    }

    private static void mapInternalProperties(@NotNull TypeMapping.Builder builder) {
        builder.properties(FieldNames.PATH,
                        b1 -> b1.keyword(builder3 -> builder3))
                .properties(ElasticIndexDefinition.PATH_RANDOM_VALUE,
                        b1 -> b1.integer(b2 -> b2.docValues(true).index(false)))
                .properties(FieldNames.ANCESTORS,
                        b1 -> b1.text(
                                b2 -> b2.analyzer("ancestor_analyzer")
                                        .searchAnalyzer("keyword")
                                        .searchQuoteAnalyzer("keyword")))
                .properties(FieldNames.PATH_DEPTH,
                        b1 -> b1.integer(b2 -> b2.docValues(false)))
                .properties(FieldNames.FULLTEXT,
                        b1 -> b1.text(b2 -> b2.analyzer("oak_analyzer")))
                .properties(ElasticIndexDefinition.DYNAMIC_BOOST_FULLTEXT,
                        b1 -> b1.text(b2 -> b2.analyzer("oak_analyzer")))
                .properties(FieldNames.SPELLCHECK,
                        b1 -> b1.text(b2 -> b2.analyzer("trigram")))
                .properties(FieldNames.SUGGEST,
                        b1 -> b1.nested(
                                // TODO: evaluate https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-search-speed.html#faster-prefix-queries
                                b2 -> b2.properties(SUGGEST_NESTED_VALUE,
                                        b3 -> b3.text(
                                                b4 -> b4.analyzer("oak_analyzer")
                                        )
                                )
                        )
                )
                .properties(ElasticIndexDefinition.DYNAMIC_PROPERTIES, b1 -> b1.nested(
                                b2 -> b2.properties(DYNAMIC_PROPERTY_NAME, b3 -> b3.keyword(b4 -> b4))
                                        .properties(DYNAMIC_PROPERTY_VALUE,
                                                b3 -> b3.text(b4 -> b4.analyzer("oak_analyzer"))
                                        )
                        )
                )
                .properties(ElasticIndexDefinition.LAST_UPDATED, b -> b.date(d -> d));
        // TODO: the mapping below is for features currently not supported. These need to be reviewed
        // mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
        //  .field("type", "keyword")
        //  .endObject();
        // mappingBuilder.startObject(FieldNames.NULL_PROPS)
        // .field("type", "keyword")
        // .endObject();
    }

    private static void mapInferenceDefinition(@NotNull TypeMapping.Builder builder, @NotNull ElasticIndexDefinition.InferenceDefinition inferenceDefinition) {
        // Store the inference configuration in the index metadata so that it can be used by the inference service
        builder.meta("inference", JsonData.of(inferenceDefinition));

        if (inferenceDefinition.properties != null) {
            inferenceDefinition.properties.forEach(p -> builder.properties(p.name,
                    b -> b.object(bo -> bo
                            .properties("value", pb -> pb.denseVector(dv ->
                                            dv.index(true)
                                                    .dims(p.dims)
                                                    .similarity(p.similarity)
                                    )
                            )
                            .properties("metadata", pb -> pb.flattened(b1 -> b1))
                    )
            ));
        }
    }

    /**
     * Returns a {@code PutIndicesSettingsRequest} to make an index ready to be queried and updated in near real time.
     *
     * @param remoteIndexName the final index name (no alias)
     * @param indexDefinition the definition used to read settings/mappings
     * @return an {@code PutIndicesSettingsRequest}
     */
    public static PutIndicesSettingsRequest enableIndexRequest(String remoteIndexName, ElasticIndexDefinition indexDefinition) {
        IndexSettings indexSettings = IndexSettings.of(is -> is
                .numberOfReplicas(Integer.toString(indexDefinition.numberOfReplicas))
                // TODO: we should pass null to reset the refresh interval to the default value but the following bug prevents it. We need to wait for a fix
                // <a href="https://github.com/elastic/elasticsearch-java/issues/283">https://github.com/elastic/elasticsearch-java/issues/283</a>
                .refreshInterval(Time.of(t -> t.time("1s"))));

        return PutIndicesSettingsRequest.of(pisr -> pisr
                .index(remoteIndexName)
                .settings(indexSettings));
    }

    private static ObjectBuilder<IndexSettings> loadSettings(@NotNull IndexSettings.Builder builder,
                                                             @NotNull ElasticIndexDefinition indexDefinition) {
        // collect analyzer settings
        IndexSettingsAnalysis.Builder analyzerBuilder =
                ElasticCustomAnalyzer.buildCustomAnalyzers(indexDefinition.getAnalyzersNodeState(), "oak_analyzer");
        if (analyzerBuilder == null) {
            analyzerBuilder = new IndexSettingsAnalysis.Builder()
                    .filter(OAK_WORD_DELIMITER_GRAPH_FILTER,
                            tokenFilter -> tokenFilter.definition(
                                    tokenFilterDef -> tokenFilterDef.wordDelimiterGraph(
                                            wdgBuilder -> wdgBuilder.generateWordParts(true)
                                                    .stemEnglishPossessive(true)
                                                    .generateNumberParts(true)
                                                    .splitOnNumerics(indexDefinition.analyzerConfigSplitOnNumerics())
                                                    .splitOnCaseChange(indexDefinition.analyzerConfigSplitOnCaseChange())
                                                    .preserveOriginal(indexDefinition.analyzerConfigIndexOriginalTerms()))
                            ))
                    .analyzer("oak_analyzer",
                            ab -> ab.custom(
                                    customAnalyzer -> customAnalyzer.tokenizer("standard")
                                            .filter("lowercase", OAK_WORD_DELIMITER_GRAPH_FILTER)));
        }
        // path restrictions support
        analyzerBuilder.analyzer("ancestor_analyzer",
                ab -> ab.custom(customAnalyzer -> customAnalyzer.tokenizer("path_hierarchy")));

        // spellcheck support
        analyzerBuilder.filter("shingle",
                tokenFilter -> tokenFilter.definition(
                        tokenFilterDef -> tokenFilterDef.shingle(
                                shingle -> shingle.minShingleSize("2").maxShingleSize("3"))));
        analyzerBuilder.analyzer("trigram",
                ab -> ab.custom(
                        customAnalyzer -> customAnalyzer.tokenizer("standard").filter("lowercase", "shingle")));

        // set up the index
        builder.index(indexBuilder -> indexBuilder
                        // Make the index more lenient when a field cannot be converted to the mapped type. Without this setting
                        // the entire document will fail to update. Instead, only the specific field won't be updated.
                        .mapping(mf -> mf.ignoreMalformed(true))
                        // static setting: cannot be changed after the index gets created
                        .numberOfShards(Integer.toString(indexDefinition.numberOfShards))
                        // dynamic settings: see #enableIndexRequest
                        .refreshInterval(INITIAL_REFRESH_INTERVAL)
                        .numberOfReplicas(INITIAL_NUMBER_OF_REPLICAS))
                .analysis(analyzerBuilder.build());

        return builder;
    }

    private static void mapIndexRules(@NotNull TypeMapping.Builder builder,
                                      @NotNull ElasticIndexDefinition indexDefinition) {
        checkIndexRules(indexDefinition);
        for (Map.Entry<String, List<PropertyDefinition>> entry : indexDefinition.getPropertiesByName().entrySet()) {
            final String name = entry.getKey();
            final List<PropertyDefinition> propertyDefinitions = entry.getValue();
            Type<?> type = null;
            for (PropertyDefinition pd : propertyDefinitions) {
                type = Type.fromTag(pd.getType(), false);
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

            for (PropertyDefinition pd : indexDefinition.getDynamicBoostProperties()) {
                builder.properties(pd.nodeName,
                        b1 -> b1.nested(
                                b2 -> b2.properties(DYNAMIC_BOOST_NESTED_VALUE,
                                                b3 -> b3.text(
                                                        b4 -> b4.analyzer("oak_analyzer")))
                                        .properties(DYNAMIC_BOOST_NESTED_BOOST,
                                                b3 -> b3.double_(f -> f)
                                        )
                        )
                );
            }

            for (PropertyDefinition propertyDefinition : indexDefinition.getSimilarityProperties()) {
                ElasticPropertyDefinition pd = (ElasticPropertyDefinition) propertyDefinition;
                int denseVectorSize = pd.getSimilaritySearchDenseVectorSize();

                DenseVectorProperty denseVectorProperty = new DenseVectorProperty.Builder()
                    .index(true)
                    .dims(denseVectorSize)
                    .similarity(DEFAULT_SIMILARITY_METRIC)
                    .build();

                builder.properties(FieldNames.createSimilarityFieldName(pd.name), b1 -> b1.denseVector(denseVectorProperty));
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
