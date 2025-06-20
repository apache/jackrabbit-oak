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
import co.elastic.clients.elasticsearch._types.mapping.DenseVectorProperty;
import co.elastic.clients.elasticsearch._types.mapping.DenseVectorSimilarity;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.IndexSettingsAnalysis;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.json.DelegatingDeserializer;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.json.JsonpMapperBase;
import co.elastic.clients.json.ObjectDeserializer;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.json.stream.JsonParser;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConfig;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceIndexConfig;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticPropertyDefinition.DEFAULT_SIMILARITY_METRIC;

/**
 * Provides utility functions around Elasticsearch indexing
 */
class ElasticIndexHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticIndexHelper.class);
    private static final ObjectMapper mapper = new ObjectMapper();
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
        // We only add mappings if both the inference config (in queryEngineSettings config) and the inference index config are enabled.
        if (InferenceConfig.getInstance().isInferenceEnabled() && InferenceConfig.getInstance().isEnabled()) {
            mapInferenceConfig(builder, indexDefinition, InferenceConfig.getInstance());
        }
        return builder;
    }

    private static void mapInferenceConfig(TypeMapping.Builder builder, @NotNull ElasticIndexDefinition indexDefinition, @NotNull InferenceConfig inferenceConfig) {
        String indexName = PathUtils.getName(indexDefinition.getIndexName());

        InferenceIndexConfig inferenceIndexConfig = inferenceConfig.getInferenceIndexConfig(indexName);
        if (InferenceIndexConfig.NOOP.equals(inferenceIndexConfig)) {
            return;
        }
        try {
            // We are already validating the enricherConfigJson in the InferenceIndexConfig constructor
            Map<String, Object> enricherConfigJson = mapper.readValue(inferenceConfig.getInferenceIndexConfig(indexName).getEnricherConfig(),
                    new TypeReference<Map<String, Object>>() {
                    });
            // Store the enricher configuration in the index metadata so that it can be used by the enricher service
            enricherConfigJson.forEach((k, v) -> {
                builder.meta(k, JsonData.of(v));
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("enricherConfig parsing should never fail as it is validated in InferenceIndexConfig" + e.getMessage());
        }

        //todo: we should make these mappings configurable
        builder.properties(InferenceConstants.VECTOR_SPACES, b -> b.object(spaces -> {
                for (var inferenceModelConfig : inferenceIndexConfig.getInferenceModelConfigs().entrySet()) {
                    if (inferenceModelConfig.getValue().isEnabled()) {
                        spaces.properties(inferenceModelConfig.getKey(), v -> v.nested(vb -> {
                            vb.properties("id", p -> p.keyword(k -> k));
                            vb.properties("vector", p -> p.denseVector(dv -> dv));
                            vb.properties("metadata", p -> p.object(o -> o.enabled(false)));
                            return vb;
                        }));
                    }
                }
                return spaces;
            }))
            .properties(InferenceConstants.ENRICH_NODE, b-> b.object(s -> s.properties(
                jsonToMapping(InferenceConfig.getInstance().getEnricherStatusMapping()))));
    }

    private static void mapInternalProperties(@NotNull TypeMapping.Builder builder) {
        builder.properties(FieldNames.PATH,
                        // path cannot be used for searches, just for sorting
                        p -> p.keyword(k -> k.docValues(true).index(false)))
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
                .properties(ElasticIndexDefinition.LAST_UPDATED, b -> b.date(d -> d))
                .properties(FieldNames.NULL_PROPS, p -> p.keyword(k -> k.docValues(false)));
    }

    private static void mapInferenceDefinition(@NotNull TypeMapping.Builder builder, @NotNull ElasticIndexDefinition.InferenceDefinition inferenceDefinition) {
        // Store the inference configuration in the index metadata so that it can be used by the inference service
        builder.meta("inference", JsonData.of(inferenceDefinition));

        if (inferenceDefinition.properties != null) {
            inferenceDefinition.properties.forEach(p -> builder.properties(
                    ElasticIndexUtils.fieldName(p.name),
                    b -> b.object(bo -> bo
                            .properties("value", pb -> pb.denseVector(dv ->
                                            dv.index(true)
                                                    .dims(p.dims)
                                                    .similarity(
                                                            Arrays.stream(DenseVectorSimilarity.values()).filter(s ->
                                                                    Objects.equals(s.jsonValue(), p.similarity)).findAny().orElseThrow()
                                                    )
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
                                shingle -> shingle.minShingleSize(2).maxShingleSize(3))));
        analyzerBuilder.analyzer("trigram",
                ab -> ab.custom(
                        customAnalyzer -> customAnalyzer.tokenizer("standard").filter("lowercase", "shingle")));

        // set up the index
        builder.index(indexBuilder -> indexBuilder
                        // Make the index more lenient when a field cannot be converted to the mapped type. Without this setting
                        // the entire document will fail to update. Instead, only the specific field won't be updated.
                        .mapping(mf -> mf.ignoreMalformed(true).
                                totalFields(f -> f.limit(Long.toString(indexDefinition.limitTotalFields))))
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
        for (IndexingRule rule : indexDefinition.getDefinedRules()) {
            Iterable<PropertyDefinition> iterable = rule.getNamePatternsProperties()::iterator;
            for (PropertyDefinition pd : iterable) {
                ElasticPropertyDefinition epd = (ElasticPropertyDefinition) pd;
                if (epd.isFlattened()) {
                    Property.Builder pBuilder = new Property.Builder();
                    pBuilder.flattened(b2 -> b2.index(true));
                    builder.properties(FieldNames.FLATTENED_FIELD_PREFIX +
                            ElasticIndexUtils.fieldName(pd.nodeName), pBuilder.build());
                }
            }
        }
        for (Map.Entry<String, List<PropertyDefinition>> entry : indexDefinition.getPropertiesByName().entrySet()) {
            String propertyName = entry.getKey();
            String fieldName = ElasticIndexUtils.fieldName(propertyName);
            List<PropertyDefinition> propertyDefinitions = entry.getValue();
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
            builder.properties(fieldName, pBuilder.build());

            for (PropertyDefinition pd : indexDefinition.getDynamicBoostProperties()) {
                builder.properties(ElasticIndexUtils.fieldName(pd.nodeName),
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
                        .similarity(
                                Arrays.stream(DenseVectorSimilarity.values()).filter(s ->
                                        Objects.equals(s.jsonValue(), DEFAULT_SIMILARITY_METRIC)).findAny().orElseThrow()
                        )
                        .build();

                builder.properties(FieldNames.createSimilarityFieldName(
                                ElasticIndexUtils.fieldName(pd.name)),
                        b1 -> b1.denseVector(denseVectorProperty));
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

    public static Map<String, Property> jsonToMapping(String json) {
        TypeMapping typeMapping = withJson(new TypeMapping.Builder(),
            new StringReader(json), new JacksonJsonpMapper()).build();
        return typeMapping.properties();
    }

    // https://discuss.elastic.co/t/reusing-internal-implementation-to-transform-json-mapping-to-es-mapping/300597/3

    /**
     * Helper method to deserialize JSON into Elasticsearch objects using the client's internal deserializers.
     *
     * @param builder Builder object for the target type
     * @param json    Reader containing the JSON to deserialize
     * @param mapper  JsonpMapper to use for deserialization
     * @param <T>     The type of object to be built
     * @param <B>     The builder type
     * @return Builder object populated with data from the JSON
     */
    private static <T, B extends ObjectBuilder<T>> B withJson(
        B builder, Reader json, JsonpMapper mapper) {
        // Find which deserializer is needed for the builder's class
        JsonpDeserializer<?> classDeserializer =
            JsonpMapperBase.findDeserializer(builder.getClass().getEnclosingClass());

        @SuppressWarnings("unchecked")
        ObjectDeserializer<B> builderDeserializer =
            (ObjectDeserializer<B>) DelegatingDeserializer.unwrap(classDeserializer);

        JsonParser parser = mapper.jsonProvider().createParser(json);
        builderDeserializer.deserialize(builder, parser, mapper, parser.next());
        return builder;
    }
}
