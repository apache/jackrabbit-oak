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
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;

import co.elastic.clients.elasticsearch._types.mapping.BinaryProperty;
import co.elastic.clients.elasticsearch._types.mapping.BooleanProperty;
import co.elastic.clients.elasticsearch._types.mapping.DateProperty;
import co.elastic.clients.elasticsearch._types.mapping.DoubleNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping.Builder;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides utility functions around Elasticsearch indexing.
 *
 * TODO: this class has been partially migrated to use the new Elasticsearch Java Client. It cannot be used yet since
 * the new client does not support custom mappings/settings needed to configure elastiknn.
 *
 * See discussion in https://discuss.elastic.co/t/elasticsearch-java-client-support-for-custom-mappings-settings/303172
 *
 * The migration will continue when this roadmap item gets fixed https://github.com/elastic/elasticsearch-java/issues/252
 */
class ElasticIndexHelper2 {

    private static final String ES_DENSE_VECTOR_DIM_PROP = "dims";

    // Unset the refresh interval and disable replicas at index creation to optimize for initial loads
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html
    private static final String INITIAL_REFRESH_INTERVAL = "-1";
    private static final int INITIAL_NUMBER_OF_REPLICAS = 0;

    /**
     * Returns a {@code CreateIndexRequest} with settings and mappings translated from the specified {@code ElasticIndexDefinition}.
     * The returned object can be used to create and index optimized for bulk loads (eg: reindexing) but not for queries.
     * To make it usable, a #enableIndexRequest needs to be performed.
     * @param remoteIndexName the final index name
     * @param indexDefinition the definition used to read settings/mappings
     * @return a {@code CreateIndexRequest}
     * @throws IOException if an error happens while creating the request
     */
    public static CreateIndexRequest createIndexRequest(String remoteIndexName, ElasticIndexDefinition indexDefinition) throws IOException {
        return new CreateIndexRequest.Builder()
                .index(remoteIndexName)
                .settings(loadSettings(indexDefinition))
                .mappings(mapIndexRules(mapInternalProperties(indexDefinition),indexDefinition))
                .build();
    }

    /**
     * Returns a {@code UpdateSettingsRequest} to make an index ready to be queried and updated in near real time.
     * @param remoteIndexName the final index name (no alias)
     * @param indexDefinition the definition used to read settings/mappings
     * @return an {@code UpdateSettingsRequest}
     */
    public static PutIndicesSettingsRequest enableIndexRequest(String remoteIndexName, ElasticIndexDefinition indexDefinition) {
        return PutIndicesSettingsRequest.of(r -> r
                .index(remoteIndexName)
                .settings(s -> s
                        .refreshInterval(i -> i)// null=reset a setting back to the default value
                        .numberOfReplicas(String.valueOf(indexDefinition.numberOfReplicas))));
        // TODO check the refresh interval to null
    }
    
    private static IndexSettings loadSettings(ElasticIndexDefinition indexDefinition) throws IOException {
        return new IndexSettings.Builder()
                .numberOfShards(String.valueOf(indexDefinition.numberOfShards))// dynamic settings: see #enableIndexRequest
                .numberOfReplicas(String.valueOf(INITIAL_NUMBER_OF_REPLICAS))
                .refreshInterval(t -> t.time(INITIAL_REFRESH_INTERVAL))
                .analysis(a -> a.filter("oak_word_delimiter_graph_filter", f -> f
                                .definition(d -> d
                                        .wordDelimiterGraph(w -> w
                                                .generateWordParts(true)
                                                .stemEnglishPossessive(true)
                                                .generateNumberParts(true)
                                                .preserveOriginal(indexDefinition.indexOriginalTerms()))))
                        .filter("shingle", f -> f
                                .definition(d -> d
                                        .shingle(s -> s
                                                .minShingleSize("2")
                                                .maxShingleSize("3"))))
                        .analyzer("oak_analyzer", o -> o
                                .custom(c -> c
                                        .tokenizer("standard")
                                        .filter("lowercase", "oak_word_delimiter_graph_filter")))
                        .analyzer("ancestor_analyzer", o -> o
                                .custom(c -> c
                                        .tokenizer("path_hierarchy")))
                        .analyzer("trigram", o -> o
                                .custom(c -> c
                                        .tokenizer("standard")
                                        .filter("lowercase", "shingle"))))
                .build();
    }

    private static TypeMapping.Builder mapInternalProperties(ElasticIndexDefinition indexDefinition) {
        return new TypeMapping.Builder()
                .properties(FieldNames.PATH, f -> f.keyword(k -> k))
                .properties(FieldNames.ANCESTORS, f -> f
                        .text(t -> t
                                .analyzer("ancestor_analyzer")
                                .searchAnalyzer("keyword")
                                .searchQuoteAnalyzer("keyword")))
                .properties(FieldNames.PATH_DEPTH, f -> f.integer(i -> i.docValues(false)))
                .properties(FieldNames.FULLTEXT, f -> f.text(t -> t.analyzer("oak_analyzer")))
                // TODO: the mapping below is for features currently not supported. These need to be reviewed
                //.properties(FieldNames.NOT_NULL_PROPS, f->f
                //        .keyword(k->k))
                //.properties(FieldNames.NULL_PROPS, f->f
                //        .keyword(k->k))
                //
                // mapIndexRules
                ;
    }
    
    private static TypeMapping mapIndexRules(Builder mapInternalProperties, ElasticIndexDefinition indexDefinition) throws IOException {
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
            Property property = null;
            {
                // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
                if (Type.BINARY.equals(type)) {
                    property = new Property(BinaryProperty.of(b -> b));
                    property.binary();
                } else if (Type.LONG.equals(type)) {
                    property = new Property(LongNumberProperty.of(b -> b));
                } else if (Type.DOUBLE.equals(type) || Type.DECIMAL.equals(type)) {
                    property = new Property(DoubleNumberProperty.of(b -> b));
                } else if (Type.DATE.equals(type)) {
                    property = new Property(DateProperty.of(b -> b));
                } else if (Type.BOOLEAN.equals(type)) {
                    property = new Property(BooleanProperty.of(b -> b));
                } else {
                    if (indexDefinition.isAnalyzed(propertyDefinitions)) {
                        property = new Property(TextProperty.of(b -> b
                                .analyzer("oak_analyzer")
                                // always add keyword for sorting / faceting as subfield
                                .fields("keyword", t -> t.keyword(k -> k.ignoreAbove(256)))));
                    } else {
                        // always add keyword for sorting / faceting
                        property = new Property(KeywordProperty.of(b -> b
                                .ignoreAbove(256)));
                    }
                }
            }
            mapInternalProperties.properties(name, property);
        }
        mapInternalProperties.properties(FieldNames.SPELLCHECK, f -> f.text(t -> t.analyzer("trigram")));
        if (useInSuggest) {
            mapInternalProperties.properties(FieldNames.SUGGEST, f -> f
                    .nested(n -> n
                            // TODO: evaluate https://www.elastic.co/guide/en/elasticsearch/reference/current/faster-prefix-queries.html
                            .properties("value", v -> v
                                    .text(t -> t.analyzer("oak_analyzer")))));
        }

        for (PropertyDefinition pd : indexDefinition.getDynamicBoostProperties()) {
            mapInternalProperties.properties(pd.nodeName, f -> f
                    .nested(n -> n
                            .properties("value", ff -> ff
                                    .text(t -> t.analyzer("oak_analyzer")))
                            .properties("boost", ff -> ff.double_(d -> d))));
        }

        // TODO define Property Type for Elastiknn
//        for (PropertyDefinition propertyDefinition : indexDefinition.getSimilarityProperties()) {
//            ElasticPropertyDefinition pd = (ElasticPropertyDefinition) propertyDefinition;
//            int denseVectorSize = pd.getSimilaritySearchDenseVectorSize();
//        
//            
//            mappingBuilder.startObject(FieldNames.createSimilarityFieldName(pd.name));
//            {
//                mappingBuilder.field("type", "elastiknn_dense_float_vector");
//                mappingBuilder.startObject("elastiknn");
//                {
//                    mappingBuilder.field(ES_DENSE_VECTOR_DIM_PROP, denseVectorSize);
//                    mappingBuilder.field("model", "lsh");
//                    mappingBuilder.field("similarity", pd.getSimilaritySearchParameters().getIndexTimeSimilarityFunction());
//                    mappingBuilder.field("L", pd.getSimilaritySearchParameters().getL());
//                    mappingBuilder.field("k", pd.getSimilaritySearchParameters().getK());
//                    mappingBuilder.field("w", pd.getSimilaritySearchParameters().getW());
//                }
//                mappingBuilder.endObject();
//            }
//            mappingBuilder.endObject();
//        }

        mapInternalProperties.properties(ElasticIndexDefinition.SIMILARITY_TAGS, f->f
                .text(t->t
                        .analyzer("oak_analyzer")));
        
        return mapInternalProperties.build();
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
