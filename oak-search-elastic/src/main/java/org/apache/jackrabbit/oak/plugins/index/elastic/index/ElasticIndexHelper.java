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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides utility functions around Elasticsearch indexing
 */
class ElasticIndexHelper {

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

    private static XContentBuilder loadSettings(ElasticIndexDefinition indexDefinition) throws IOException {
        final XContentBuilder settingsBuilder = XContentFactory.jsonBuilder();
        settingsBuilder.startObject();
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
                        settingsBuilder.field("preserve_original", indexDefinition.indexOriginalTerms());
                    }
                    settingsBuilder.endObject();
                    if (indexDefinition.isSpellcheckEnabled()) {
                        settingsBuilder.startObject("shingle")
                                .field("type", "shingle")
                                .field("min_shingle_size", 2)
                                .field("max_shingle_size", 3)
                                .endObject();
                    }
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
                    if (indexDefinition.isSpellcheckEnabled()) {
                        settingsBuilder.startObject("trigram")
                                .field("type", "custom")
                                .field("tokenizer", "standard")
                                .array("filter", "lowercase", "shingle")
                                .endObject();
                    }
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
        // when the specific features will be implemented
//                mappingBuilder.startObject(FieldNames.SUGGEST)
//                        .field("type", "completion")
//                        .endObject();
//                mappingBuilder.startObject(FieldNames.NOT_NULL_PROPS)
//                        .field("type", "keyword")
//                        .endObject();
//                mappingBuilder.startObject(FieldNames.NULL_PROPS)
//                        .field("type", "keyword")
//                        .endObject();
    }

    private static void mapIndexRules(ElasticIndexDefinition indexDefinition, XContentBuilder mappingBuilder) throws IOException {
        checkIndexRules(indexDefinition);
        boolean useInSuggest = false;
        for (Map.Entry<String, List<PropertyDefinition>> entry : indexDefinition.getPropertiesByName().entrySet()) {
            final String name = entry.getKey();
            final List<PropertyDefinition> propertyDefinitions = entry.getValue();

            Type<?> type = null;
            boolean useInSpellCheck = false;
            for (PropertyDefinition pd : propertyDefinitions) {
                type = Type.fromTag(pd.getType(), false);
                if (pd.useInSpellcheck) {
                    useInSpellCheck = true;
                }
                if (pd.useInSuggest) {
                    useInSuggest = true;
                }
            }

            mappingBuilder.startObject(name);
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
                    if (indexDefinition.isAnalyzed(propertyDefinitions)) {
                        mappingBuilder.field("type", "text");
                        mappingBuilder.field("analyzer", "oak_analyzer");
                        // always add keyword for sorting / faceting as sub-field
                        mappingBuilder.startObject("fields");
                        {
                            mappingBuilder.startObject("keyword")
                                    .field("type", "keyword")
                                    .field("ignore_above", 256)
                                    .endObject();
                            if (useInSpellCheck) {
                                mappingBuilder.startObject("trigram")
                                        .field("type", "text").field("analyzer", "trigram")
                                        .endObject();
                            }
                        }
                        mappingBuilder.endObject();
                    } else {
                        // always add keyword for sorting / faceting
                        mappingBuilder
                                .field("type", "keyword")
                                .field("ignore_above", 256);
                    }
                }
            }
            mappingBuilder.endObject();
        }

        if (useInSuggest) {
            mappingBuilder.startObject(FieldNames.SUGGEST);
            {
                mappingBuilder.field("type", "nested");
                mappingBuilder.startObject("properties");
                {
                    mappingBuilder.startObject("suggestion")
                            .field("type", "text")
                            .field("analyzer", "oak_analyzer")
                            .endObject();
                }
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
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
