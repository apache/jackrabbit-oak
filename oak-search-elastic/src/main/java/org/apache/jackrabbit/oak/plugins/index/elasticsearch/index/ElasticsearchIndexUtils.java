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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.index;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Provides utility functions around Elasticsearch indexing
 */
class ElasticsearchIndexUtils {

    public static CreateIndexRequest createIndexRequest(ElasticsearchIndexDefinition indexDefinition) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexDefinition.getRemoteIndexName());

        // provision settings
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pathhierarchy-tokenizer.html
        request.settings(Settings.builder()
                .put("analysis.analyzer.ancestor_analyzer.type", "custom")
                .put("analysis.analyzer.ancestor_analyzer.tokenizer", "path_hierarchy"));

        // provision mappings
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder();
        mappingBuilder.startObject();
        {
            mappingBuilder.startObject("properties");
            {
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
                        .endObject();
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
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject();
        request.mapping(mappingBuilder);

        return request;
    }
}
