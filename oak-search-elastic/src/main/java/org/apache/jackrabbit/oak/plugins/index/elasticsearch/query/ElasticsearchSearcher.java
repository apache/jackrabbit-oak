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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.query;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexCoordinateFactory;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexCoordinate;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class ElasticsearchSearcher {
    private final ElasticsearchIndexCoordinate esIndexCoord;
    private final RestHighLevelClient client;

    ElasticsearchSearcher(@NotNull ElasticsearchIndexCoordinateFactory esIndexCoordFactory,
                          @NotNull ElasticsearchIndexNode indexNode) {
        ElasticsearchIndexDefinition defn = indexNode.getDefinition();
        esIndexCoord = esIndexCoordFactory.getElasticsearchIndexCoordinate(defn);
        client = esIndexCoord.getClient();
    }

    public SearchResponse search(QueryBuilder query, int batchSize) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(query)
                .fetchSource(false)
                .storedField(FieldNames.PATH)
                .size(batchSize)
                ;

        SearchRequest request = new SearchRequest(esIndexCoord.getEsIndexName())
                .source(searchSourceBuilder);

        return client.search(request, RequestOptions.DEFAULT);
    }
}
