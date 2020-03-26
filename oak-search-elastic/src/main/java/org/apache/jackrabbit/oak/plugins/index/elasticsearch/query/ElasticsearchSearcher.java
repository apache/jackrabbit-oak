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

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDescriptor;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class ElasticsearchSearcher {
    private final ElasticsearchIndexDescriptor indexDescriptor;

    ElasticsearchSearcher(@NotNull ElasticsearchIndexNode indexNode) {
        indexDescriptor = indexNode.getIndexDescriptor();
    }

    public SearchResponse search(QueryBuilder query, int batchSize) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(query)
                .fetchSource(false)
                .storedField(FieldNames.PATH)
                .size(batchSize);

        SearchRequest request = new SearchRequest(indexDescriptor.getIndexName())
                .source(searchSourceBuilder);

        return indexDescriptor.getClient().search(request, RequestOptions.DEFAULT);
    }
}
