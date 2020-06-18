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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.elastic.util.SearchSourceBuilderUtil;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

public class ElasticSearcher {
    private final ElasticIndexNode indexNode;

    ElasticSearcher(@NotNull ElasticIndexNode indexNode) {
        this.indexNode = indexNode;
    }

    public SearchResponse search(ElasticSearcherModel elasticSearcherModel) throws IOException {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilderUtil.createSearchSourceBuilder(elasticSearcherModel);

        SearchRequest request = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                .source(searchSourceBuilder);

        return indexNode.getConnection().getClient().search(request, RequestOptions.DEFAULT);
    }

    public MultiSearchResponse search(List<ElasticSearcherModel> elasticSearcherModels) throws IOException {
        MultiSearchRequest ms = new MultiSearchRequest();
        for (ElasticSearcherModel elasticSearcherModel: elasticSearcherModels) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilderUtil.createSearchSourceBuilder(elasticSearcherModel);
            SearchRequest request = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                    .source(searchSourceBuilder);
            ms.add(request);
        }
        return indexNode.getConnection().getClient().msearch(ms, RequestOptions.DEFAULT);
    }

}
