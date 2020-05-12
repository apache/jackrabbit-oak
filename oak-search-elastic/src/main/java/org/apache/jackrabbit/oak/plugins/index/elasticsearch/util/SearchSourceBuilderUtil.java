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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.util;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcherModel;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class SearchSourceBuilderUtil {

    public static SearchSourceBuilder createSearchSourceBuilder(ElasticsearchSearcherModel elasticsearchSearcherModel) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(elasticsearchSearcherModel.getQueryBuilder())
                .fetchSource(elasticsearchSearcherModel.fetchSource())
                .storedField(elasticsearchSearcherModel.getStoredField())
                .size(elasticsearchSearcherModel.getBatchSize())
                .from(elasticsearchSearcherModel.getFrom());

        for (AggregationBuilder aggregationBuilder : elasticsearchSearcherModel.getAggregationBuilders()) {
            searchSourceBuilder.aggregation(aggregationBuilder);
        }
        return searchSourceBuilder;
    }
}
