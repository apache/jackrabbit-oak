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
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;

public class ElasticsearchIndexStatistics implements IndexStatistics {
    private final ElasticsearchIndexDescriptor elasticsearchIndexDescriptor;

    ElasticsearchIndexStatistics(ElasticsearchIndexDescriptor elasticsearchIndexDescriptor) {
        this.elasticsearchIndexDescriptor = elasticsearchIndexDescriptor;
    }

    @Override
    public int numDocs() {
        CountRequest countRequest = new CountRequest();
        countRequest.query(QueryBuilders.matchAllQuery());
        try {
            CountResponse count = elasticsearchIndexDescriptor.getClient().count(countRequest, RequestOptions.DEFAULT);
            return (int) count.getCount();
        } catch (IOException e) {
            // ignore failure
            return 100000;
        }
    }

    @Override
    public int getDocCountFor(String key) {
        CountRequest countRequest = new CountRequest();
        countRequest.query(QueryBuilders.existsQuery(key));
        try {
            CountResponse count = elasticsearchIndexDescriptor.getClient().count(countRequest, RequestOptions.DEFAULT);
            return (int) count.getCount();
        } catch (IOException e) {
            // ignore failure
            return 1000;
        }
    }
}
