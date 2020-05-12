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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.LinkedList;
import java.util.List;

public class ElasticsearchSearcherModel {

    private QueryBuilder queryBuilder;
    private List<AggregationBuilder> aggregationBuilders = new LinkedList<>();
    private int batchSize;
    private boolean fetchSource;
    private String storedField = FieldNames.PATH;
    private int from;

    private ElasticsearchSearcherModel(QueryBuilder queryBuilder, List<AggregationBuilder> aggregationBuilders,
                                       int batchSize, boolean fetchSource, String storedField, int from) {
        this.queryBuilder = queryBuilder;
        this.aggregationBuilders = aggregationBuilders;
        this.batchSize = batchSize;
        this.fetchSource = fetchSource;
        this.storedField = storedField;
        this.from = from;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getFrom() {
        return from;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public List<AggregationBuilder> getAggregationBuilders() {
        return aggregationBuilders;
    }

    public boolean fetchSource() {
        return fetchSource;
    }

    public String getStoredField() {
        return storedField;
    }

    public static class ElasticsearchSearcherModelBuilder {
        private QueryBuilder queryBuilder;
        private List<AggregationBuilder> aggregationBuilders = new LinkedList<>();
        private int batchSize;
        private boolean fetchSource = false;
        private String storedField = FieldNames.PATH;
        private int from;

        public ElasticsearchSearcherModelBuilder withQuery(QueryBuilder query) {
            this.queryBuilder = query;
            return this;
        }

        public ElasticsearchSearcherModelBuilder withAggregation(List<TermsAggregationBuilder> aggregationBuilders) {
            this.aggregationBuilders.addAll(aggregationBuilders);
            return this;
        }

        public ElasticsearchSearcherModelBuilder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ElasticsearchSearcherModelBuilder withFrom(int from) {
            this.from = from;
            return this;
        }

        public ElasticsearchSearcherModel build() {
            return new ElasticsearchSearcherModel(queryBuilder, aggregationBuilders, batchSize, fetchSource, storedField, from);
        }
    }
}
