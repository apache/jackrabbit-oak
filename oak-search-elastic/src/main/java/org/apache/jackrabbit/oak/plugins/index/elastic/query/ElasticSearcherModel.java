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

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder;

import java.util.LinkedList;
import java.util.List;

public class ElasticSearcherModel {

    private final QueryBuilder queryBuilder;
    private final List<AggregationBuilder> aggregationBuilders;
    private final int batchSize;
    private final boolean fetchSource;
    private final String storedField;
    private final int from;
    private SuggestBuilder searchSourceBuilder;

    private ElasticSearcherModel(QueryBuilder queryBuilder, List<AggregationBuilder> aggregationBuilders,
                                 int batchSize, boolean fetchSource, String storedField, int from, SuggestBuilder searchSourceBuilder) {
        this.queryBuilder = queryBuilder;
        this.aggregationBuilders = aggregationBuilders;
        this.batchSize = batchSize;
        this.fetchSource = fetchSource;
        this.storedField = storedField;
        this.from = from;
        this.searchSourceBuilder = searchSourceBuilder;
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

    public SuggestBuilder getSuggestBuilder() {
        return searchSourceBuilder;
    }

    public static class ElasticSearcherModelBuilder {
        private QueryBuilder queryBuilder;
        private final List<AggregationBuilder> aggregationBuilders = new LinkedList<>();
        private int batchSize;
        private boolean fetchSource = false;
        private String storedField = FieldNames.PATH;
        private SuggestBuilder searchSourceBuilder;
        private int from;

        public ElasticSearcherModelBuilder withQuery(QueryBuilder query) {
            this.queryBuilder = query;
            return this;
        }

        public ElasticSearcherModelBuilder withAggregation(List<TermsAggregationBuilder> aggregationBuilders) {
            this.aggregationBuilders.addAll(aggregationBuilders);
            return this;
        }

        public ElasticSearcherModelBuilder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public ElasticSearcherModelBuilder withFrom(int from) {
            this.from = from;
            return this;
        }

        public ElasticSearcherModel build() {
            return new ElasticSearcherModel(queryBuilder, aggregationBuilders, batchSize, fetchSource, storedField, from, searchSourceBuilder);
        }

        public ElasticSearcherModelBuilder withSpellCheck(SuggestBuilder searchSourceBuilder) {
            this.searchSourceBuilder = searchSourceBuilder;
            return this;
        }
    }
}
