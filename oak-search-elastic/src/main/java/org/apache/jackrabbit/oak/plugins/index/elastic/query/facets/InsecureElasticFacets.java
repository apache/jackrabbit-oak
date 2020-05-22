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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.facets;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticSearcher;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticSearcherModel;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticAggregationBuilderUtil;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsecureElasticFacets implements ElasticFacets {
    private static final Logger LOG = LoggerFactory.getLogger(InsecureElasticFacets.class);

    private ElasticSearcher searcher;
    private QueryBuilder query;
    private QueryIndex.IndexPlan plan;
    private ElasticAggregationData elasticAggregationData;

    public InsecureElasticFacets(ElasticSearcher searcher, QueryBuilder query,
                                 QueryIndex.IndexPlan plan, ElasticAggregationData elasticAggregationData) {
        this.searcher = searcher;
        this.query = query;
        this.plan = plan;
        this.elasticAggregationData = elasticAggregationData;
    }

    @Override
    public Map<String, List<FulltextIndex.Facet>> getFacets(ElasticIndexDefinition indexDefinition,
                                                            int numberOfFacets) throws IOException {
        if (elasticAggregationData != null && numberOfFacets <= elasticAggregationData.getNumberOfFacets()) {
            return changeToFacetList(elasticAggregationData.getAggregations().getAsMap(), numberOfFacets);
        }
        LOG.warn("Facet data is being retrieved by again calling Elasticsearch");
        List<TermsAggregationBuilder> aggregationBuilders = ElasticAggregationBuilderUtil.getAggregators(plan, indexDefinition, numberOfFacets);
        ElasticSearcherModel elasticSearcherModel = new ElasticSearcherModel.ElasticSearcherModelBuilder()
                .withQuery(query)
                .withAggregation(aggregationBuilders)
                .build();
        Map<String, Aggregation> facetResult = searcher.search(elasticSearcherModel).getAggregations().getAsMap();
        return changeToFacetList(facetResult, numberOfFacets);
    }

    Map<String, List<FulltextIndex.Facet>> changeToFacetList(Map<String, Aggregation> docs, int topFacetCount) {
        Map<String, List<FulltextIndex.Facet>> facetMap = new HashMap<>();
        for (String facet : docs.keySet()) {
            Terms terms = (Terms) docs.get(facet);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            final List<FulltextIndex.Facet> facetList = new ArrayList<>();
            for (Terms.Bucket bucket : buckets) {
                String facetKey = bucket.getKeyAsString();
                long facetCount = bucket.getDocCount();
                facetList.add(new FulltextIndex.Facet(facetKey, (int) facetCount));
            }

            if ((facetList.size() > topFacetCount)) {
                facetMap.put(facet, facetList.subList(0, topFacetCount));
            } else {
                facetMap.put(facet, facetList);
            }
        }
        return facetMap;
    }

    @Override
    public ElasticSearcher getSearcher() {
        return searcher;
    }

    @Override
    public QueryBuilder getQuery() {
        return query;
    }

    @Override
    public QueryIndex.IndexPlan getPlan() {
        return plan;
    }

    @Override
    public ElasticAggregationData getElasticAggregationData() {
        return elasticAggregationData;
    }
}
