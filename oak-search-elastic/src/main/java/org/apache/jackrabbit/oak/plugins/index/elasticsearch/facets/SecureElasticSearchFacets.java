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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch.facets;

import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcher;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchSearcherModel;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchAggregationBuilderUtil;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchConstants;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class SecureElasticSearchFacets extends InsecureElasticSearchFacets {

    public SecureElasticSearchFacets(ElasticsearchSearcher searcher, QueryBuilder query,
                                     QueryIndex.IndexPlan plan) {
        super(searcher, query, plan, null);
    }

    /*
    We are not using elasticSearch's aggregation as we have to fetch each document to validate access rights
    for docs.
     */
    @Override
    public Map<String, List<FulltextIndex.Facet>> getElasticSearchFacets(int numberOfFacets) throws IOException {
        Map<String, Map<String, Long>> secureFacetCount = new HashMap<>();
        Filter filter = getPlan().getFilter();
        boolean doFetch = true;
        for (int from = 0; doFetch; from += ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE) {
            ElasticsearchSearcherModel elasticsearchSearcherModel = new ElasticsearchSearcherModel.ElasticsearchSearcherModelBuilder()
                    .withQuery(getQuery())
                    .withBatchSize(ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE)
                    .withFrom(from)
                    .build();
            SearchResponse docs = getSearcher().search(elasticsearchSearcherModel);
            SearchHit[] searchHits = docs.getHits().getHits();
            long totalResults = docs.getHits().getTotalHits().value;
            if (totalResults <= from + ElasticsearchConstants.ELASTICSEARCH_QUERY_BATCH_SIZE || searchHits.length == 0) {
                doFetch = false;
            }

            List<String> accessibleDocs = ElasticFacetHelper.getAccessibleDocIds(searchHits, filter);
            if (accessibleDocs.isEmpty()) continue;
            QueryBuilder queryWithAccessibleDocIds = QueryBuilders.termsQuery("_id", accessibleDocs);
            Map<String, Aggregation> accessibleDocsAggregation = getAggregationForDocIds(queryWithAccessibleDocIds, accessibleDocs.size());
            collateAggregations(secureFacetCount, accessibleDocsAggregation);
        }

        Map<String, List<FulltextIndex.Facet>> facetResult = new HashMap<>();
        for (String facet : secureFacetCount.keySet()) {
            PriorityQueue<ElasticSearchFacet> pq = new PriorityQueue<>(numberOfFacets, (o1, o2) -> o2.getCount().compareTo(o1.getCount()));
            Map<String, Long> facetLabelMap = secureFacetCount.get(facet);
            for (String label : facetLabelMap.keySet()) {
                pq.add(new ElasticSearchFacet(label, facetLabelMap.get(label)));
            }
            List<FulltextIndex.Facet> fc = new LinkedList<>();
            pq.forEach(elasticSearchFacet -> fc.add(elasticSearchFacet.convertToFacet()));
            facetResult.put(facet, fc);
        }
        return facetResult;
    }

    private void collateAggregations(Map<String, Map<String, Long>> secureFacetCount, Map<String, Aggregation> docs) {
        for (String facet : docs.keySet()) {
            Terms terms = (Terms) docs.get(facet);
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String label = bucket.getKeyAsString();
                Long count = bucket.getDocCount();
                collateFacetData(secureFacetCount, facet, label, count);
            }
        }
    }

    private void collateFacetData(Map<String, Map<String, Long>> globalData, String facet, String label, Long count) {
        if (globalData.get(facet) == null) {
            Map<String, Long> labelValueMap = new HashMap<>();
            labelValueMap.put(label, count);
            globalData.put(facet, labelValueMap);
        } else {
            if (globalData.get(facet).get(label) == null) {
                globalData.get(facet).put(label, count);
            } else {
                Long existingCount = globalData.get(facet).get(label);
                globalData.get(facet).put(label, existingCount + count);
            }
        }
    }

    private Map<String, Aggregation> getAggregationForDocIds(QueryBuilder queryWithAccessibleDocIds, int facetCount) throws IOException {
        List<TermsAggregationBuilder> aggregationBuilders = ElasticsearchAggregationBuilderUtil.getAggregators(getPlan(), facetCount);
        ElasticsearchSearcherModel idBasedelasticsearchSearcherModelWithAggregation = new ElasticsearchSearcherModel.ElasticsearchSearcherModelBuilder()
                .withQuery(queryWithAccessibleDocIds)
                .withAggregation(aggregationBuilders)
                .build();

        SearchResponse facetDocs = getSearcher().search(idBasedelasticsearchSearcherModelWithAggregation);
        Map<String, Aggregation> aggregationMap = facetDocs.getAggregations().asMap();
        return aggregationMap;
    }
}
