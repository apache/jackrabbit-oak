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

import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.facets.ElasticAggregationData;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.facets.ElasticFacetHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticAggregationBuilderUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticConstants;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class ElasticQueryProcess implements ElasticProcess {

    private static final Logger LOG = LoggerFactory
            .getLogger(ElasticQueryProcess.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(ElasticQueryProcess.class.getName() + ".perf"));

    private final QueryBuilder queryBuilder;
    private int nextBatchSize = ElasticConstants.ELASTIC_QUERY_BATCH_SIZE;
    private final ElasticResultRowIterator.ElasticRowIteratorState elasticRowIteratorState;
    private final ElasticResultRowIterator.ElasticFacetProvider elasticsearchFacetProvider;


    ElasticQueryProcess(QueryBuilder queryBuilder, ElasticResultRowIterator.ElasticRowIteratorState elasticRowIteratorState,
                        ElasticResultRowIterator.ElasticFacetProvider elasticFacetProvider) {
        this.queryBuilder = queryBuilder;
        this.elasticRowIteratorState = elasticRowIteratorState;
        this.elasticsearchFacetProvider = elasticFacetProvider;
    }

    @Override
    public SearchHit process() throws IOException {

        ElasticIndexDefinition indexDefinition = elasticRowIteratorState.getIndexNode().getDefinition();
        ElasticSearcher searcher = new ElasticSearcher(elasticRowIteratorState.getIndexNode());
        int numberOfFacets = elasticRowIteratorState.getIndexNode().getDefinition().getNumberOfTopFacets();
        List<TermsAggregationBuilder> aggregationBuilders = ElasticAggregationBuilderUtil
                .getAggregators(elasticRowIteratorState.getPlan(), indexDefinition, numberOfFacets);

        ElasticSearcherModel elasticSearcherModel = new ElasticSearcherModel.ElasticSearcherModelBuilder()
                .withQuery(this.queryBuilder)
                .withBatchSize(nextBatchSize)
                .withAggregation(aggregationBuilders)
                .build();

        // TODO: custom scoring

        SearchResponse docs;
        SearchHit lastDocToRecord = null;
        long start = PERF_LOGGER.start();

        while (true) {
            LOG.debug("loading {} entries for query {}", nextBatchSize, this.queryBuilder);
            docs = searcher.search(elasticSearcherModel);
            long totalHits = docs.getHits().getTotalHits().value;
            ElasticAggregationData elasticAggregationData =
                    new ElasticAggregationData(numberOfFacets, totalHits, docs.getAggregations());

            SearchHit[] searchHits = docs.getHits().getHits();
            PERF_LOGGER.end(start, -1, "{} ...", searchHits.length);

            elasticRowIteratorState.updateEstimator(docs.getHits().getTotalHits().value);
            if (searchHits.length < nextBatchSize) {
                elasticRowIteratorState.setLastDoc(true);
            }
            nextBatchSize = (int) Math.min(nextBatchSize * 2L, ElasticConstants.ELASTIC_QUERY_MAX_BATCH_SIZE);
            if (aggregationBuilders.size() > 0 && !elasticsearchFacetProvider.isInitiliazed()) {
                elasticsearchFacetProvider.initialize(ElasticFacetHelper.getAggregates(searcher, queryBuilder,
                        elasticRowIteratorState.getIndexNode(), elasticRowIteratorState.getPlan(), elasticAggregationData));
            }

            // TODO: excerpt

            // TODO: explanation

            // TODO: sim search

            for (SearchHit doc : searchHits) {
                // TODO : excerpts

                FulltextIndex.FulltextResultRow row = convertToRow(doc, elasticsearchFacetProvider);
                if (row != null) {
                    elasticRowIteratorState.addResultRow(row);
                }
                lastDocToRecord = doc;
            }

            if (elasticRowIteratorState.isEmpty() && searchHits.length > 0) {
                //queue is still empty but more results can be fetched
                //from Lucene so still continue
                elasticRowIteratorState.lastIteratedDoc = lastDocToRecord;
            } else {
                break;
            }
        }
        return lastDocToRecord;
    }

    @Override
    public String getQuery() {
        return Strings.toString((queryBuilder));
    }

    private FulltextIndex.FulltextResultRow convertToRow(SearchHit hit, ElasticResultRowIterator.ElasticFacetProvider elasticsearchFacetProvider) {
        final Map<String, Object> sourceMap = hit.getSourceAsMap();
        String path = (String) sourceMap.get(FieldNames.PATH);
        if (path != null) {
            if ("".equals(path)) {
                path = "/";
            }
            String originalPath = path;
            path = elasticRowIteratorState.getPlanResult().transformPath(path);

            if (path == null) {
                LOG.trace("Ignoring path {} : Transformation returned null", originalPath);
                return null;
            }

            boolean shouldIncludeForHierarchy = elasticRowIteratorState.getRowInclusionPredicate()
                    .shouldInclude(path, elasticRowIteratorState.getPlan());
            LOG.trace("Matched path {}; shouldIncludeForHierarchy: {}", path, shouldIncludeForHierarchy);
            return shouldIncludeForHierarchy ? new FulltextIndex.FulltextResultRow(path, hit.getScore(), null,
                    elasticsearchFacetProvider, null)
                    : null;
        }
        return null;
    }

}
