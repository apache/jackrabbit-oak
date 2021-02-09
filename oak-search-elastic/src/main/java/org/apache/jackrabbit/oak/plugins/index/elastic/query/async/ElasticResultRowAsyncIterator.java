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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.util.LMSEstimator;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Class to iterate over Elastic results of a given {@link IndexPlan}.
 * The results are produced asynchronously into an internal unbounded {@link BlockingQueue}. To avoid too many calls to
 * Elastic the results are loaded in chunks (using search_after strategy) and loaded only when needed.
 */
public class ElasticResultRowAsyncIterator implements Iterator<FulltextResultRow>, ElasticResponseListener.SearchHitListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResultRowAsyncIterator.class);
    // this is an internal special message to notify the consumer the result set has been completely returned
    private static final FulltextResultRow POISON_PILL =
            new FulltextResultRow("___OAK_POISON_PILL___", 0d, Collections.emptyMap(), null, null);

    private final BlockingQueue<FulltextResultRow> queue = new LinkedBlockingQueue<>();

    private final ElasticIndexNode indexNode;
    private final IndexPlan indexPlan;
    private final Predicate<String> rowInclusionPredicate;
    private final LMSEstimator estimator;

    private final ElasticQueryScanner elasticQueryScanner;
    private final ElasticRequestHandler elasticRequestHandler;
    private final ElasticResponseHandler elasticResponseHandler;
    private final ElasticFacetProvider elasticFacetProvider;

    private FulltextResultRow nextRow;

    public ElasticResultRowAsyncIterator(@NotNull ElasticIndexNode indexNode,
                                         @NotNull ElasticRequestHandler elasticRequestHandler,
                                         @NotNull ElasticResponseHandler elasticResponseHandler,
                                         @NotNull QueryIndex.IndexPlan indexPlan,
                                         Predicate<String> rowInclusionPredicate,
                                         LMSEstimator estimator) {
        this.indexNode = indexNode;
        this.elasticRequestHandler = elasticRequestHandler;
        this.elasticResponseHandler = elasticResponseHandler;
        this.indexPlan = indexPlan;
        this.rowInclusionPredicate = rowInclusionPredicate;
        this.estimator = estimator;

        this.elasticFacetProvider = elasticRequestHandler.getAsyncFacetProvider(elasticResponseHandler);
        this.elasticQueryScanner = initScanner();
    }

    @Override
    public boolean hasNext() {
        // if nextRow is not null it means the caller invoked hasNext() before without calling next()
        if (nextRow == null) {
            if (queue.isEmpty()) {
                // this triggers, when needed, the scan of the next results chunk
                elasticQueryScanner.scan();
            }
            try {
                nextRow = queue.take();
            } catch (InterruptedException e) {
                throw new IllegalStateException("Error reading next result from Elastic", e);
            }
        }
        return !POISON_PILL.path.equals(nextRow.path);
    }

    @Override
    public FulltextResultRow next() {
        if (nextRow == null) { // next is called without hasNext
            if (!hasNext()) {
                return null;
            }
        }
        FulltextResultRow row = null;
        if (nextRow != null && !POISON_PILL.path.equals(nextRow.path)) {
            row = nextRow;
            nextRow = null;
        }
        return row;
    }

    @Override
    public void on(ElasticResponseHandler.SearchResponseHit searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null) {
            if (rowInclusionPredicate != null && !rowInclusionPredicate.test(path)) {
                LOG.trace("Path {} not included because of hierarchy inclusion rules", path);
                return;
            }
            try {
                queue.put(new FulltextResultRow(path, searchHit.score, null, elasticFacetProvider, null));
            } catch (InterruptedException e) {
                throw new IllegalStateException("Error producing results into the iterator queue", e);
            }
        }
    }

    @Override
    public void endData() {
        try {
            queue.put(POISON_PILL);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Error inserting poison pill into the iterator queue", e);
        }
    }

    private ElasticQueryScanner initScanner() {
        List<ElasticResponseListener> listeners = new ArrayList<>();
        // TODO: we could avoid to register this listener when the client is interested in facets only. It would save space and time
        listeners.add(this);
        if (elasticFacetProvider != null) {
            listeners.add(elasticFacetProvider);
        }

        return new ElasticQueryScanner(listeners);
    }

    /**
     * Scans Elastic results asynchronously and notify listeners.
     */
    class ElasticQueryScanner implements ResponseListener {

        private static final int SMALL_RESULT_SET_SIZE = 10;

        private final Set<ElasticResponseListener> allListeners = new HashSet<>();
        private final List<SearchHitListener> searchHitListeners = new ArrayList<>();
        private final List<AggregationListener> aggregationListeners = new ArrayList<>();

        private final QueryBuilder query;
        private final List<FieldSortBuilder> sorts;
        private final String[] sourceFields;

        // concurrent data structures to coordinate chunks loading
        private final AtomicBoolean anyDataLeft = new AtomicBoolean(false);

        private int scannedRows;
        private int requests;
        private boolean fullScan;
        private long searchStartTime;

        // reference to the last document sort values for search_after queries
        private Object[] lastHitSortValues;

        // Semaphore to guarantee only one in-flight request to Elastic
        private final Semaphore semaphore = new Semaphore(1);

        private final ElasticMetricHandler elasticMetricHandler = indexNode.getElasticMetricHandler();

        ElasticQueryScanner(List<ElasticResponseListener> listeners) {
            this.query = elasticRequestHandler.baseQuery();
            this.sorts = elasticRequestHandler.baseSorts();

            Set<String> sourceFieldsSet = new HashSet<>();
            AtomicBoolean needsAggregations = new AtomicBoolean(false);
            Consumer<ElasticResponseListener> register = (listener) -> {
                allListeners.add(listener);
                sourceFieldsSet.addAll(listener.sourceFields());
                if (listener instanceof SearchHitListener) {
                    SearchHitListener searchHitListener = (SearchHitListener) listener;
                    searchHitListeners.add(searchHitListener);
                    if (searchHitListener.isFullScan()) {
                        fullScan = true;
                    }
                }
                if (listener instanceof AggregationListener) {
                    aggregationListeners.add((AggregationListener) listener);
                    needsAggregations.set(true);
                }
            };

            listeners.forEach(register);
            this.sourceFields = sourceFieldsSet.toArray(new String[0]);

            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                    .query(query)
                    // use a smaller size when the query contains aggregations. This improves performance
                    // when the client is only interested in insecure facets
                    .size(needsAggregations.get() ? Math.min(SMALL_RESULT_SET_SIZE, getFetchSize(requests)) : getFetchSize(requests))
                    .fetchSource(sourceFields, null);

            this.sorts.forEach(searchSourceBuilder::sort);

            if (needsAggregations.get()) {
                elasticRequestHandler.aggregations().forEach(searchSourceBuilder::aggregation);
            }

            LOG.trace("Kicking initial search for query {}", searchSourceBuilder);
            semaphore.tryAcquire();

            searchStartTime = System.currentTimeMillis();
            requests++;

            Request request = elasticRequestHandler.createLowLevelRequest(searchSourceBuilder,
                    indexNode.getDefinition().getRemoteIndexAlias());
            indexNode.getConnection().getClient().getLowLevelClient().performRequestAsync(request, this);
            elasticMetricHandler.markQuery(true);
        }

        /**
         * Handle the response action notifying the registered listeners. Depending on the listeners' configuration
         * it could keep loading chunks or wait for a {@code #scan} call to resume scanning.
         *
         * Some code in this method relies on structure that are not thread safe. We need to make sure
         * these data structures are modified before releasing the semaphore.
         */
        @Override
        public void onSuccess(Response response) {
            long searchTotalTime = System.currentTimeMillis() - searchStartTime;
            ElasticResponseHandler.SearchResponse searchResponse;
            try {
                searchResponse = elasticResponseHandler.parse(response);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to parse response", e);
            }

            ElasticResponseHandler.SearchResponseHit[] searchHits = searchResponse.hits.hits;
            int hitsSize = searchHits != null ? searchHits.length : 0;
            elasticMetricHandler.measureQuery(hitsSize, searchResponse.took, searchTotalTime, searchResponse.timedOut);
            if (hitsSize > 0) {
                long totalHits = searchResponse.hits.total.value;
                LOG.debug("Processing search response that took {} to read {}/{} docs", searchResponse.took, hitsSize, totalHits);
                lastHitSortValues = searchHits[hitsSize - 1].sort;
                scannedRows += hitsSize;
                anyDataLeft.set(totalHits > scannedRows);
                estimator.update(indexPlan.getFilter(), totalHits);

                // now that we got the last hit we can release the semaphore to potentially unlock other requests
                semaphore.release();

                if (requests == 1) {
                    for (SearchHitListener l : searchHitListeners) {
                        l.startData(totalHits);
                    }

                    if (!aggregationListeners.isEmpty()) {
                        LOG.trace("Emitting aggregations {}", searchResponse.aggregations);
                        for (AggregationListener l : aggregationListeners) {
                            l.on(searchResponse.aggregations);
                        }
                    }
                }

                LOG.trace("Emitting {} search hits, for a total of {} scanned results", searchHits.length, scannedRows);
                for (ElasticResponseHandler.SearchResponseHit hit : searchHits) {
                    for (SearchHitListener l : searchHitListeners) {
                        l.on(hit);
                    }
                }

                if (!anyDataLeft.get()) {
                    LOG.trace("No data left: closing scanner, notifying listeners");
                    close();
                } else if (fullScan) {
                    scan();
                }
            } else {
                LOG.trace("No results: closing scanner, notifying listeners");
                close();
            }
        }

        @Override
        public void onFailure(Exception e) {
            elasticMetricHandler.measureFailedQuery(System.currentTimeMillis() - searchStartTime);
            LOG.error("Error retrieving data from Elastic: closing scanner, notifying listeners", e);
            // closing scanner immediately after a failure avoiding them to hang (potentially) forever
            close();
        }

        /**
         * Triggers a scan of a new chunk of the result set, if needed.
         */
        private void scan() {
            if (semaphore.tryAcquire() && anyDataLeft.get()) {
                final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                        .query(query)
                        .size(getFetchSize(requests++))
                        .fetchSource(sourceFields, null)
                        .searchAfter(lastHitSortValues);

                this.sorts.forEach(searchSourceBuilder::sort);

                final SearchRequest searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                        .source(searchSourceBuilder);
                LOG.trace("Kicking new search after query {}", searchRequest.source());

                searchStartTime = System.currentTimeMillis();
                Request request = elasticRequestHandler.createLowLevelRequest(searchSourceBuilder,
                        indexNode.getDefinition().getRemoteIndexAlias());
                indexNode.getConnection().getClient().getLowLevelClient().performRequestAsync(request, this);
                elasticMetricHandler.markQuery(false);
            } else {
                LOG.trace("Scanner is closing or still processing data from the previous scan");
            }
        }

        /* picks the size in the fetch array at index=requests or the last if out of bound */
        private int getFetchSize(int requestId) {
            int[] queryFetchSizes = indexNode.getDefinition().queryFetchSizes;
            return queryFetchSizes.length > requestId ?
                    queryFetchSizes[requestId] : queryFetchSizes[queryFetchSizes.length -1];
        }

        // close all listeners
        private void close() {
            semaphore.release();
            for (ElasticResponseListener l : allListeners) {
                l.endData();
            }
        }
    }
}
