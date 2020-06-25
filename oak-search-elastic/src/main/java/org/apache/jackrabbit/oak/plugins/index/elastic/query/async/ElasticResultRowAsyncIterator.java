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

import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.util.LMSEstimator;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (queue.isEmpty()) {
            // this triggers, when needed, the scan of the next results chunk
            elasticQueryScanner.scan();
        }
        try {
            nextRow = queue.take();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Error reading next result from Elastic", e);
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
        return nextRow != null && POISON_PILL.path.equals(nextRow.path) ? null : nextRow;
    }

    @Override
    public void on(SearchHit searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null) {
            if (rowInclusionPredicate != null && !rowInclusionPredicate.test(path)) {
                LOG.trace("Path {} not included because of hierarchy inclusion rules", path);
                return;
            }
            try {
                queue.put(new FulltextResultRow(path, searchHit.getScore(), null, elasticFacetProvider, null));
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

        return new ElasticQueryScanner(elasticRequestHandler, listeners);
    }

    /**
     * Scans Elastic results asynchronously and notify listeners.
     */
    class ElasticQueryScanner implements ActionListener<SearchResponse> {

        private static final int SMALL_RESULT_SET_SIZE = 10;
        private static final int MEDIUM_RESULT_SET_SIZE = 100;
        private static final int LARGE_RESULT_SET_SIZE = 1000;

        private final Set<ElasticResponseListener> allListeners = new HashSet<>();
        private final List<SearchHitListener> searchHitListeners = new ArrayList<>();
        private final List<AggregationListener> aggregationListeners = new ArrayList<>();

        private final QueryBuilder query;
        private final String[] sourceFields;

        // concurrent data structures to coordinate chunks loading
        private final AtomicBoolean anyDataLeft = new AtomicBoolean(false);

        private int scannedRows = 0;
        private boolean firstRequest = true;
        private boolean fullScan = false;

        // reference to the last document sort values for search_after queries
        private Object[] lastHitSortValues;

        // Semaphore to guarantee only one in-flight request to Elastic
        private final Semaphore semaphore = new Semaphore(1);

        ElasticQueryScanner(ElasticRequestHandler requestHandler,
                            List<ElasticResponseListener> listeners) {
            this.query = requestHandler.baseQuery();

            final Set<String> sourceFieldsSet = new HashSet<>();
            final AtomicBoolean needsAggregations = new AtomicBoolean(false);
            final Consumer<ElasticResponseListener> register = (listener) -> {
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

            final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
                    .query(query)
                    // use a smaller size when the client asks for facets. This improves performance
                    // when the client is only interested in insecure facets
                    .size(needsAggregations.get() ? SMALL_RESULT_SET_SIZE : MEDIUM_RESULT_SET_SIZE)
                    .fetchSource(sourceFields, null)
                    // TODO: this needs to be moved in the requestHandler when sorting will be properly supported
                    .sort(SortBuilders.fieldSort("_score").order(SortOrder.DESC))
                    .sort(SortBuilders.fieldSort(FieldNames.PATH).order(SortOrder.ASC)); // tie-breaker

            if (needsAggregations.get()) {
                requestHandler.aggregations().forEach(searchSourceBuilder::aggregation);
            }

            final SearchRequest searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                    .source(searchSourceBuilder);

            LOG.trace("Kicking initial search for query {}", searchSourceBuilder);
            semaphore.tryAcquire();
            indexNode.getConnection().getClient().searchAsync(searchRequest, RequestOptions.DEFAULT, this);
        }

        /**
         * Handle the response action notifying the registered listeners. Depending on the listeners' configuration
         * it could keep loading chunks or wait for a {@code #scan} call to resume scanning.
         *
         * Some code in this method relies on structure that are not thread safe. We need to make sure
         * these data structures are modified before releasing the semaphore.
         */
        @Override
        public void onResponse(SearchResponse searchResponse) {
            final SearchHit[] searchHits = searchResponse.getHits().getHits();
            if (searchHits != null && searchHits.length > 0) {
                long totalHits = searchResponse.getHits().getTotalHits().value;
                LOG.debug("Processing search response that took {} to read {}/{} docs",
                        searchResponse.getTook(), searchHits.length, totalHits);
                lastHitSortValues = searchHits[searchHits.length - 1].getSortValues();
                scannedRows += searchHits.length;
                anyDataLeft.set(totalHits > scannedRows);
                estimator.update(indexPlan.getFilter(), totalHits);

                // now that we got the last hit we can release the semaphore to potentially unlock other requests
                semaphore.release();

                if (firstRequest) {
                    for (SearchHitListener l : searchHitListeners) {
                        l.startData(totalHits);
                    }

                    if (!aggregationListeners.isEmpty()) {
                        Aggregations aggregations = searchResponse.getAggregations();
                        LOG.trace("Emitting aggregations {}", aggregations);
                        for (AggregationListener l : aggregationListeners) {
                            l.on(aggregations);
                        }
                    }

                    firstRequest = false;
                }

                LOG.trace("Emitting {} search hits, for a total of {} scanned results", searchHits.length, scannedRows);
                for (SearchHit hit : searchHits) {
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
                        .size(LARGE_RESULT_SET_SIZE)
                        .fetchSource(sourceFields, null)
                        .searchAfter(lastHitSortValues)
                        // TODO: this needs to be moved in the requestHandler when sorting will be properly supported
                        .sort(SortBuilders.fieldSort("_score").order(SortOrder.DESC))
                        .sort(SortBuilders.fieldSort(FieldNames.PATH).order(SortOrder.ASC)); // tie-breaker

                final SearchRequest searchRequest = new SearchRequest(indexNode.getDefinition().getRemoteIndexAlias())
                        .source(searchSourceBuilder);
                LOG.trace("Kicking new search after query {}", searchRequest.source());

                indexNode.getConnection().getClient().searchAsync(searchRequest, RequestOptions.DEFAULT, this);
            } else {
                LOG.trace("Scanner is closing or still processing data from the previous scan");
            }
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
