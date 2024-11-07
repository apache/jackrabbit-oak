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

import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexNode;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticQueryIterator;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticResponseHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.async.facets.ElasticFacetProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.IndexPlan;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Highlight;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.SourceConfig;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonpUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Class to iterate over Elastic results of a given {@link IndexPlan}.
 * The results are produced asynchronously into an internal unbounded {@link BlockingQueue}. To avoid too many calls to
 * Elastic the results are loaded in chunks (using search_after strategy) and loaded only when needed.
 * <p>
 * The resources held by this class are automatically released when the iterator is exhausted. In case the iterator is not
 * exhausted, it is recommended for the caller to invoke {@link #close()} to release the resources.
 * </p
 */
public class ElasticResultRowAsyncIterator implements ElasticQueryIterator, ElasticResponseListener.SearchHitListener {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticResultRowAsyncIterator.class);
    // this is an internal special message to notify the consumer the result set has been completely returned
    private static final FulltextResultRow POISON_PILL =
            new FulltextResultRow("___OAK_POISON_PILL___", 0d, Collections.emptyMap(), null, null);

    private final BlockingQueue<FulltextResultRow> queue;

    private final ElasticIndexNode indexNode;
    private final IndexPlan indexPlan;
    private final Predicate<String> rowInclusionPredicate;
    private final ElasticMetricHandler metricHandler;
    private final ElasticQueryScanner elasticQueryScanner;
    private final ElasticRequestHandler elasticRequestHandler;
    private final ElasticResponseHandler elasticResponseHandler;
    private final ElasticFacetProvider elasticFacetProvider;
    private final AtomicReference<Throwable> errorRef = new AtomicReference<>();

    private FulltextResultRow nextRow;

    public ElasticResultRowAsyncIterator(@NotNull ElasticIndexNode indexNode,
                                         @NotNull ElasticRequestHandler elasticRequestHandler,
                                         @NotNull ElasticResponseHandler elasticResponseHandler,
                                         @NotNull QueryIndex.IndexPlan indexPlan,
                                         Predicate<String> rowInclusionPredicate,
                                         ElasticMetricHandler metricHandler) {
        this.indexNode = indexNode;
        this.elasticRequestHandler = elasticRequestHandler;
        this.elasticResponseHandler = elasticResponseHandler;
        this.indexPlan = indexPlan;
        this.rowInclusionPredicate = rowInclusionPredicate;
        this.metricHandler = metricHandler;
        this.elasticFacetProvider = elasticRequestHandler.getAsyncFacetProvider(indexNode.getConnection(), elasticResponseHandler);
        // set the queue size to the limit of the query. This is to avoid to load too many results in memory in case the
        // consumer is slow to process them
        this.queue = new LinkedBlockingQueue<>((int) indexPlan.getFilter().getQueryLimits().getLimitReads());
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
                nextRow = queue.poll(indexNode.getDefinition().queryTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupt status
                throw new IllegalStateException("Error reading next result from Elastic", e);
            }
        }

        // Check if there are any Throwable filled from onFailure Callback in the errorReference
        // Any exception (such as ParseException) during the prefetch (init scanner) via the async call to ES would be available here
        // when the cursor is actually being traversed.
        // This is being done so that we can log the caller stack trace in case of any exception from ES and not just the trace of the async query thread.

        Throwable error = errorRef.getAndSet(null);
        if (error != null) {
            error.fillInStackTrace();
            LOG.error("Error while fetching results from Elastic for [{}]", indexPlan.getFilter(), error);
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
    public boolean on(Hit<ObjectNode> searchHit) {
        final String path = elasticResponseHandler.getPath(searchHit);
        if (path != null) {
            if (rowInclusionPredicate != null && !rowInclusionPredicate.test(path)) {
                LOG.trace("Path {} not included because of hierarchy inclusion rules", path);
                return false;
            }
            LOG.trace("Path {} satisfies hierarchy inclusion rules", path);
            try {
                queue.put(new FulltextResultRow(path, searchHit.score() != null ? searchHit.score() : 0.0,
                        elasticResponseHandler.excerpts(searchHit), elasticFacetProvider, null));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupt status
                throw new IllegalStateException("Error producing results into the iterator queue", e);
            }
            return true;
        }
        return false;
    }

    @Override
    public void endData() {
        try {
            queue.put(POISON_PILL);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // restore interrupt status
            throw new IllegalStateException("Error inserting poison pill into the iterator queue", e);
        }
    }

    private ElasticQueryScanner initScanner() {
        List<ElasticResponseListener> listeners = new ArrayList<>();
        // TODO: we could avoid to register this listener when the client is interested in facets only. It would save space and time
        listeners.add(this);
        if (elasticFacetProvider != null && elasticFacetProvider instanceof ElasticResponseListener) {
            listeners.add((ElasticResponseListener) elasticFacetProvider);
        }

        return new ElasticQueryScanner(listeners);
    }

    /*
     * TODO: to return the explain output, the scanner gets created and an initial request to Elastic is sent. This could
     * be avoided if we decouple the scanner creation from the first request to Elastic. This would require to change the
     * way the scanner is created and the way the explain is retrieved. This is not a priority now and should not be an issue
     * since the first request returns a small amount of data and the explain is a user debug feature.
     */
    @Override
    public String explain() {
        return JsonpUtils.toString(elasticQueryScanner.searchRequest, new StringBuilder()).toString();
    }

    @Override
    public void close() {
        elasticQueryScanner.close();
    }

    /**
     * Scans Elastic results asynchronously and notify listeners.
     */
    class ElasticQueryScanner {

        private static final int SMALL_RESULT_SET_SIZE = 10;

        private final Set<ElasticResponseListener> allListeners = new HashSet<>();
        private final List<SearchHitListener> searchHitListeners = new ArrayList<>();
        private final List<AggregationListener> aggregationListeners = new ArrayList<>();

        private final Query query;
        private final SearchRequest searchRequest;
        private final @NotNull List<SortOptions> sorts;
        private final Highlight highlight;
        private final SourceConfig sourceConfig;

        // concurrent data structures to coordinate chunks loading
        private final AtomicBoolean anyDataLeft = new AtomicBoolean(false);

        private int scannedRows;
        private int requests;
        private boolean fullScan;
        private long searchStartTime;

        // reference to the last document sort values for search_after queries
        private List<FieldValue> lastHitSortValues;

        // Semaphore to guarantee only one in-flight request to Elastic
        private final Semaphore semaphore = new Semaphore(1);

        ElasticQueryScanner(List<ElasticResponseListener> listeners) {
            this.query = elasticRequestHandler.baseQuery();
            this.sorts = elasticRequestHandler.baseSorts();
            this.highlight = elasticRequestHandler.highlight();

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
            this.sourceConfig = SourceConfig.of(fn -> fn.filter(f -> f.includes(new ArrayList<>(sourceFieldsSet))));

            searchRequest = SearchRequest.of(builder -> {
                        builder
                                .index(indexNode.getDefinition().getIndexAlias())
                                .trackTotalHits(thb -> thb.count(indexNode.getDefinition().trackTotalHits))
                                .sort(sorts)
                                .source(sourceConfig)
                                .query(query)
                                .highlight(highlight)
                                // use a smaller size when the query contains aggregations. This improves performance
                                // when the client is only interested in insecure facets
                                .size(needsAggregations.get() ? Math.min(SMALL_RESULT_SET_SIZE, getFetchSize(requests)) : getFetchSize(requests));
                        if (needsAggregations.get()) {
                            builder.aggregations(elasticRequestHandler.aggregations());
                        }

                        return builder;
                    }
            );

            LOG.trace("Kicking initial search for query {}", searchRequest);
            semaphore.tryAcquire();

            searchStartTime = System.currentTimeMillis();
            requests++;

            indexNode.getConnection().getAsyncClient()
                    .search(searchRequest, ObjectNode.class)
                    .whenComplete(((searchResponse, throwable) -> {
                        if (throwable != null) {
                            onFailure(throwable);
                        } else onSuccess(searchResponse);
                    }));
            metricHandler.markQuery(indexNode.getDefinition().getIndexPath(), true);
        }

        /**
         * Handle the response action notifying the registered listeners. Depending on the listeners' configuration
         * it could keep loading chunks or wait for a {@code #scan} call to resume scanning.
         * <p>
         * Some code in this method relies on structure that are not thread safe. We need to make sure
         * these data structures are modified before releasing the semaphore.
         */
        public void onSuccess(SearchResponse<ObjectNode> searchResponse) {
            long searchTotalTime = System.currentTimeMillis() - searchStartTime;

            List<Hit<ObjectNode>> searchHits = searchResponse.hits().hits();
            int hitsSize = searchHits != null ? searchHits.size() : 0;
            metricHandler.measureQuery(indexNode.getDefinition().getIndexPath(), hitsSize, searchResponse.took(),
                    searchTotalTime, searchResponse.timedOut());
            if (hitsSize > 0) {
                long totalHits = searchResponse.hits().total().value();
                LOG.debug("Processing search response that took {} to read {}/{} docs", searchResponse.took(), hitsSize, totalHits);
                lastHitSortValues = searchHits.get(hitsSize - 1).sort();
                scannedRows += hitsSize;
                if (searchResponse.hits().total().relation() == TotalHitsRelation.Eq) {
                    anyDataLeft.set(totalHits > scannedRows);
                } else {
                    anyDataLeft.set(true);
                }

                // now that we got the last hit we can release the semaphore to potentially unlock other requests
                semaphore.release();

                if (requests == 1) {
                    for (SearchHitListener l : searchHitListeners) {
                        l.startData(totalHits);
                    }

                    if (!aggregationListeners.isEmpty()) {
                        LOG.trace("Emitting aggregations {}", searchResponse.aggregations());
                        for (AggregationListener l : aggregationListeners) {
                            l.on(searchResponse.aggregations());
                        }
                    }
                }

                LOG.trace("Emitting {} search hits, for a total of {} scanned results", searchHits.size(), scannedRows);

                BitSet listenersWithHits = new BitSet(searchHitListeners.size());

                for (Hit<ObjectNode> hit : searchHits) {
                    for (int index = 0; index < searchHitListeners.size(); index++) {
                        SearchHitListener l = searchHitListeners.get(index);
                        if (l.on(hit)) {
                            listenersWithHits.set(index);
                        }
                    }
                }
                // if any listener has not processed any hit, it means we need to load more data since there could be
                // listeners waiting for some results before triggering a new scan
                boolean areAllListenersProcessed = listenersWithHits.cardinality() == searchHitListeners.size();

                if (!anyDataLeft.get()) {
                    LOG.trace("No data left: closing scanner, notifying listeners");
                    close();
                } else if (fullScan || !areAllListenersProcessed) {
                    scan();
                }
            } else {
                LOG.trace("No results: closing scanner, notifying listeners");
                close();
            }
        }

        public void onFailure(Throwable t) {
            metricHandler.measureFailedQuery(indexNode.getDefinition().getIndexPath(),
                    System.currentTimeMillis() - searchStartTime);
            // Check in case errorRef is already set - this seems unlikely since we close the scanner once we hit failure.
            // But still, in case this do happen, we will log a warning.
            Throwable error = errorRef.getAndSet(t);
            if (error != null) {
                LOG.warn("Error reference for async iterator was previously set to {}. It has now been reset to new error {}", error.getMessage(), t.getMessage());
            }

            if (t instanceof ElasticsearchException) {
                LOG.error("Elastic could not process the request for jcr query [{}] :: Corresponding ES query {} :: ES Response {} : closing scanner, notifying listeners",
                        indexPlan.getFilter(), query, ((ElasticsearchException) t).error(), t);
            } else {
                LOG.error("Error retrieving data for jcr query [{}] :: Corresponding ES query {} : closing scanner, notifying listeners",
                        indexPlan.getFilter(), query, t);
            }
            // closing scanner immediately after a failure avoiding them to hang (potentially) forever
            close();
        }

        /**
         * Triggers a scan of a new chunk of the result set, if needed.
         */
        private void scan() {
            if (semaphore.tryAcquire() && anyDataLeft.get()) {
                final SearchRequest searchReq = SearchRequest.of(s -> s
                        .index(indexNode.getDefinition().getIndexAlias())
                        .trackTotalHits(thb -> thb.count(indexNode.getDefinition().trackTotalHits))
                        .sort(sorts)
                        .source(sourceConfig)
                        .searchAfter(lastHitSortValues)
                        .query(query)
                        .highlight(highlight)
                        .size(getFetchSize(requests++))
                );
                LOG.trace("Kicking new search after query {}", searchReq);

                searchStartTime = System.currentTimeMillis();
                indexNode.getConnection().getAsyncClient()
                        .search(searchReq, ObjectNode.class)
                        .whenComplete(((searchResponse, throwable) -> {
                            if (throwable != null) {
                                onFailure(throwable);
                            } else onSuccess(searchResponse);
                        }));
                metricHandler.markQuery(indexNode.getDefinition().getIndexPath(), false);
            } else {
                LOG.trace("Scanner is closing or still processing data from the previous scan");
            }
        }

        /* picks the size in the fetch array at index=requests or the last if out of bound */
        private int getFetchSize(int requestId) {
            int[] queryFetchSizes = indexNode.getDefinition().queryFetchSizes;
            return queryFetchSizes.length > requestId ?
                    queryFetchSizes[requestId] : queryFetchSizes[queryFetchSizes.length - 1];
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
