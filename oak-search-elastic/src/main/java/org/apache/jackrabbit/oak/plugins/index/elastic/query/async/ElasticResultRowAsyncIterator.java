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
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
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
            new FulltextResultRow("___OAK_POISON_PILL___", 0d, Map.of(), null, null);

    private final BlockingQueue<FulltextResultRow> queue;

    private final ElasticIndexNode indexNode;
    private final IndexPlan indexPlan;
    private final Predicate<String> rowInclusionPredicate;
    private final ElasticMetricHandler metricHandler;
    private final long enqueueTimeoutMs;
    private final ElasticQueryScanner elasticQueryScanner;
    private final ElasticRequestHandler elasticRequestHandler;
    private final ElasticResponseHandler elasticResponseHandler;
    private final ElasticFacetProvider elasticFacetProvider;
    // Errors reported by Elastic. These errors are logged but not propagated to the caller. They cause end of stream.
    // This is done to keep compatibility with the Lucene implementation of the iterator.
    // See for instance FullTextAnalyzerCommonTest#testFullTextTermWithUnescapedBraces for an example of a test where
    // a parsing error in a query is swallowed by the iterator and the iterator returns no results.
    private final AtomicReference<Throwable> queryErrorRef = new AtomicReference<>();
    // System errors (e.g. timeout, interrupted). These errors are propagated to the caller.
    private final AtomicReference<Throwable> systemErrorRef = new AtomicReference<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private FulltextResultRow nextRow;

    public ElasticResultRowAsyncIterator(@NotNull ElasticIndexNode indexNode,
                                         @NotNull ElasticRequestHandler elasticRequestHandler,
                                         @NotNull ElasticResponseHandler elasticResponseHandler,
                                         @NotNull QueryIndex.IndexPlan indexPlan,
                                         Predicate<String> rowInclusionPredicate,
                                         ElasticMetricHandler metricHandler,
                                         long enqueueTimeoutMs) {
        this.indexNode = indexNode;
        this.elasticRequestHandler = elasticRequestHandler;
        this.elasticResponseHandler = elasticResponseHandler;
        this.indexPlan = indexPlan;
        this.rowInclusionPredicate = rowInclusionPredicate;
        this.metricHandler = metricHandler;
        this.enqueueTimeoutMs = enqueueTimeoutMs;
        this.elasticFacetProvider = elasticRequestHandler.getAsyncFacetProvider(indexNode.getConnection(), elasticResponseHandler);
        // set the queue size to the limit of the query. This is to avoid to load too many results in memory in case the
        // consumer is slow to process them
        int limitReads = (int) indexPlan.getFilter().getQueryLimits().getLimitReads();
        LOG.debug("Creating ElasticResultRowAsyncIterator with limitReads={}", limitReads);
        this.queue = new LinkedBlockingQueue<>(limitReads);
        this.elasticQueryScanner = initScanner();
    }

    @Override
    public boolean hasNext() {
        // if nextRow is not null it means the caller invoked hasNext() before without calling next()
        if (nextRow == null && !isClosed.get()) {
            if (queue.isEmpty()) {
                // this triggers, when needed, the scan of the next results chunk
                elasticQueryScanner.scan();
            }
            try {
                long timeoutMs = indexNode.getDefinition().queryTimeoutMs;
                nextRow = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
                if (nextRow == null) {
                    LOG.warn("Timeout waiting for next result from Elastic, waited {} ms. Closing scanner.", timeoutMs);
                    close();
                    throw new IllegalStateException("Timeout waiting for next result from Elastic");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();  // restore interrupt status
                throw new IllegalStateException("Error reading next result from Elastic", e);
            }
        }

        // Check if there are any Throwable filled from onFailure Callback in the errorReference
        // Any exception (such as ParseException) during the prefetch (init scanner) via the async call to ES would be available here
        // when the cursor is actually being traversed.
        // This is being done so that we can log the caller stack trace in case of any exception from ES and not just the trace of the async query thread.
        Throwable error = queryErrorRef.get();
        if (error != null) {
            error.fillInStackTrace();
            LOG.error("Error while fetching results from Elastic for [{}]", indexPlan.getFilter(), error);
            return false;
        }

        if (nextRow != POISON_PILL) {
            // there is a valid next row
            return true;
        }

        // Received the POISON_PILL. Did the elastic query terminate gracefully?
        Throwable systemError = systemErrorRef.get();
        if (systemError == null) {
            return false; // No more results, graceful termination
        } else {
            throw new IllegalStateException("Error while fetching results from Elastic for [" + indexPlan.getFilter() + "]", error);
        }
    }

    @Override
    public FulltextResultRow next() {
        if (nextRow == null) { // next is called without hasNext
            if (!hasNext()) {
                return null;
            }
        }
        FulltextResultRow row = null;
        if (nextRow != null &&  nextRow != POISON_PILL) {
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
                FulltextResultRow resultRow = new FulltextResultRow(path, searchHit.score() != null ? searchHit.score() : 0.0,
                        elasticResponseHandler.excerpts(searchHit), elasticFacetProvider, null);
                long startNs = System.nanoTime();
                boolean successful = queue.offer(resultRow, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
                if (!successful) {
                    // if we cannot insert the result into the queue, we close the scanner to avoid further processing
                    throw new IllegalStateException("Timeout waiting to insert result into the iterator queue for path: " + path +
                            ". Waited " + (System.nanoTime() - startNs) / 1_000_000 + " ms");
                }
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
            boolean success = queue.offer(POISON_PILL, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
            if (!success) {
                LOG.warn("Timeout waiting to insert poison pill into the iterator queue. The iterator might not be closed properly.");
            }
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
        return elasticQueryScanner.searchRequest.toString();
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            LOG.debug("Closing ElasticResultRowAsyncIterator for index {}", indexNode.getDefinition().getIndexPath());
            elasticQueryScanner.close();
        } else {
            LOG.warn("ElasticResultRowAsyncIterator for index {} is already closed", indexNode.getDefinition().getIndexPath());
        }
    }

    /**
     * Scans Elastic results asynchronously and notify listeners.
     */
    class ElasticQueryScanner {

        private static final int SMALL_RESULT_SET_SIZE = 10;

        private final Set<ElasticResponseListener> allListeners = new HashSet<>();
        private final List<SearchHitListener> searchHitListeners = new ArrayList<>();
        private final List<AggregationListener> aggregationListeners = new ArrayList<>();

        private final String sessionId;
        private final Query query;
        private final SearchRequest searchRequest;
        private final @NotNull List<SortOptions> sorts;
        private final Highlight highlight;
        private final SourceConfig sourceConfig;

        // concurrent data structures to coordinate chunks loading
        private final AtomicBoolean anyDataLeft = new AtomicBoolean(false);
        private final AtomicBoolean isClosed = new AtomicBoolean(false);

        private int scannedRows;
        private int requests;
        private boolean fullScan;
        private long searchStartTime;

        // reference to the last document sort values for search_after queries
        private List<FieldValue> lastHitSortValues;

        // Semaphore to guarantee only one in-flight request to Elastic
        private final Semaphore semaphore = new Semaphore(1);
        volatile private CompletableFuture<SearchResponse<ObjectNode>> ongoingRequest;

        ElasticQueryScanner(List<ElasticResponseListener> listeners) {
            this.query = elasticRequestHandler.baseQuery();
            this.sessionId = "oak-" + ElasticIndexUtils.sha256Hash(this.query.toString().getBytes(StandardCharsets.UTF_8));
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
                                .size(needsAggregations.get() ? Math.min(SMALL_RESULT_SET_SIZE, getFetchSize(requests)) : getFetchSize(requests))
                                // consistently route the same queries to the same shard copy (primary or replica) within the shard set
                                .preference(sessionId);
                        if (needsAggregations.get()) {
                            builder.aggregations(elasticRequestHandler.aggregations());
                        }

                        return builder;
                    }
            );

            LOG.trace("Kicking initial search for query {}", searchRequest);
            boolean permitAcquired = semaphore.tryAcquire();
            if (!permitAcquired) {
                LOG.warn("Semaphore not acquired for initial search, scanner is closing or still processing data from the previous scan");
                throw new IllegalStateException("Scanner is closing or still processing data from the previous scan");
            }

            searchStartTime = System.currentTimeMillis();
            requests++;

            ongoingRequest = indexNode.getConnection().getAsyncClient()
                    .search(searchRequest, ObjectNode.class)
                    .whenComplete((this::handleResponse));
            metricHandler.markQuery(indexNode.getDefinition().getIndexPath(), true);
        }

        /**
         * Handle the response action notifying the registered listeners. Depending on the listeners' configuration
         * it could keep loading chunks or wait for a {@code #scan} call to resume scanning.
         * <p>
         * Some code in this method relies on structure that are not thread safe. We need to make sure
         * these data structures are modified before releasing the semaphore.
         */
        private void onSuccess(@NotNull SearchResponse<ObjectNode> searchResponse) {
            long searchTotalTime = System.currentTimeMillis() - searchStartTime;
            List<Hit<ObjectNode>> searchHits = searchResponse.hits().hits();
            int hitsSize = searchHits != null ? searchHits.size() : 0;
            metricHandler.measureQuery(indexNode.getDefinition().getIndexPath(), hitsSize, searchResponse.took(),
                    searchTotalTime, searchResponse.timedOut());
            if (hitsSize > 0) {
                long totalHits = searchResponse.hits().total().value();
                LOG.debug("Processing search response that took {} ms to read {}/{} docs", searchResponse.took(), hitsSize, totalHits);
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

        private void onFailure(@NotNull Throwable t) {
            metricHandler.measureFailedQuery(indexNode.getDefinition().getIndexPath(),
                    System.currentTimeMillis() - searchStartTime);
            // Check in case errorRef is already set - this seems unlikely since we close the scanner once we hit failure.
            // But still, in case this do happen, we will log a warning.
            Throwable error = queryErrorRef.getAndSet(t);
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
            if (isClosed.get()) {
                LOG.debug("Scanner is closed, ignoring scan request");
                return;
            }
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
                        // consistently route the same queries to the same shard copy (primary or replica) within the shard set
                        .preference(sessionId)
                );
                LOG.trace("Kicking new search after query {}", searchReq);

                searchStartTime = System.currentTimeMillis();
                ongoingRequest = indexNode.getConnection().getAsyncClient()
                        .search(searchReq, ObjectNode.class)
                        .whenComplete(this::handleResponse);
                metricHandler.markQuery(indexNode.getDefinition().getIndexPath(), false);
            } else {
                LOG.trace("Scanner is closing or still processing data from the previous scan");
            }
        }

        private void handleResponse(SearchResponse<ObjectNode> searchResponse, Throwable throwable) {
            ongoingRequest = null;
            if (isClosed.get()) {
                LOG.info("Scanner is closed, not processing search response");
                return;
            }
            try {
                if (throwable == null) {
                    onSuccess(searchResponse);
                } else {
                    onFailure(throwable);
                }
            } catch (Throwable t) {
                LOG.warn("Error processing search response", t);
                Throwable prevValue = systemErrorRef.getAndSet(t);
                if (prevValue != null) {
                    LOG.warn("System error reference was previously set to {}. It has now been reset to new error {}", prevValue.getMessage(), t.getMessage());
                }
                try {
                    if (!queue.offer(POISON_PILL, enqueueTimeoutMs, TimeUnit.MILLISECONDS)) {
                        LOG.warn("Timeout waiting to enqueue poison pill after error processing search response. The iterator might not be closed properly.");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // restore interrupt status
                    LOG.warn("Interrupted while trying to enqueue poison pill after error processing search response", e);
                }
                // This method should not throw exceptions, see the whenComplete() contract.
            }
        }

        /* picks the size in the fetch array at index=requests or the last if out of bound */
        private int getFetchSize(int requestId) {
            int[] queryFetchSizes = indexNode.getDefinition().queryFetchSizes;
            return queryFetchSizes.length > requestId ?
                    queryFetchSizes[requestId] : queryFetchSizes[queryFetchSizes.length - 1];
        }

        private void close() {
            if (isClosed.compareAndSet(false, true)) {
                LOG.debug("Closing ElasticQueryScanner for index {}", indexNode.getDefinition().getIndexPath());
                // Close listeners and release the semaphore
                semaphore.release();
                for (ElasticResponseListener l : allListeners) {
                    try {
                        l.endData();
                    } catch (Exception ex) {
                        LOG.warn("Error while closing listener {}", l.getClass().getName(), ex);
                    }
                }
                allListeners.clear();

                if (ongoingRequest != null) {
                    ongoingRequest.cancel(true);
                    ongoingRequest = null;
                }
            } else {
                LOG.info("ElasticQueryScanner for index {} is already closed", indexNode.getDefinition().getIndexPath());
            }
        }
    }
}
