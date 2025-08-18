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
package org.apache.jackrabbit.oak.plugins.index.elastic.index;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.json.JsonData;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.log.LogSilencer;
import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ElasticBulkProcessorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticBulkProcessorHandler.class);
    private static final LogSilencer LOG_SILENCER = new LogSilencer(Duration.ofSeconds(5).toMillis(), 50);

    /**
     * Keeps information about an index that is being written by the bulk processor
     */
    static class IndexInfo {
        public final String indexName;
        public final ElasticIndexDefinition indexDefinition;
        public final NodeBuilder definitionBuilder;
        public final boolean waitForESAcknowledgement;
        public final boolean isRealTime;
        /**
         * Exceptions occurred while trying to update index in elasticsearch
         */
        public final ConcurrentLinkedQueue<ErrorCause> suppressedErrorCauses = new ConcurrentLinkedQueue<>();

        long indexOperations = 0;
        long deleteOperations = 0;
        long updateOperations = 0;
        boolean indexModified = false;

        IndexInfo(String indexName, ElasticIndexDefinition indexDefinition, NodeBuilder definitionBuilder, boolean waitForESAcknowledgement, boolean isRealTime) {
            this.indexName = indexName;
            this.indexDefinition = indexDefinition;
            this.definitionBuilder = definitionBuilder;
            this.waitForESAcknowledgement = waitForESAcknowledgement;
            this.isRealTime = isRealTime;
        }
    }

    /**
     * Context object associated with each operation passed to the bulk processor
     */
    public final static class OperationContext {
        final IndexInfo indexInfo;
        final String documentId;

        OperationContext(IndexInfo indexInfo, String documentId) {
            this.indexInfo = indexInfo;
            this.documentId = documentId;
        }

        @Override
        public String toString() {
            return "OperationContext{" +
                    "indexInfo=" + indexInfo.indexName +
                    ", documentId='" + documentId + '\'' +
                    '}';
        }
    }

    public static final String BULK_ACTIONS_PROP = "oak.indexer.elastic.bulkProcessor.maxBulkOperations";
    public static final int BULK_ACTIONS_DEFAULT = 8192;
    public static final String BULK_SIZE_BYTES_PROP = "oak.indexer.elastic.bulkProcessor.maxBulkSizeBytes";
    public static final int BULK_SIZE_BYTES_DEFAULT = 8 * 1024 * 1024; // 8MB
    public static final String BULK_FLUSH_INTERVAL_MS_PROP = "oak.indexer.elastic.bulkProcessor.bulkFlushIntervalMs";
    public static final int BULK_FLUSH_INTERVAL_MS_DEFAULT = 3000;
    public static final String BULK_MAX_CONCURRENT_REQUESTS_PROP = "oak.indexer.elastic.bulkProcessor.maxConcurrentRequests";
    private static final int BULK_MAX_CONCURRENT_REQUESTS_DEFAULT = 1;
    // when true, fails indexing in case of bulk failures
    public static final String FAIL_ON_ERROR_PROP = "oak.indexer.elastic.bulkProcessor.failOnError";
    public static final boolean FAIL_ON_ERROR_DEFAULT = false;

    private static final String SYNC_MODE_PROPERTY = "sync-mode";
    private static final String SYNC_RT_MODE = "rt";
    private static final int MAX_SUPPRESSED_ERROR_CAUSES = 50;

    private final int bulkMaxOperations = ConfigHelper.getSystemPropertyAsInt(BULK_ACTIONS_PROP, BULK_ACTIONS_DEFAULT);
    private final int bulkMaxSizeBytes = ConfigHelper.getSystemPropertyAsInt(BULK_SIZE_BYTES_PROP, BULK_SIZE_BYTES_DEFAULT);
    private final int bulkFlushIntervalMillis = ConfigHelper.getSystemPropertyAsInt(BULK_FLUSH_INTERVAL_MS_PROP, BULK_FLUSH_INTERVAL_MS_DEFAULT);
    private final int bulkMaxConcurrentRequests = ConfigHelper.getSystemPropertyAsInt(BULK_MAX_CONCURRENT_REQUESTS_PROP, BULK_MAX_CONCURRENT_REQUESTS_DEFAULT);
    // If false, failures to index documents will not throw an exception, but will be logged instead. If true, if a document
    // fails to index, an exception will be thrown in the next call done to that particular index (add a document or close the index).
    // Connection errors will always throw an exception, regardless of this setting, because they relate to the connection to the Elasticsearch server.
    private final boolean failOnIndexingError = ConfigHelper.getSystemPropertyAsBoolean(FAIL_ON_ERROR_PROP, FAIL_ON_ERROR_DEFAULT);

    private final ElasticConnection elasticConnection;
    private final BulkIngester<OperationContext> bulkIngester;

    // Used to keep track of the sequence number of the batches that are currently being processed.
    // This is used to wait until all operations for a writer are processed before closing it.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition bulkProcessedCondition = lock.newCondition();
    private final HashSet<Long> pendingBulks = new HashSet<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, IndexInfo> registeredIndexes = new ConcurrentHashMap<>();
    private volatile Throwable lastConnectionError = null;

    // Time blocked waiting to add operations to the bulk processor.
    private final long startTime = System.nanoTime();
    private long totalWaitTimeNanos = 0;

    public ElasticBulkProcessorHandler(@NotNull ElasticConnection elasticConnection) {
        this.elasticConnection = elasticConnection;
        // BulkIngester does not support retry policies. Some retries though are already implemented in the transport layer.
        // More details here: https://github.com/elastic/elasticsearch-java/issues/478
        LOG.info("Creating bulk ingester [maxOperations: {}, maxSizeBytes: {}, flushIntervalMillis: {}, maxConcurrentRequests: {}]",
                bulkMaxOperations, bulkMaxSizeBytes, bulkFlushIntervalMillis, bulkMaxConcurrentRequests);
        this.bulkIngester = BulkIngester.of(b -> {
            b = b.client(elasticConnection.getAsyncClient())
                    .listener(new OakBulkListener());
            if (bulkMaxOperations > 0) {
                b = b.maxOperations(bulkMaxOperations);
            }
            if (bulkMaxSizeBytes > 0) {
                b = b.maxSize(bulkMaxSizeBytes);
            }
            if (bulkFlushIntervalMillis > 0) {
                b = b.flushInterval(bulkFlushIntervalMillis, TimeUnit.MILLISECONDS);
            }
            if (bulkMaxConcurrentRequests > 0) {
                b = b.maxConcurrentRequests(bulkMaxConcurrentRequests);
            }
            return b;
        });
    }

    /**
     * Registers an ElasticIndex with the given index definition configuration.
     * <p>
     * The `sync-mode` property can be set to `rt` (real-time). In this case the returned handler will be real-time.
     * This option is available for sync index definitions only.
     */
    public void registerIndex(String indexName, ElasticIndexDefinition indexDefinition, NodeBuilder definitionBuilder, CommitInfo commitInfo, boolean waitForESAcknowledgement) {
        checkOpen();
        if (registeredIndexes.containsKey(indexName)) {
            LOG.warn("Index already registered: {}", indexName);
        } else {
            registeredIndexes.computeIfAbsent(indexName, indexNameFinal -> {
                LOG.debug("Registering index: {}", indexNameFinal);
                PropertyState async = indexDefinition.getDefinitionNodeState().getProperty("async");
                boolean isRealTime;
                if (async == null) {
                    // commit-info has priority over configuration in index definition
                    String syncMode = null;
                    if (commitInfo != null) {
                        syncMode = (String) commitInfo.getInfo().get(SYNC_MODE_PROPERTY);
                    }

                    if (syncMode == null) {
                        PropertyState syncModeProp = indexDefinition.getDefinitionNodeState().getProperty("sync-mode");
                        if (syncModeProp != null) {
                            syncMode = syncModeProp.getValue(Type.STRING);
                        }
                    }
                    isRealTime = SYNC_RT_MODE.equals(syncMode);
                } else {
                    isRealTime = false;
                }
                return new IndexInfo(indexName, indexDefinition, definitionBuilder, waitForESAcknowledgement, isRealTime);
            });
        }
    }

    private void checkOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Bulk processor handler is closed");
        }
    }

    IndexInfo getIndexInfo(String indexName) {
        return registeredIndexes.get(indexName);
    }

    /**
     * Indexes a document in the bulk processor. The document is identified by the given id. If the document already exists it will be replaced by the new one.
     *
     * @param indexName the index name
     * @param id        the document id
     * @param document  the document to index
     * @throws IOException if an error happened while processing the bulk request
     */
    public void index(String indexName, String id, ElasticDocument document) throws IOException {
        checkOpen();
        IndexInfo indexInfo = getIndexInfoOrFail(indexName);
        indexInfo.indexOperations++;
        add(BulkOperation.of(op -> op.index(idx -> idx.index(indexName).id(id).document(document))),
                new OperationContext(indexInfo, id)
        );
    }

    public void update(String indexName, String id, ElasticDocument document) throws IOException {
        checkOpen();
        IndexInfo indexInfo = getIndexInfoOrFail(indexName);
        OperationContext context = new OperationContext(indexInfo, id);
        indexInfo.updateOperations++;
        if (document.getPropertiesToRemove().isEmpty()) {
            add(BulkOperation.of(op ->
                    op.update(uf -> uf.index(indexName).id(id).action(uaf -> uaf.doc(document).docAsUpsert(true)))
            ), context);
        } else {
            // when updating a document we need to remove the properties that are not present in the new document
            // to do so we need to keep track of the properties that are present in the document before the update
            // and add a specific script bulk operation to remove them
            // Create a script to update the document and remove properties in one operation
            StringBuilder script = new StringBuilder();
            script.append("ctx._source.putAll(params.document);");
            for (String property : document.getPropertiesToRemove()) {
                script.append("ctx._source.remove('").append(property).append("');");
            }

            // Add the update operation with the script
            add(BulkOperation.of(op -> op.update(uf ->
                            uf.index(indexName).id(id).action(uaf ->
                                    uaf.script(s -> s.source(script.toString()).params("document", JsonData.of(document)))
                                            .upsert(document)))),
                    context);
        }
    }

    public void delete(String indexName, String id) throws IOException {
        checkOpen();
        IndexInfo indexInfo = getIndexInfoOrFail(indexName);
        indexInfo.deleteOperations++;
        add(BulkOperation.of(op -> op.delete(idx -> idx.index(indexName).id(id))), new OperationContext(indexInfo, id));
    }

    /**
     * Flushes an index. The underlying bulk ingestor will be flushed, to ensure that all pending operations for this
     * index are sent to the server. If this index was registered with waitForESAcknowledgement set to true, then this
     * method will wait until we receive an acknowledgement from the server for all the operations up to when this
     * method was called.
     * <p>
     * Note: Flushing an index will have the side effect of flushing all pending operations for all indexes registered
     * with the bulk processor. This should be transparent for the user, but it may mean that this method would take
     * longer to return than if it was flushing only the operations for the index being closed.
     *
     * @return {@code true} if at least one update was performed, {@code false} otherwise
     * @throws IOException if an error happened while processing the bulk requests
     */
    public boolean flushIndex(String indexName) throws IOException {
        LOG.debug("Flushing index: {}", indexName);
        checkOpen();
        // TODO: Must wait for all operations for this index to complete
        IndexInfo indexInfo = registeredIndexes.remove(indexName);
        if (indexInfo == null) {
            throw new IllegalArgumentException("Index not registered: " + indexName);
        }

        // Some of the operations for this index pending may be buffered for sending in the bulk ingester.
        // Force sending them now.
        bulkIngester.flush();

        if (indexInfo.waitForESAcknowledgement) {
            // All the operations for this index have been sent. Now we need to wait for all of them to be processed
            long highestBulkRequestSent = bulkIngester.requestCount();
            lock.lock();
            try {
                // This request number is higher or equal than any request that may contain operations for the index that
                // we are closing. Wait until all requests lower or equal to this number are processed.
                OptionalLong lowestPendingBulkRequest = pendingBulks.stream().mapToLong(Long::longValue).min();
                // If there is no pending request, we return immediately
                long remainingTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(bulkFlushIntervalMillis * 5L);
                while (lowestPendingBulkRequest.isPresent() && lowestPendingBulkRequest.getAsLong() <= highestBulkRequestSent) {
                    LOG.debug("Waiting for request {} to be processed. Lowest pending request: {}", highestBulkRequestSent, lowestPendingBulkRequest.getAsLong());
                    try {
                        if (remainingTimeoutNanos <= 0) {
                            LOG.error("Timeout waiting for bulk requests to return");
                            break;
                        }
                        // wait on condition and check return value
                        remainingTimeoutNanos = bulkProcessedCondition.awaitNanos(remainingTimeoutNanos);
                        lowestPendingBulkRequest = pendingBulks.stream().mapToLong(Long::longValue).min();
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted while waiting for bulk processor to close", e);
                        Thread.currentThread().interrupt();  // restore interrupt status
                    }
                }
                LOG.debug("All requests up to {} have been processed, index flushed and closed", highestBulkRequestSent);
            } finally {
                lock.unlock();
            }
        }

        // TODO: Support real time indexes
        // BulkIngester does not support customization of intermediate requests. This means we cannot intercept the last
        // request and apply a WAIT_UNTIL refresh policy. The workaround is to force a refresh when the handler is closed.
        // We can improve this when this issue gets fixed:
        // <a href="https://github.com/elastic/elasticsearch-java/issues/703">elasticsearch-java#703</a>
        if (indexInfo.isRealTime) {
            LOG.debug("Real-time index {}", indexName);
            if (indexInfo.indexModified) {
                LOG.debug("Forcing refresh");
                try {
                    this.elasticConnection.getClient().indices().refresh(b -> b.index(indexName));
                } catch (IOException e) {
                    LOG.warn("Error refreshing index {}", indexName, e);
                }
            }
        }

        checkConnectionFailures();
        checkFailuresForIndex(indexInfo);
        LOG.trace("Bulk identifier -> update status = {}", registeredIndexes);
        return indexInfo.indexModified;
    }

    /**
     * Closes the bulk ingester. Any registered indexes must have been closed before calling this method.
     *
     * @throws IOException if an error happened while processing the bulk requests
     */
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            LOG.info("Closing bulk processor handler");
            printStatistics();
            // This blocks until all requests are processed
            bulkIngester.close();
            // Fail is some of the indexes were not closed
            if (!registeredIndexes.isEmpty()) {
                LOG.warn("Some indexes were not closed properly: {}", Collections.list(registeredIndexes.keys()));
            }
            checkConnectionFailures();
        }
    }

    private void checkConnectionFailures() throws IOException {
        if (lastConnectionError != null) {
            IOException ioe = new IOException("Service error while indexing.", lastConnectionError);
            lastConnectionError = null; // Clear the last connection error after throwing it
            throw ioe;
        }
    }

    private void checkFailuresForIndex(IndexInfo indexInfo) throws IOException {
        if (!indexInfo.suppressedErrorCauses.isEmpty()) {
            List<ErrorCause> suppressedErrors = new ArrayList<>(indexInfo.suppressedErrorCauses);
            indexInfo.suppressedErrorCauses.clear();
            String overflowMessage = suppressedErrors.size() >= MAX_SUPPRESSED_ERROR_CAUSES ?
                    ". (Too many failed operations in last bulk request, including only " + suppressedErrors.size() + " errors)"
                    : "";
            IOException ioe = new IOException("Error indexing documents for index: " + indexInfo.indexName + ". See suppressed errors for details" + overflowMessage);
            suppressedErrors.forEach(ec -> ioe.addSuppressed(new IllegalStateException(ec.reason())));
            throw ioe;
        }
    }

    private IndexInfo getIndexInfoOrFail(String indexName) {
        IndexInfo indexInfo = registeredIndexes.get(indexName);
        if (indexInfo == null) {
            throw new IllegalArgumentException("Index not registered: " + indexName);
        }
        return indexInfo;
    }

    private void add(BulkOperation operation, OperationContext context) throws IOException {
        // fail fast: we don't want to wait until the processor gets closed to fail
        checkConnectionFailures();
        if (failOnIndexingError) {
            checkFailuresForIndex(context.indexInfo);
        }
        long start = System.nanoTime();
        bulkIngester.add(operation, context);
        long end = System.nanoTime();
        totalWaitTimeNanos += end - start;
    }

    public void printStatistics() {
        LOG.info("BulkIngester statistics: [operationsCount: {}, requestCount: {}, avgOperationsPerBulk: {}, " +
                        "operationContentionsCount: {}, requestContentionsCount: {}, totalWaitTimeMs: {}, percentageWaitTime: {}]",
                bulkIngester.operationsCount(), bulkIngester.requestCount(),
                FormattingUtils.safeComputeAverage(bulkIngester.operationsCount(), bulkIngester.requestCount()),
                bulkIngester.operationContentionsCount(), bulkIngester.requestContentionsCount(),
                TimeUnit.NANOSECONDS.toMillis(totalWaitTimeNanos),
                FormattingUtils.safeComputePercentage(totalWaitTimeNanos, System.nanoTime() - startTime));
    }

    private class OakBulkListener implements BulkListener<OperationContext> {

        @Override
        public void beforeBulk(long executionId, BulkRequest request, List<OperationContext> contexts) {
            lock.lock();
            try {
                pendingBulks.add(executionId);
            } finally {
                lock.unlock();
            }
            if (bulkIngester.requestCount() % 64 == 0) {
                LOG.info("Sending bulk with id {} -> #ops: {}", executionId, contexts.size());
                printStatistics();
            }

            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", request.operations()
                        .stream()
                        .map(BulkOperation::toString)
                        .collect(Collectors.joining("\n"))
                );
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List<OperationContext> contexts, BulkResponse response) {
            // Bullk request has been processed successfully. Some operations may have failed, but the request itself was successful.
            try {
                LOG.debug("Bulk with id {} processed in {} ms", executionId, response.took());
                if (LOG.isTraceEnabled()) {
                    LOG.trace(response.toString());
                }

                for (int i = 0; i < contexts.size(); i++) {
                    IndexInfo indexInfo = contexts.get(i).indexInfo;
                    BulkResponseItem item = response.items().get(i);
                    if (item.error() == null) {
                        indexInfo.indexModified = true;
                    } else {
                        if (failOnIndexingError && indexInfo.suppressedErrorCauses.size() < MAX_SUPPRESSED_ERROR_CAUSES) {
                            indexInfo.suppressedErrorCauses.add(item.error());
                        }
                        String type = item.error().type() != null ? item.error().type() : "type-unknown";
                        String reason = item.error().reason() != null ? item.error().reason() : "reason-unknown";
                        if (reason.length() > 20) {
                            reason = reason.substring(0, 20) + "...";
                        }
                        String logSilenceKey = indexInfo.indexName + ":" + type + ":" + reason;
                        if (!LOG_SILENCER.silence(logSilenceKey)) {
                            // Log entry to be used to parse logs to get the failed doc id/path if needed
                            LOG.warn("Failure Details: BulkItem ID: {}, Index: {}, Failure Cause: {} - {}",
                                    item.id(), item.index(), item.error(), LogSilencer.SILENCING_POSTFIX);
                        }
                    }
                }
            } finally {
                lock.lock();
                try {
                    boolean removed = pendingBulks.remove(executionId);
                    if (!removed) {
                        LOG.warn("Bulk with id {} was not pending", executionId);
                    }
                    bulkProcessedCondition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List<OperationContext> contexts, Throwable failure) {
            // Called in case of a connection failure or other error that prevented the full bulk request from being executed
            try {
                LOG.warn("ElasticIndex Update Bulk Failure : Bulk with id {} threw an error", executionId, failure);
                // Keep only the last connection error
                lastConnectionError = failure;
            } finally {
                lock.lock();
                try {
                    pendingBulks.remove(executionId);
                    bulkProcessedCondition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}
