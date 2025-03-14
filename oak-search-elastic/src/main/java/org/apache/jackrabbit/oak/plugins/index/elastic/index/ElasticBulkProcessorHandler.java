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
import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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

    public static final String BULK_ACTIONS_PROP = "oak.indexer.elastic.bulkProcessor.maxBulkOperations";
    public static final int BULK_ACTIONS_DEFAULT = 250;
    public static final String BULK_SIZE_BYTES_PROP = "oak.indexer.elastic.bulkProcessor.maxBulkSizeBytes";
    public static final int BULK_SIZE_BYTES_DEFAULT = 1024 * 1024; // 1MB
    public static final String BULK_FLUSH_INTERVAL_MS_PROP = "oak.indexer.elastic.bulkProcessor.bulkFlushIntervalMs";
    public static final int BULK_FLUSH_INTERVAL_MS_DEFAULT = 3000;
    public static final String BULK_PROCESSOR_CONCURRENCY_PROP = "oak.indexer.elastic.bulkProcessor.concurrency";
    // when true, fails indexing in case of bulk failures
    public static final String FAIL_ON_ERROR_PROP = "oak.indexer.elastic.bulkProcessor.failOnError";
    public static final boolean FAIL_ON_ERROR_DEFAULT = true;
    private static final String SYNC_MODE_PROPERTY = "sync-mode";
    private static final String SYNC_RT_MODE = "rt";

    private final int FAILED_DOC_COUNT_FOR_STATUS_NODE = ConfigHelper.getSystemPropertyAsInt("oak.failedDocStatusLimit", 10000);
    private final int BULK_MAX_OPERATIONS = ConfigHelper.getSystemPropertyAsInt(BULK_ACTIONS_PROP, BULK_ACTIONS_DEFAULT);
    private final int BULK_MAX_SIZE_BYTES = ConfigHelper.getSystemPropertyAsInt(BULK_SIZE_BYTES_PROP, BULK_SIZE_BYTES_DEFAULT);
    private final int BULK_FLUSH_INTERVAL_MS = ConfigHelper.getSystemPropertyAsInt(BULK_FLUSH_INTERVAL_MS_PROP, BULK_FLUSH_INTERVAL_MS_DEFAULT);
    private final int BULK_PROCESSOR_CONCURRENCY = ConfigHelper.getSystemPropertyAsInt(BULK_PROCESSOR_CONCURRENCY_PROP, 1);
    private final boolean FAIL_ON_ERROR = ConfigHelper.getSystemPropertyAsBoolean(FAIL_ON_ERROR_PROP, FAIL_ON_ERROR_DEFAULT);
    protected final ElasticConnection elasticConnection;
    protected final BulkIngester<OperationContext> bulkIngester;

    // Used to keep track of the sequence number of the batches that are currently being processed.
    // This is used to wait until all operations for a writer are processed before closing it.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition bulkProcessedCondition = lock.newCondition();
    private final HashSet<Long> pendingBulks = new HashSet<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, IndexInfo> registeredIndexes = new ConcurrentHashMap<>();

    protected long totalOperations;
    private final ConcurrentLinkedQueue<ErrorCause> globalSuppressedErrorCauses = new ConcurrentLinkedQueue<>();

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

    public ElasticBulkProcessorHandler(@NotNull ElasticConnection elasticConnection) {
        this.elasticConnection = elasticConnection;
        // BulkIngester does not support retry policies. Some retries though are already implemented in the transport layer.
        // More details here: https://github.com/elastic/elasticsearch-java/issues/478
        LOG.info("Creating bulk ingester [maxActions: {}, maxSizeBytes: {} flushInterval {}, concurrency {}]",
                BULK_MAX_OPERATIONS, BULK_MAX_SIZE_BYTES, BULK_FLUSH_INTERVAL_MS, BULK_PROCESSOR_CONCURRENCY_PROP);
        this.bulkIngester = BulkIngester.of(b -> {
            b = b.client(elasticConnection.getAsyncClient())
                    .listener(new OakBulkListener());
            if (BULK_MAX_OPERATIONS > 0) {
                b = b.maxOperations(BULK_MAX_OPERATIONS);
            }
            if (BULK_MAX_SIZE_BYTES > 0) {
                b = b.maxSize(BULK_MAX_SIZE_BYTES);
            }
            if (BULK_FLUSH_INTERVAL_MS > 0) {
                b = b.flushInterval(BULK_FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
            }
            if (BULK_PROCESSOR_CONCURRENCY > 0) {
                b = b.maxConcurrentRequests(BULK_PROCESSOR_CONCURRENCY);
            }
            return b;
        });
    }

    /**
     * Registers an ElasticIndex with the given index definition configuration.
     * <p>
     * The `sync-mode` property can be set to `rt` (real-time). In this case the returned handler will be real-time.
     * This option is available for sync index definitions only.
     *
     * @param indexName
     * @param indexDefinition
     * @param definitionBuilder
     * @param commitInfo
     * @param waitForESAcknowledgement
     */
    public void registerIndex(String indexName, ElasticIndexDefinition indexDefinition, NodeBuilder definitionBuilder, CommitInfo commitInfo, boolean waitForESAcknowledgement) {
        checkOpen();
        if (registeredIndexes.containsKey(indexName)) {
            LOG.warn("Index already registered: {}", indexName);
        } else {
            registeredIndexes.computeIfAbsent(indexName, indexNameFinal -> {
                LOG.warn("Registering index: {}", indexNameFinal);
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
     * Waits for all the bulk requests to return.
     *
     * @return {@code true} if at least one update was performed, {@code false} otherwise
     * @throws IOException if an error happened while processing the bulk requests
     */
    public boolean closeIndex(String indexName) throws IOException {
        LOG.info("Closing index: {}", indexName);
        checkOpen();
        // TODO: Must wait for all operations for this index to complete
        IndexInfo indexInfo = registeredIndexes.remove(indexName);
        if (indexInfo == null) {
            throw new IllegalArgumentException("Index not registered: " + indexName);
        }

        // Some of the operations for this index pending may be buffered for sending in the bulk ingester.
        // Force sending them now.
        LOG.trace("Flushing bulk ingester {}", bulkIngester);
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
                long remainingTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(BULK_FLUSH_INTERVAL_MS * 5L);
                while (lowestPendingBulkRequest.isPresent() && lowestPendingBulkRequest.getAsLong() <= highestBulkRequestSent) {
                    LOG.info("Waiting for request {} to be processed. Lowest pending request: {}", lowestPendingBulkRequest.getAsLong(), lowestPendingBulkRequest.getAsLong());
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
                LOG.info("All requests up to {} have been processed, index flushed and closed", highestBulkRequestSent);
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

        checkFailuresForIndex(indexInfo);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Bulk identifier -> update status = {}", registeredIndexes);
        }
        return indexInfo.indexModified;
    }

    /**
     * Closes the bulk ingester and waits for all the bulk requests to return.
     *
     * @throws IOException if an error happened while processing the bulk requests
     */
    public void close() throws IOException {
        if (closed.getAndSet(true)) {
            LOG.info("Already closed");
        } else {
            LOG.info("Closing bulk processor handler");
            LOG.trace("Calling close on bulk ingester {}", bulkIngester);
            // This blocks until all requests are processed
            // Fail is some of the indexes were not closed
            if (!registeredIndexes.isEmpty()) {
                throw new IllegalStateException("Some indexes are still open: " + Collections.list(registeredIndexes.keys()));
            }
            bulkIngester.close();
            LOG.trace("Bulk Ingester {} closed", bulkIngester);
            if (!globalSuppressedErrorCauses.isEmpty()) {
                IOException ioe = new IOException("Exception while indexing. See suppressed for details");
                globalSuppressedErrorCauses.stream().map(ec -> new IllegalStateException(ec.reason())).forEach(ioe::addSuppressed);
                throw ioe;
            }
        }
    }

    private void checkFailuresForIndex(IndexInfo indexInfo) throws IOException {
        if (!indexInfo.suppressedErrorCauses.isEmpty()) {
            IOException ioe = new IOException("Exception while indexing. See suppressed for details");
            indexInfo.suppressedErrorCauses.stream().map(ec -> new IllegalStateException(ec.reason())).forEach(ioe::addSuppressed);
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
        if (totalOperations%128 == 0) {
            LOG.info("Adding operation: [{}]: {}", context.indexInfo.indexName, context.documentId);
        }
        // fail fast: we don't want to wait until the processor gets closed to fail
        checkFailuresForIndex(context.indexInfo);
        bulkIngester.add(operation, context);
        totalOperations++;
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


            LOG.info("Sending bulk with id {} -> #ops: {}", executionId, contexts.size());
//            LOG.info("Bulk Requests: \n{}", request.operations()
//                    .stream()
//                    .map(BulkOperation::toString)
//                    .collect(Collectors.joining("\n"))
//            );
            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", request.operations()
                        .stream()
                        .map(BulkOperation::toString)
                        .collect(Collectors.joining("\n"))
                );
            }
        }

        private final class FailedDocSetTracker {
            final HashSet<String> failedDocSet;
            private final NodeBuilder status;
            private boolean updated = false;
            private boolean overflow = false;

            public FailedDocSetTracker(NodeBuilder definitionBuilder) {
                this.failedDocSet = new LinkedHashSet<>();
                this.status = definitionBuilder.child(IndexDefinition.STATUS_NODE);
                // Read the current failed paths (if any) on the :status node into failedDocList
                PropertyState failedDocsProperty = status.getProperty(IndexDefinition.FAILED_DOC_PATHS);
                if (failedDocsProperty != null) {
                    for (String str : failedDocsProperty.getValue(Type.STRINGS)) {
                        failedDocSet.add(str);
                    }
                }
            }

            public void addFailedDocument(String documentId) {
                if (failedDocSet.size() < FAILED_DOC_COUNT_FOR_STATUS_NODE) {
                    failedDocSet.add(documentId);
                    updated = true;
                } else {
                    this.overflow = true;
                }
            }

            public void saveFailedDocSets() {
                if (overflow) {
                    LOG.info("Cannot store all new Failed Docs because {} has been filled up. " +
                            "See previous log entries to find out the details of failed paths", IndexDefinition.FAILED_DOC_PATHS);
                }
                if (updated) {
                    status.setProperty(IndexDefinition.FAILED_DOC_PATHS, failedDocSet, Type.STRINGS);
                }
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List<OperationContext> contexts, BulkResponse response) {
            try {
                LOG.info("Bulk with id {} processed in {} ms", executionId, response.took() / 1_000_000 );
                if (LOG.isTraceEnabled()) {
                    LOG.trace(response.toString());
                }

                HashMap<String, FailedDocSetTracker> failedDocSetMap = new HashMap<>();
                for (int i = 0; i < contexts.size(); i++) {
                    IndexInfo indexInfo = contexts.get(i).indexInfo;
                    BulkResponseItem item = response.items().get(i);
                    if (item.error() == null) {
                        indexInfo.indexModified = true;
                    } else {
                        FailedDocSetTracker failedDocSet = failedDocSetMap.computeIfAbsent(
                                indexInfo.indexName,
                                // TODO: this must be thread safe because there may be several callback threads.
                                //   However, this is not performance critical so we can use coarse grained locking
                                k -> new FailedDocSetTracker(indexInfo.definitionBuilder));

                        if (FAIL_ON_ERROR) {
                            indexInfo.suppressedErrorCauses.add(item.error());
                        }
                        String documentId = contexts.get(i).documentId;
                        failedDocSet.addFailedDocument(documentId);

                        // Log entry to be used to parse logs to get the failed doc id/path if needed
                        LOG.error("ElasticIndex Update Doc Failure: Error while adding/updating doc with id: [{}]", documentId);
                        LOG.error("Failure Details: BulkItem ID: {}, Index: {}, Failure Cause: {}",
                                item.id(), item.index(), item.error());
                    }
                }

                for (FailedDocSetTracker failedDocSet : failedDocSetMap.values()) {
                    failedDocSet.saveFailedDocSets();
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
            try {
                LOG.error("ElasticIndex Update Bulk Failure : Bulk with id {} threw an error", executionId, failure);
                globalSuppressedErrorCauses.add(ErrorCause.of(ec -> {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    failure.printStackTrace(pw);
                    return ec.reason(failure.getMessage()).stackTrace(sw.toString());
                }));
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
