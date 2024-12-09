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
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class ElasticBulkProcessorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticBulkProcessorHandler.class);
    private static final int FAILED_DOC_COUNT_FOR_STATUS_NODE = Integer.getInteger("oak.failedDocStatusLimit", 10000);

    private static final int BULK_PROCESSOR_CONCURRENCY =
        Integer.getInteger("oak.indexer.elastic.bulkProcessorConcurrency", 1);
    private static final String SYNC_MODE_PROPERTY = "sync-mode";
    private static final String SYNC_RT_MODE = "rt";

    protected final ElasticConnection elasticConnection;
    protected final String indexName;
    protected final ElasticIndexDefinition indexDefinition;
    private final NodeBuilder definitionBuilder;
    protected final BulkIngester<String> bulkIngester;
    private final boolean waitForESAcknowledgement;

    /**
     * Coordinates communication between bulk processes. It has a main controller registered at creation time and
     * de-registered on {@link ElasticIndexWriter#close(long)}. Each bulk request register a new party in
     * this Phaser in {@link OakBulkListener#beforeBulk(long, BulkRequest, List)} and de-register itself when
     * the request returns.
     */
    private final Phaser phaser = new Phaser(1); // register main controller

    /**
     * Exceptions occurred while trying to update index in elasticsearch
     */
    private final ConcurrentLinkedQueue<ErrorCause> suppressedErrorCauses = new ConcurrentLinkedQueue<>();

    /**
     * Key-value structure to keep the history of bulk requests. Keys are the bulk execution ids, the boolean
     * value is {@code true} when at least an update is performed, otherwise {@code false}.
     */
    private final ConcurrentHashMap<Long, Boolean> updatesMap = new ConcurrentHashMap<>();

    protected long totalOperations;

    // TODO: workaround for https://github.com/elastic/elasticsearch-java/pull/867 remove when fixed
    private final ScheduledExecutorService scheduler;

    private ElasticBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                        @NotNull String indexName,
                                        @NotNull ElasticIndexDefinition indexDefinition,
                                        @NotNull NodeBuilder definitionBuilder,
                                        boolean waitForESAcknowledgement) {
        this.elasticConnection = elasticConnection;
        this.indexName = indexName;
        this.indexDefinition = indexDefinition;
        this.definitionBuilder = definitionBuilder;
        this.waitForESAcknowledgement = waitForESAcknowledgement;
        // TODO: workaround for https://github.com/elastic/elasticsearch-java/pull/867 remove when fixed
        this.scheduler = Executors.newScheduledThreadPool(BULK_PROCESSOR_CONCURRENCY + 1, (r) -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("oak-bulk-ingester#");
            t.setDaemon(true);
            return t;
        });
        this.bulkIngester = initBulkIngester();
    }

    /**
     * Returns an ElasticBulkProcessorHandler instance based on the index definition configuration.
     * <p>
     * The `sync-mode` property can be set to `rt` (real-time). In this case the returned handler will be real-time.
     * This option is available for sync index definitions only.
     */
    public static ElasticBulkProcessorHandler getBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                                                      @NotNull String indexName,
                                                                      @NotNull ElasticIndexDefinition indexDefinition,
                                                                      @NotNull NodeBuilder definitionBuilder, CommitInfo commitInfo,
                                                                      boolean waitForESAcknowledgement) {
        PropertyState async = indexDefinition.getDefinitionNodeState().getProperty("async");

        if (async != null) {
            return new ElasticBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder, waitForESAcknowledgement);
        }

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

        if (SYNC_RT_MODE.equals(syncMode)) {
            return new RealTimeBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder, waitForESAcknowledgement);
        }

        return new ElasticBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder, waitForESAcknowledgement);
    }

    private BulkIngester<String> initBulkIngester() {
        // BulkIngester does not support retry policies. Some retries though are already implemented in the transport layer.
        // More details here: https://github.com/elastic/elasticsearch-java/issues/478
        return BulkIngester.of(b -> {
            b = b.client(elasticConnection.getAsyncClient())
                    .listener(new OakBulkListener());
            if (indexDefinition.bulkActions > 0) {
                b = b.maxOperations(indexDefinition.bulkActions);
            }
            if (indexDefinition.bulkSizeBytes > 0) {
                b = b.maxSize(indexDefinition.bulkSizeBytes);
            }
            if (indexDefinition.bulkFlushIntervalMs > 0) {
                b = b.flushInterval(indexDefinition.bulkFlushIntervalMs, TimeUnit.MILLISECONDS);
            }

            // TODO: workaround for https://github.com/elastic/elasticsearch-java/pull/867 remove when fixed
            b = b.scheduler(scheduler);

            return b.maxConcurrentRequests(BULK_PROCESSOR_CONCURRENCY);
        });
    }

    private void checkFailures() throws IOException {
        if (!suppressedErrorCauses.isEmpty()) {
            IOException ioe = new IOException("Exception while indexing. See suppressed for details");
            suppressedErrorCauses.stream().map(ec -> new IllegalStateException(ec.reason())).forEach(ioe::addSuppressed);
            throw ioe;
        }
    }

    /**
     * Indexes a document in the bulk processor. The document is identified by the given id. If the document already exists it will be replaced by the new one.
     * @param id the document id
     * @param document the document to index
     * @throws IOException if an error happened while processing the bulk request
     */
    public void index(String id, ElasticDocument document) throws IOException {
        add(BulkOperation.of(op -> op.index(idx -> idx.index(indexName).id(id).document(document))), id);
    }

    public void update(String id, ElasticDocument document) throws IOException {
        if (document.getPropertiesToRemove().isEmpty()) {
            add(BulkOperation.of(op ->
                    op.update(uf -> uf.index(indexName).id(id).action(uaf -> uaf.doc(document).docAsUpsert(true)))
            ), id);
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
            add(BulkOperation.of(op -> op.update(uf -> uf.index(indexName).id(id)
                    .action(uaf -> uaf.script(s -> s.source(script.toString()).params("document", JsonData.of(document)))
                            .upsert(document)))), id);
        }
    }

    public void delete(String id) throws IOException {
        add(BulkOperation.of(op -> op.delete(idx -> idx.index(indexName).id(id))), id);
    }

    private void add(BulkOperation operation, String context) throws IOException {
        // fail fast: we don't want to wait until the processor gets closed to fail
        checkFailures();
        bulkIngester.add(operation, context);
        totalOperations++;
    }

    /**
     * Closes the bulk ingester and waits for all the bulk requests to return.
     * @return {@code true} if at least one update was performed, {@code false} otherwise
     * @throws IOException if an error happened while processing the bulk requests
     */
    public boolean close() throws IOException {
        try {
            LOG.trace("Calling close on bulk ingester {}", bulkIngester);
            bulkIngester.close();
            LOG.trace("Bulk Ingester {} closed", bulkIngester);

            // de-register main controller
            int phase = phaser.arriveAndDeregister();

            if (totalOperations == 0) { // no need to invoke phaser await if we already know there were no operations
                LOG.debug("No operations executed in this processor. Close immediately");
                return false;
            }

            if (waitForESAcknowledgement) {
                try {
                    phaser.awaitAdvanceInterruptibly(phase, indexDefinition.bulkFlushIntervalMs * 5, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    LOG.error("Error waiting for bulk requests to return", e);
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for bulk processor to close", e);
                    Thread.currentThread().interrupt();  // restore interrupt status
                }
            }

            checkFailures();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk identifier -> update status = {}", updatesMap);
            }
            return updatesMap.containsValue(Boolean.TRUE);
        } finally {
            // TODO: workaround for https://github.com/elastic/elasticsearch-java/pull/867 remove when fixed
            new ExecutorCloser(scheduler).close();
        }
    }

    private class OakBulkListener implements BulkListener<String> {

        @Override
        public void beforeBulk(long executionId, BulkRequest request, List<String> contexts) {
            // register new bulk party
            phaser.register();

            // init update status
            updatesMap.put(executionId, Boolean.FALSE);

            LOG.debug("Sending bulk with id {} -> {}", executionId, contexts);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", request.operations()
                        .stream()
                        .map(BulkOperation::toString)
                        .collect(Collectors.joining("\n"))
                );
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List<String> contexts, BulkResponse response) {
            try {
                LOG.debug("Bulk with id {} processed in {} ms", executionId, response.took());
                if (LOG.isTraceEnabled()) {
                    LOG.trace(response.toString());
                }
                if (response.items().stream().anyMatch(i -> i.error() != null)) { // check if some operations failed to execute
                    Set<String> failedDocSet = new LinkedHashSet<>();
                    NodeBuilder status = definitionBuilder.child(IndexDefinition.STATUS_NODE);
                    // Read the current failed paths (if any) on the :status node into failedDocList
                    if (status.hasProperty(IndexDefinition.FAILED_DOC_PATHS)) {
                        for (String str : status.getProperty(IndexDefinition.FAILED_DOC_PATHS).getValue(Type.STRINGS)) {
                            failedDocSet.add(str);
                        }
                    }

                    int initialSize = failedDocSet.size();
                    boolean isFailedDocSetFull = false;

                    boolean hasSuccesses = false;
                    for (int i = 0; i < contexts.size(); i++) {
                        BulkResponseItem item = response.items().get(i);
                        if (item.error() != null) {
                            if (indexDefinition.failOnError) {
                                suppressedErrorCauses.add(item.error());
                            }
                            if (!isFailedDocSetFull && failedDocSet.size() < FAILED_DOC_COUNT_FOR_STATUS_NODE) {
                                failedDocSet.add(contexts.get(i));
                            } else {
                                isFailedDocSetFull = true;
                            }
                            // Log entry to be used to parse logs to get the failed doc id/path if needed
                            LOG.error("ElasticIndex Update Doc Failure: Error while adding/updating doc with id: [{}]", contexts.get(i));
                            LOG.error("Failure Details: BulkItem ID: {}, Index: {}, Failure Cause: {}",
                                    item.id(), item.index(), item.error());
                        } else if (!hasSuccesses) {
                            // Set indexUpdated to true even if 1 item was updated successfully
                            updatesMap.put(executionId, Boolean.TRUE);
                            hasSuccesses = true;
                        }
                    }

                    if (isFailedDocSetFull) {
                        LOG.info("Cannot store all new Failed Docs because {} has been filled up. " +
                                "See previous log entries to find out the details of failed paths", IndexDefinition.FAILED_DOC_PATHS);
                    } else if (failedDocSet.size() != initialSize) {
                        status.setProperty(IndexDefinition.FAILED_DOC_PATHS, failedDocSet, Type.STRINGS);
                    }
                } else {
                    updatesMap.put(executionId, Boolean.TRUE);
                }
            } finally {
                phaser.arriveAndDeregister();
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, List<String> contexts, Throwable failure) {
            try {
                LOG.error("ElasticIndex Update Bulk Failure : Bulk with id {} threw an error", executionId, failure);
                suppressedErrorCauses.add(ErrorCause.of(ec -> {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    failure.printStackTrace(pw);
                    return ec.reason(failure.getMessage()).stackTrace(sw.toString());
                }));
            } finally {
                phaser.arriveAndDeregister();
            }
        }
    }

    /**
     * {@link ElasticBulkProcessorHandler} extension with real time behaviour.
     * It also uses the same async bulk processor as the parent except for the last flush that waits until the
     * indexed documents are searchable.
     * <p>
     * BulkIngester does not support customization of intermediate requests. This means we cannot intercept the last
     * request and apply a WAIT_UNTIL refresh policy. The workaround is to force a refresh when the handler is closed.
     * We can improve this when this issue gets fixed:
     * <a href="https://github.com/elastic/elasticsearch-java/issues/703">elasticsearch-java#703</a>
     */
    protected static class RealTimeBulkProcessorHandler extends ElasticBulkProcessorHandler {

        private RealTimeBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                             @NotNull String indexName,
                                             @NotNull ElasticIndexDefinition indexDefinition,
                                             @NotNull NodeBuilder definitionBuilder,
                                             boolean waitForESAcknowledgement) {
            super(elasticConnection, indexName, indexDefinition, definitionBuilder, waitForESAcknowledgement);
        }

        @Override
        public boolean close() throws IOException {
            // calling super closes the bulk processor. If not empty it calls #requestConsumer for the last time
            boolean closed = super.close();
            // it could happen that close gets called when the bulk has already been flushed. In these cases we trigger
            // an actual refresh to make sure the docs are searchable before returning from the method
            if (totalOperations > 0) {
                LOG.debug("Forcing refresh");
                try {
                	this.elasticConnection.getClient().indices().refresh(b -> b.index(indexName));
                } catch (IOException e) {
                    LOG.warn("Error refreshing index {}", indexName, e);
                }
            }
            return closed;
        }
    }
}
