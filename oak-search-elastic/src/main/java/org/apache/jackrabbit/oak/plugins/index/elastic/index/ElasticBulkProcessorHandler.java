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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.importer.AsyncLaneSwitcher;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

class ElasticBulkProcessorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticBulkProcessorHandler.class);
    private final int FAILED_DOC_COUNT_FOR_STATUS_NODE = Integer.getInteger("oak.failedDocStatusLimit", 10000);

    private static final int BULK_PROCESSOR_CONCURRENCY =
        Integer.getInteger("oak.indexer.elastic.bulkProcessorConcurrency", 1);
    private static final String SYNC_MODE_PROPERTY = "sync-mode";
    private static final String SYNC_RT_MODE = "rt";
    private static boolean waitForESAcknowledgement = true;

    protected final ElasticConnection elasticConnection;
    protected final String indexName;
    protected final ElasticIndexDefinition indexDefinition;
    private final NodeBuilder definitionBuilder;
    protected final BulkProcessor bulkProcessor;

    /**
     * Coordinates communication between bulk processes. It has a main controller registered at creation time and
     * de-registered on {@link ElasticIndexWriter#close(long)}. Each bulk request register a new party in
     * this Phaser in {@link OakBulkProcessorListener#beforeBulk(long, BulkRequest)} and de-register itself when
     * the request returns.
     */
    private final Phaser phaser = new Phaser(1); // register main controller

    /**
     * IOException object wrapping any error/exception which occurred while trying to update index in elasticsearch.
     */
    private volatile IOException ioException;

    /**
     * Key-value structure to keep the history of bulk requests. Keys are the bulk execution ids, the boolean
     * value is {@code true} when at least an update is performed, otherwise {@code false}.
     */
    private final ConcurrentHashMap<Long, Boolean> updatesMap = new ConcurrentHashMap<>();

    protected long totalOperations;

    private ElasticBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                        @NotNull String indexName,
                                        @NotNull ElasticIndexDefinition indexDefinition,
                                        @NotNull NodeBuilder definitionBuilder) {
        this.elasticConnection = elasticConnection;
        this.indexName = indexName;
        this.indexDefinition = indexDefinition;
        this.definitionBuilder = definitionBuilder;
        this.bulkProcessor = initBulkProcessor();
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
                                                                      @NotNull NodeBuilder definitionBuilder, CommitInfo commitInfo) {
        PropertyState async = indexDefinition.getDefinitionNodeState().getProperty("async");

        if (async != null) {
            // Check if this indexing call is a part of async cycle or a commit hook or called from oak-run for offline reindex
            // In case it's from async cycle - commit info will have a indexingCheckpointTime key.
            // Otherwise, it's part of commit hook based indexing due to async property having a value nrt
            // If the IndexDefinition has a property async-previous set, this implies it's being called from oak-run for offline-reindex.
            // we need to set waitForESAcknowledgement = false only in the second case i.e.
            // when this is a part of commit hook due to async property having a value nrt
            if (!(commitInfo.getInfo().containsKey(IndexConstants.CHECKPOINT_CREATION_TIME) || AsyncLaneSwitcher.isLaneSwitched(definitionBuilder))) {
                waitForESAcknowledgement = false;
            }
            return new ElasticBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder);
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
            return new RealTimeBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder);
        }

        return new ElasticBulkProcessorHandler(elasticConnection, indexName, indexDefinition, definitionBuilder);
    }

    private BulkProcessor initBulkProcessor() {
        return BulkProcessor.builder(requestConsumer(),
                new OakBulkProcessorListener(), this.indexName + "-bulk-processor")
                .setBulkActions(indexDefinition.bulkActions)
                .setConcurrentRequests(BULK_PROCESSOR_CONCURRENCY)
                .setBulkSize(new ByteSizeValue(indexDefinition.bulkSizeBytes))
                .setFlushInterval(TimeValue.timeValueMillis(indexDefinition.bulkFlushIntervalMs))
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
                        TimeValue.timeValueMillis(indexDefinition.bulkRetriesBackoff), indexDefinition.bulkRetries)
                )
                .build();
    }

    protected BiConsumer<BulkRequest, ActionListener<BulkResponse>> requestConsumer() {
        // TODO: migrate to ES Java client https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/indexing-bulk.html
        return (request, bulkListener) -> elasticConnection.getOldClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
    }

    public void add(DocWriteRequest<?> request) {
        bulkProcessor.add(request);
        totalOperations++;
    }

    public boolean close() throws IOException {
        LOG.trace("Calling close on bulk processor {}", bulkProcessor);
        bulkProcessor.close();
        LOG.trace("Bulk Processor {} closed", bulkProcessor);

        // de-register main controller
        int phase = phaser.arriveAndDeregister();

        if (totalOperations == 0) { // no need to invoke phaser await if we already know there were no operations
            LOG.debug("No operations executed in this processor. Close immediately");
            return false;
        }

        if (waitForESAcknowledgement) {
            try {
                phaser.awaitAdvanceInterruptibly(phase, indexDefinition.bulkFlushIntervalMs * 5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | TimeoutException e) {
                LOG.error("Error waiting for bulk requests to return", e);
            }
        }

        if (ioException != null) {
            throw ioException;
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Bulk identifier -> update status = {}", updatesMap);
        }
        return updatesMap.containsValue(Boolean.TRUE);
    }

    private class OakBulkProcessorListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest bulkRequest) {
            // register new bulk party
            phaser.register();

            // init update status
            updatesMap.put(executionId, Boolean.FALSE);

            bulkRequest.timeout(TimeValue.timeValueMinutes(2));

            LOG.debug("Sending bulk with id {} -> {}", executionId, bulkRequest.getDescription());
            if (LOG.isTraceEnabled()) {
                LOG.trace("Bulk Requests: \n{}", bulkRequest.requests()
                        .stream()
                        .map(DocWriteRequest::toString)
                        .collect(Collectors.joining("\n"))
                );
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
            LOG.debug("Bulk with id {} processed with status {} in {}", executionId, bulkResponse.status(), bulkResponse.getTook());
            if (LOG.isTraceEnabled()) {
                try {
                    LOG.trace(Strings.toString(bulkResponse.toXContent(jsonBuilder(), EMPTY_PARAMS)));
                } catch (IOException e) {
                    LOG.error("Error decoding bulk response", e);
                }
            }
            if (bulkResponse.hasFailures()) { // check if some operations failed to execute
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
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                        if (!isFailedDocSetFull && failedDocSet.size() < FAILED_DOC_COUNT_FOR_STATUS_NODE) {
                            failedDocSet.add(bulkItemResponse.getId());
                        } else {
                            isFailedDocSetFull = true;
                        }
                        // Log entry to be used to parse logs to get the failed doc id/path if needed
                        LOG.error("ElasticIndex Update Doc Failure: Error while adding/updating doc with id : [{}]", bulkItemResponse.getId());
                        LOG.error("Failure Details: BulkItem ID: " + failure.getId() + ", Failure Cause: {}", failure.getCause());
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
            phaser.arriveAndDeregister();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
            LOG.error("ElasticIndex Update Bulk Failure : Bulk with id {} threw an error", executionId, throwable);
            ElasticBulkProcessorHandler.this.ioException = new IOException(throwable);
            phaser.arriveAndDeregister();
        }
    }

    /**
     * {@link ElasticBulkProcessorHandler} extension with real time behaviour.
     * It also uses the same async bulk processor as the parent except for the last flush that waits until the
     * indexed documents are searchable.
     */
    protected static class RealTimeBulkProcessorHandler extends ElasticBulkProcessorHandler {

        private final AtomicBoolean isClosed = new AtomicBoolean(false);
        private final AtomicBoolean isDataSearchable = new AtomicBoolean(false);

        private RealTimeBulkProcessorHandler(@NotNull ElasticConnection elasticConnection,
                                             @NotNull String indexName,
                                             @NotNull ElasticIndexDefinition indexDefinition,
                                             @NotNull NodeBuilder definitionBuilder) {
            super(elasticConnection, indexName, indexDefinition, definitionBuilder);
        }

        @Override
        protected BiConsumer<BulkRequest, ActionListener<BulkResponse>> requestConsumer() {
            return (request, bulkListener) -> {
                if (isClosed.get()) {
                    LOG.debug("Processor is closing. Next request with {} actions will block until the data is searchable",
                            request.requests().size());
                    request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
                    isDataSearchable.set(true);
                }
                elasticConnection.getOldClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            };
        }

        @Override
        public boolean close() throws IOException {
            isClosed.set(true);
            // calling super closes the bulk processor. If not empty it calls #requestConsumer for the last time
            boolean closed = super.close();
            // it could happen that close gets called when the bulk has already been flushed. In these cases we trigger
            // an actual refresh to make sure the docs are searchable before returning from the method
            if (totalOperations > 0 && !isDataSearchable.get()) {
                LOG.debug("Forcing refresh");
                try {
                	this.elasticConnection.getClient().indices().refresh(b -> b.index(indexName));
                } catch (IOException e) {
                    LOG.warn("Error refreshing index " + indexName, e);
                }
            }
            return closed;
        }
    }
}
