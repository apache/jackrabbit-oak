/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_RUNNING;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.failed;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.initiated;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.PROP_UNSAFE_FOR_ACTIVE_DELETION;

public class ActiveDeletedBlobCollectorMBeanImpl implements ActiveDeletedBlobCollectorMBean {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveDeletedBlobCollectorMBeanImpl.class);

    private static final String OP_NAME = "Active lucene index blobs collection";

    /**
     * Actively deleted blob must be deleted for at least this long (in seconds)
     */
    private final long MIN_BLOB_AGE_TO_ACTIVELY_DELETE = Long.getLong("oak.active.deletion.minAge",
            TimeUnit.HOURS.toSeconds(24));

    Clock clock = Clock.SIMPLE; // package private for tests

    @Nonnull
    private final ActiveDeletedBlobCollector activeDeletedBlobCollector;

    @Nonnull
    private Whiteboard whiteboard;

    @Nonnull
    private final GarbageCollectableBlobStore blobStore;

    @Nonnull
    private final Executor executor;

    private final NodeStore store;

    private final IndexPathService indexPathService;

    private final AsyncIndexInfoService asyncIndexInfoService;

    private ManagementOperation<Void> gcOp = done(OP_NAME, null);

    /**
     * @param activeDeletedBlobCollector    deleted index blobs collector
     * @param whiteboard                    An instance of {@link Whiteboard}. It will be
     *                                      used to get checkpoing manager mbean.
     * @param store                         {@link NodeStore} instance to access repository state
     * @param indexPathService              {@link IndexPathService} instance to collect indexes available in
     *                                                              the repository
     * @param asyncIndexInfoService         {@link AsyncIndexInfoService} instance to acess state of async
     *                                                                   indexer lanes
     * @param blobStore                     An instance of {@link GarbageCollectableBlobStore}. It will be
     *                                      used to purge blobs which have been deleted from lucene indexes.
     * @param executor                      executor for running the collection task
     */
    ActiveDeletedBlobCollectorMBeanImpl(
            @Nonnull ActiveDeletedBlobCollector activeDeletedBlobCollector,
            @Nonnull Whiteboard whiteboard,
            @Nonnull NodeStore store,
            @Nonnull IndexPathService indexPathService,
            @Nonnull AsyncIndexInfoService asyncIndexInfoService,
            @Nonnull GarbageCollectableBlobStore blobStore,
            @Nonnull Executor executor) {
        this.activeDeletedBlobCollector = checkNotNull(activeDeletedBlobCollector);
        this.whiteboard = checkNotNull(whiteboard);
        this.store = store;
        this.indexPathService = indexPathService;
        this.asyncIndexInfoService = asyncIndexInfoService;
        this.blobStore = checkNotNull(blobStore);
        this.executor = checkNotNull(executor);

        LOG.info("Active blob collector initialized with minAge: {}", MIN_BLOB_AGE_TO_ACTIVELY_DELETE);
    }

    @Nonnull
    @Override
    public CompositeData startActiveCollection() {
        if (gcOp.isDone()) {
            long safeTimestampForDeletedBlobs = getSafeTimestampForDeletedBlobs();
            if (safeTimestampForDeletedBlobs == -1) {
                return failed(OP_NAME + " couldn't be run as a safe timestamp for" +
                        " purging lucene index blobs couldn't be evaluated").toCompositeData();
            }
            gcOp = newManagementOperation(OP_NAME, () -> {
                activeDeletedBlobCollector.purgeBlobsDeleted(safeTimestampForDeletedBlobs, blobStore);
                return null;
            });
            executor.execute(gcOp);
            return initiated(gcOp, OP_NAME + " started").toCompositeData();
        } else {
            return failed(OP_NAME + " already running").toCompositeData();
        }
    }

    @Nonnull
    @Override
    public CompositeData cancelActiveCollection() {
        if (!gcOp.isDone()) {
            executor.execute(newManagementOperation(OP_NAME, (Callable<Void>) () -> {
                gcOp.cancel(false);
                activeDeletedBlobCollector.cancelBlobCollection();
                return null;
            }));
            return initiated(gcOp, "Active lucene index blobs collection cancelled").toCompositeData();
        } else {
            return failed(OP_NAME + " not running").toCompositeData();
        }
    }

    @Nonnull
    @Override
    public CompositeData getActiveCollectionStatus() {
        return gcOp.getStatus().toCompositeData();
    }

    @Override
    public boolean isActiveDeletionUnsafe() {
        return activeDeletedBlobCollector.isActiveDeletionUnsafe();
    }

    @Override
    public void flagActiveDeletionUnsafeForCurrentState() {
        activeDeletedBlobCollector.flagActiveDeletionUnsafe(true);

        if (!waitForRunningIndexCycles()) {
            LOG.warn("Some indexers were still found running. Resume and quit gracefully");
            activeDeletedBlobCollector.flagActiveDeletionUnsafe(false);
        }

        try {
            markCurrentIndexFilesUnsafeForActiveDeletion();
        } catch (CommitFailedException e) {
            LOG.warn("Could not set current index files unsafe for active deletion. Resume and quit gracefully", e);
            activeDeletedBlobCollector.flagActiveDeletionUnsafe(false);
        }
    }

    @Override
    public void flagActiveDeletionSafe() {
        activeDeletedBlobCollector.flagActiveDeletionUnsafe(false);
    }

    /**
     * Wait for running index cycles for 2 minutes.
     *
     * @return true if all running index cycles have been through; false otherwise
     */
    private boolean waitForRunningIndexCycles() {
        Map<IndexStatsMBean, Long> origIndexLaneToExecutinoCountMap = Maps.asMap(
                Sets.newHashSet(StreamSupport.stream(asyncIndexInfoService.getAsyncLanes().spliterator(), false)
                        .map(lane -> asyncIndexInfoService.getInfo(lane).getStatsMBean())
                        .filter(bean -> {
                            String beanStatus;
                            try {
                                if (bean != null) {
                                    beanStatus = bean.getStatus();
                                } else {
                                    return false;
                                }
                            } catch (Exception e) {
                                LOG.warn("Exception during getting status for {}. Ignoring this indexer lane", bean.getName(), e);
                                return false;
                            }
                            return STATUS_RUNNING.equals(beanStatus);
                        })
                        .collect(Collectors.toList())),
                IndexStatsMBean::getTotalExecutionCount);

        if (!origIndexLaneToExecutinoCountMap.isEmpty()) {
            LOG.info("Found running index lanes ({}). Sleep a bit before continuing.",
                    transform(origIndexLaneToExecutinoCountMap.keySet(), IndexStatsMBean::getName));
            try {
                clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(1));
            } catch (InterruptedException e) {
                LOG.info("Thread interrupted during initial wait", e);
                Thread.currentThread().interrupt();
            }
        }

        long start = clock.getTime();
        while (!origIndexLaneToExecutinoCountMap.isEmpty()) {
            Map.Entry<IndexStatsMBean, Long> indexLaneEntry = origIndexLaneToExecutinoCountMap.entrySet().iterator().next();
            IndexStatsMBean indexLaneBean = indexLaneEntry.getKey();

            long oldExecCnt = indexLaneEntry.getValue();
            long newExecCnt = indexLaneBean.getTotalExecutionCount();
            String beanStatus = indexLaneBean.getStatus();

            if (!STATUS_RUNNING.equals(beanStatus) || oldExecCnt != newExecCnt) {
                origIndexLaneToExecutinoCountMap.remove(indexLaneBean);
                LOG.info("Lane {} has moved - oldExecCnt {}, newExecCnt {}", indexLaneBean.getName(), oldExecCnt, newExecCnt);
            } else if (clock.getTime() - start > TimeUnit.MINUTES.toMillis(2)) {
                LOG.warn("Timed out while waiting for running index lane executions");
                break;
            } else {
                LOG.info("Lane {} still has execution count {}. Waiting....", indexLaneBean.getName(), newExecCnt);

                try {
                    clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(1));
                } catch (InterruptedException e) {
                    LOG.info("Thread interrupted", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return origIndexLaneToExecutinoCountMap.isEmpty();
    }

    private void markCurrentIndexFilesUnsafeForActiveDeletion() throws CommitFailedException {
        NodeBuilder rootBuilder = store.getRoot().builder();
        for (String indexPath : indexPathService.getIndexPaths()) {
            markCurrentIndexFilesUnsafeForActiveDeletionFor(rootBuilder, indexPath);
        }

        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void markCurrentIndexFilesUnsafeForActiveDeletionFor(NodeBuilder rootBuilder, String indexPath) {
        NodeBuilder indexPathBuilder = getBuilderForPath(rootBuilder, indexPath);
        if (!TYPE_LUCENE.equals(indexPathBuilder.getProperty(TYPE_PROPERTY_NAME).getValue(STRING))) {
            LOG.debug("Ignoring index {} as it's not a lucene index", indexPath);
            return;
        }

        NodeBuilder dataNodeBuilder = indexPathBuilder.getChildNode(INDEX_DATA_CHILD_NAME);
        for (String indexFileName : dataNodeBuilder.getChildNodeNames()) {
            NodeBuilder indexFileBuilder = dataNodeBuilder.getChildNode(indexFileName);

            indexFileBuilder.setProperty(PROP_UNSAFE_FOR_ACTIVE_DELETION, true);
        }
    }

    private static NodeBuilder getBuilderForPath(NodeBuilder rootBuilder, String path) {
        NodeBuilder builder = rootBuilder;
        for (String elem : PathUtils.elements(path)) {
            builder = builder.getChildNode(elem);
        }
        return builder;
    }

    private long getSafeTimestampForDeletedBlobs() {
        long timestamp = clock.getTime() - TimeUnit.SECONDS.toMillis(MIN_BLOB_AGE_TO_ACTIVELY_DELETE);

        long minCheckpointTimestamp = getOldestCheckpointCreationTimestamp();

        if (minCheckpointTimestamp == -1) {
            return minCheckpointTimestamp;
        }

        if (minCheckpointTimestamp < timestamp) {
            LOG.info("Oldest checkpoint timestamp ({}) is older than buffer period ({}) for deleted blobs." +
                    " Using that instead", minCheckpointTimestamp, timestamp);
            timestamp = minCheckpointTimestamp;
        }

        return timestamp;
    }

    private long getOldestCheckpointCreationTimestamp() {
        Tracker<CheckpointMBean> tracker = whiteboard.track(CheckpointMBean.class);

        try {
            List<CheckpointMBean> services = tracker.getServices();
            if (services.size() == 1) {
                return services.get(0).getOldestCheckpointCreationTimestamp();
            } else if (services.isEmpty()) {
                LOG.warn("Unable to get checkpoint mbean. No service of required type found.");
                return -1;
            } else {
                LOG.warn("Unable to get checkpoint mbean. Multiple services of required type found.");
                return -1;
            }
        } finally {
            tracker.stop();
        }
    }
}
