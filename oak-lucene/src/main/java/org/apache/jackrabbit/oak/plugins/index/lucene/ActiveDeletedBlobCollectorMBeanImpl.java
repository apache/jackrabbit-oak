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

import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.ActiveDeletedBlobCollectorFactory.ActiveDeletedBlobCollector;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.failed;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.initiated;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;

public class ActiveDeletedBlobCollectorMBeanImpl implements ActiveDeletedBlobCollectorMBean {
    private static final Logger LOG = LoggerFactory.getLogger(ActiveDeletedBlobCollectorMBeanImpl.class);

    public static final String OP_NAME = "Active lucene index blobs collection";

    /**
     * Actively deleted blob must be deleted for at least this long (in seconds)
     */
    private final long MIN_BLOB_AGE_TO_ACTIVELY_DELETE = Long.getLong("oak.active.deletion.minAge",
            TimeUnit.HOURS.toSeconds(24));

    private final Clock clock = Clock.SIMPLE;

    @Nonnull
    private final ActiveDeletedBlobCollector activeDeletedBlobCollector;

    @Nonnull
    private Whiteboard whiteboard;

    @Nonnull
    private final GarbageCollectableBlobStore blobStore;

    @Nonnull
    private final Executor executor;


    private ManagementOperation<Void> gcOp = done(OP_NAME, null);

    /**
     * @param activeDeletedBlobCollector    deleted index blobs collector
     * @param executor                      executor for running the collection task
     */
    public ActiveDeletedBlobCollectorMBeanImpl(
            @Nonnull ActiveDeletedBlobCollector activeDeletedBlobCollector,
            @Nonnull Whiteboard whiteboard,
            @Nonnull GarbageCollectableBlobStore blobStore,
            @Nonnull Executor executor) {
        this.activeDeletedBlobCollector = checkNotNull(activeDeletedBlobCollector);
        this.whiteboard = checkNotNull(whiteboard);
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
