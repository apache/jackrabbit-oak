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
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.segment.scheduler.Commit;
import org.apache.jackrabbit.oak.segment.scheduler.LockBasedScheduler;
import org.apache.jackrabbit.oak.segment.scheduler.Scheduler;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The top level class for the segment store.
 * <p>
 * The root node of the JCR content tree is actually stored in the node "/root",
 * and checkpoints are stored under "/checkpoints".
 */
public class SegmentNodeStore implements NodeStore, Observable {

    public static class SegmentNodeStoreBuilder {
        private static final Logger LOG = LoggerFactory.getLogger(SegmentNodeStoreBuilder.class);

        @Nonnull
        private final Revisions revisions;

        @Nonnull
        private final SegmentReader reader;

        @Nonnull
        private final SegmentWriter writer;

        @CheckForNull
        private final BlobStore blobStore;
        
        private boolean isCreated;
        
        private boolean dispatchChanges = true;

        @Nonnull
        private StatisticsProvider statsProvider = StatisticsProvider.NOOP;
        
        private SegmentNodeStoreBuilder(
                @Nonnull Revisions revisions,
                @Nonnull SegmentReader reader,
                @Nonnull SegmentWriter writer,
                @Nullable BlobStore blobStore) {
            this.revisions = revisions;
            this.reader = reader;
            this.writer = writer;
            this.blobStore = blobStore;
        }

        
        @Nonnull
        public SegmentNodeStoreBuilder dispatchChanges(boolean dispatchChanges) {
            this.dispatchChanges = dispatchChanges;
            return this;
        }
        
        /**
         * {@link StatisticsProvider} for collecting statistics related to SegmentStore
         * @param statisticsProvider
         * @return this instance
         */
        @Nonnull
        public SegmentNodeStoreBuilder withStatisticsProvider(@Nonnull StatisticsProvider statisticsProvider) {
            this.statsProvider = checkNotNull(statisticsProvider);
            return this;
        }
        
        @Nonnull
        public SegmentNodeStore build() {
            checkState(!isCreated);
            isCreated = true;
            LOG.info("Creating segment node store {}", this);
            return new SegmentNodeStore(this);
        }

        @Nonnull
        private static String getString(@CheckForNull BlobStore blobStore) {
            return "blobStore=" + (blobStore == null ? "inline" : blobStore);
        }
        
        @Override
        public String toString() {
            return "SegmentNodeStoreBuilder{" +
                    getString(blobStore) +
                    '}';
        }
    }

    @Nonnull
    public static SegmentNodeStoreBuilder builder(
            @Nonnull Revisions revisions,
            @Nonnull SegmentReader reader,
            @Nonnull SegmentWriter writer,
            @Nullable BlobStore blobStore) {
        return new SegmentNodeStoreBuilder(checkNotNull(revisions),
                checkNotNull(reader), checkNotNull(writer), blobStore);
    }

    static final String ROOT = "root";

    public static final String CHECKPOINTS = "checkpoints";

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final SegmentWriter writer;

    @Nonnull
    private final Scheduler scheduler;

    @CheckForNull
    private final BlobStore blobStore;
    
    private final SegmentNodeStoreStats stats;

    private SegmentNodeStore(SegmentNodeStoreBuilder builder) {
        this.reader = builder.reader;
        this.writer = builder.writer;
        this.blobStore = builder.blobStore;
        
        this.scheduler = LockBasedScheduler.builder(builder.revisions, builder.reader)
                .dispatchChanges(builder.dispatchChanges)
                .withStatisticsProvider(builder.statsProvider)
                .build();
        
        this.stats = new SegmentNodeStoreStats(builder.statsProvider);
    }

    @Override
    public Closeable addObserver(Observer observer) {
        return scheduler.addObserver(observer);
    }

    @Override @Nonnull
    public NodeState getRoot() {
        return scheduler.getHeadNodeState().getChildNode(ROOT);
    }

    @Nonnull
    @Override
    public NodeState merge(
            @Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
            @Nonnull CommitInfo info) throws CommitFailedException {
        return scheduler.schedule(new Commit(builder, commitHook, info));
    }

    @Override @Nonnull
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        NodeState before = snb.getBaseState();
        if (!SegmentNodeState.fastEquals(before, root)) {
            SegmentNodeState after = snb.getNodeState();
            snb.reset(root);
            after.compareAgainstBaseState(
                    before, new ConflictAnnotatingRebaseDiff(snb));
        }

        return snb.getNodeState();
    }

    @Override @Nonnull
    public NodeState reset(@Nonnull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @Nonnull
    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return writer.writeStream(stream);
    }

    @Override
    public Blob getBlob(@Nonnull String reference) {
        //Use of 'reference' here is bit overloaded. In terms of NodeStore API
        //a blob reference refers to the secure reference obtained from Blob#getReference()
        //However in SegmentStore terminology a blob is referred via 'external reference'
        //That 'external reference' would map to blobId obtained from BlobStore#getBlobId
        if (blobStore != null) {
            String blobId = blobStore.getBlobId(reference);
            if (blobId != null) {
                return new BlobStoreBlob(blobStore, blobId);
            }
            return null;
        }
        throw new IllegalStateException("Attempt to read external blob with blobId [" + reference + "] " +
                "without specifying BlobStore");
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return scheduler.checkpoint(lifetime, properties);
    }

    @Override @Nonnull
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        Map<String, String> properties = newHashMap();
        checkNotNull(checkpoint);
        NodeState cp = scheduler.getHeadNodeState()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode("properties");

        for (PropertyState prop : cp.getProperties()) {
            properties.put(prop.getName(), prop.getValue(STRING));
        }

        return properties;
    }

    @Nonnull
    @Override
    public Iterable<String> checkpoints() {
        return getCheckpoints().getChildNodeNames();
    }

    @Override @CheckForNull
    public NodeState retrieve(@Nonnull String checkpoint) {
        checkNotNull(checkpoint);
        NodeState cp = scheduler.getHeadNodeState()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode(ROOT);
        if (cp.exists()) {
            return cp;
        }
        return null;
    }

    @Override
    public boolean release(@Nonnull String checkpoint) {
        return scheduler.removeCheckpoint(checkpoint);
    }

    NodeState getCheckpoints() {
        return scheduler.getHeadNodeState().getChildNode(CHECKPOINTS);
    }
    
    public SegmentNodeStoreStats getStats() {
        return stats;
    }
}
