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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;

import static org.apache.jackrabbit.oak.api.Type.STRING;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.segment.scheduler.Commit;
import org.apache.jackrabbit.oak.segment.scheduler.LockBasedScheduler;
import org.apache.jackrabbit.oak.segment.scheduler.Scheduler;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.ConflictAnnotatingRebaseDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

        @NotNull
        private final Revisions revisions;

        @NotNull
        private final SegmentReader reader;

        @NotNull
        private final SegmentWriter writer;

        @Nullable
        private final BlobStore blobStore;

        private boolean isCreated;

        private boolean dispatchChanges = true;

        @NotNull
        private StatisticsProvider statsProvider = StatisticsProvider.NOOP;

        private LoggingHook loggingHook;

        private SegmentNodeStoreBuilder(
                @NotNull Revisions revisions,
                @NotNull SegmentReader reader,
                @NotNull SegmentWriter writer,
                @Nullable BlobStore blobStore) {
            this.revisions = revisions;
            this.reader = reader;
            this.writer = writer;
            this.blobStore = blobStore;
        }


        @NotNull
        public SegmentNodeStoreBuilder dispatchChanges(boolean dispatchChanges) {
            this.dispatchChanges = dispatchChanges;
            return this;
        }

        /**
         * {@link StatisticsProvider} for collecting statistics related to SegmentStore
         * @param statisticsProvider
         * @return this instance
         */
        @NotNull
        public SegmentNodeStoreBuilder withStatisticsProvider(@NotNull StatisticsProvider statisticsProvider) {
            this.statsProvider = requireNonNull(statisticsProvider);
            return this;
        }

        /**
         * {@link LoggingHook} for recording write operations to a log file
         *
         * @return this instance
         */
        @NotNull
        public SegmentNodeStoreBuilder withLoggingHook(Consumer<String> writer) {
            this.loggingHook = LoggingHook.newLoggingHook(writer);
            return this;
        }

        @NotNull
        public SegmentNodeStore build() {
            Validate.checkState(!isCreated);
            isCreated = true;
            LOG.info("Creating segment node store {}", this);
            return new SegmentNodeStore(this);
        }

        @NotNull
        private static String getString(@Nullable BlobStore blobStore) {
            return "blobStore=" + (blobStore == null ? "inline" : blobStore);
        }

        @Override
        public String toString() {
            return "SegmentNodeStoreBuilder{" +
                    getString(blobStore) +
                    '}';
        }
    }

    @NotNull
    public static SegmentNodeStoreBuilder builder(
            @NotNull Revisions revisions,
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore) {
        return new SegmentNodeStoreBuilder(requireNonNull(revisions),
                requireNonNull(reader), requireNonNull(writer), blobStore);
    }

    static final String ROOT = "root";

    public static final String CHECKPOINTS = "checkpoints";

    @NotNull
    private final SegmentWriter writer;

    @NotNull
    private final Scheduler scheduler;

    @Nullable
    private final BlobStore blobStore;

    private final SegmentNodeStoreStats stats;

    private final LoggingHook loggingHook;

    private SegmentNodeStore(SegmentNodeStoreBuilder builder) {
        this.writer = builder.writer;
        this.blobStore = builder.blobStore;
        this.stats = new SegmentNodeStoreStats(builder.statsProvider);
        this.scheduler = LockBasedScheduler.builder(builder.revisions, builder.reader, stats)
                .dispatchChanges(builder.dispatchChanges)
                .build();
        this.loggingHook = builder.loggingHook;
    }

    @Override
    public Closeable addObserver(Observer observer) {
        if (scheduler instanceof Observable) {
            return ((Observable) scheduler).addObserver(observer);
        }

        return () -> {};
    }

    @Override @NotNull
    public NodeState getRoot() {
        return scheduler.getHeadNodeState().getChildNode(ROOT);
    }

    @NotNull
    @Override
    public NodeState merge(
            @NotNull NodeBuilder builder, @NotNull CommitHook commitHook,
            @NotNull CommitInfo info) throws CommitFailedException {
        checkArgument(builder instanceof SegmentNodeBuilder);
        checkArgument(((SegmentNodeBuilder) builder).isRootBuilder());
        if (loggingHook != null) {
            commitHook = new CompositeHook(commitHook, loggingHook);
        }
        return scheduler.schedule(new Commit(builder, commitHook, info));
    }

    @Override @NotNull
    public NodeState rebase(@NotNull NodeBuilder builder) {
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

    @Override @NotNull
    public NodeState reset(@NotNull NodeBuilder builder) {
        checkArgument(builder instanceof SegmentNodeBuilder);

        SegmentNodeBuilder snb = (SegmentNodeBuilder) builder;

        NodeState root = getRoot();
        snb.reset(root);

        return root;
    }

    @NotNull
    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return new SegmentBlob(blobStore, writer.writeStream(stream));
    }

    @Override
    public Blob getBlob(@NotNull String reference) {
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

    @NotNull
    @Override
    public String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        return scheduler.checkpoint(lifetime, properties);
    }

    @Override @NotNull
    public synchronized String checkpoint(long lifetime) {
        return checkpoint(lifetime, Collections.<String, String>emptyMap());
    }

    @NotNull
    @Override
    public Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        Map<String, String> properties = new HashMap<>();
        requireNonNull(checkpoint);
        NodeState cp = scheduler.getHeadNodeState()
                .getChildNode("checkpoints")
                .getChildNode(checkpoint)
                .getChildNode("properties");

        for (PropertyState prop : cp.getProperties()) {
            properties.put(prop.getName(), prop.getValue(STRING));
        }

        return properties;
    }

    @NotNull
    @Override
    public Iterable<String> checkpoints() {
        return getCheckpoints().getChildNodeNames();
    }

    @Override @Nullable
    public NodeState retrieve(@NotNull String checkpoint) {
        requireNonNull(checkpoint);
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
    public boolean release(@NotNull String checkpoint) {
        return scheduler.removeCheckpoint(checkpoint);
    }

    NodeState getCheckpoints() {
        return scheduler.getHeadNodeState().getChildNode(CHECKPOINTS);
    }

    public SegmentNodeStoreStats getStats() {
        return stats;
    }
}
