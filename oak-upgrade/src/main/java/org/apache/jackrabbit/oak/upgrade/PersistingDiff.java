/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class PersistingDiff implements NodeStateDiff {

    private static final Logger LOG = LoggerFactory.getLogger(PersistingDiff.class);

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the underlying segments.
     */
    private static final int UPDATE_LIMIT =
            Integer.getInteger("upgrade.update.limit", 10000);

    private final SegmentWriter writer;

    private final SegmentReader reader;

    private final BlobStore blobStore;

    private final PersistingDiff parent;

    private final String nodeName;

    private final Reporter reporter;

    @Nonnull
    private MemoryNodeBuilder builder;

    @Nonnull
    private final NodeState base;

    @CheckForNull
    private IOException exception;

    private long modCount;

    private PersistingDiff(PersistingDiff parent, String nodeName, @Nonnull NodeState base) {
        this.writer = parent.writer;
        this.reader = parent.reader;
        this.blobStore = parent.blobStore;
        this.reporter = parent.reporter;
        this.builder = new MemoryNodeBuilder(checkNotNull(base));
        this.parent = parent;
        this.base = base;
        this.nodeName = nodeName;
    }

    private PersistingDiff(SegmentWriter writer, SegmentReader reader, BlobStore blobStore, @Nonnull NodeState base) {
        this.writer = writer;
        this.reader = reader;
        this.blobStore = blobStore;
        this.reporter = new Reporter();
        this.builder = new MemoryNodeBuilder(checkNotNull(base));
        this.parent = null;
        this.base = base;
        this.nodeName = null;
    }

    public static SegmentNodeState applyDiffOnNodeState(
            FileStore fileStore,
            @Nonnull NodeState before,
            @Nonnull NodeState after,
            @Nonnull NodeState onto) throws IOException {
        return new PersistingDiff(fileStore.getWriter(), fileStore.getReader(), fileStore.getBlobStore(), onto).diff(before, after);
    }

    private void updated() throws IOException {
        if (modCount % UPDATE_LIMIT == 0) {
            RecordId newBaseId = writer.writeNode(builder.getNodeState(), null);
            SegmentNodeState newBase = new SegmentNodeState(reader, writer, blobStore, newBaseId);
            builder = new MemoryNodeBuilder(newBase);
        }
        modCount++;
    }

    private String getPath() {
        List<String> segments = new ArrayList<>();
        PersistingDiff currentDiff = this;
        while (currentDiff != null) {
            if (currentDiff.nodeName != null) {
                segments.add(currentDiff.nodeName);
            }
            currentDiff = currentDiff.parent;
        }
        segments = Lists.reverse(segments);

        StringBuilder path = new StringBuilder();
        for (String segment : segments) {
            path.append("/");
            path.append(segment);
        }
        return path.toString();
    }

    @CheckForNull
    SegmentNodeState diff(@Nonnull NodeState before, @Nonnull NodeState after) throws IOException {
        boolean success = after.compareAgainstBaseState(before, this);
        if (exception != null) {
            throw new IOException(exception);
        } else if (success) {
            NodeState nodeState = builder.getNodeState();
            checkState(modCount == 0 || !(nodeState instanceof SegmentNodeState));
            RecordId nodeId = writer.writeNode(nodeState, getStableIdBytes(after));
            reporter.reportNode(this::getPath);
            return new SegmentNodeState(reader, writer, blobStore, nodeId);
        } else {
            return null;
        }
    }

    @Override
    public boolean propertyAdded(@Nonnull PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyChanged(@Nonnull PropertyState before, @Nonnull PropertyState after) {
        builder.setProperty(after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        builder.removeProperty(before.getName());
        return true;
    }

    @Override
    public boolean childNodeAdded(@Nonnull String name, @Nonnull NodeState after) {
        try {
            SegmentNodeState segmentNodeState = new PersistingDiff(this, name, EMPTY_NODE).diff(EMPTY_NODE, after);
            if (segmentNodeState != null) {
                updated();
                builder.setChildNode(name, segmentNodeState);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeChanged(@Nonnull String name, @Nonnull NodeState before, @Nonnull NodeState after) {
        try {
            SegmentNodeState compacted = new PersistingDiff(this, name, base.getChildNode(name)).diff(before, after);
            if (compacted != null) {
                updated();
                builder.setChildNode(name, compacted);
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            exception = e;
            return false;
        }
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        try {
            updated();
            builder.getChildNode(name).remove();
            return true;
        } catch (IOException e) {
            exception = e;
            return false;
        }
    }

    @CheckForNull
    private static ByteBuffer getStableIdBytes(NodeState state) {
        if (state instanceof SegmentNodeState) {
            return ((SegmentNodeState) state).getStableIdBytes();
        } else {
            return null;
        }
    }

    private static class Reporter {

        private long count = 0;

        public void reportNode(Supplier<String> pathSupplier) {
            if (count > 0 && count % RepositorySidegrade.LOG_NODE_COPY == 0) {
                LOG.info("Copying node {}: {}", count, pathSupplier.get());
            }
            count++;
        }

    }
}
