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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A node builder that keeps track of the number of updates
 * (set property calls and so on). If there are too many updates,
 * getNodeState() is called, which will write the records to the segment,
 * and that might persist the changes (if the segment is flushed).
 */
public class SegmentNodeBuilder extends MemoryNodeBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentNodeBuilder.class);

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the underlying segments.
     */
    private static final int UPDATE_LIMIT =
            Integer.getInteger("update.limit", 10000);

    @Nullable
    private final BlobStore blobStore;

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final SegmentWriter writer;

    private final MeterStats readStats;

    /**
     * Local update counter for the root builder.
     * 
     * The value encodes both the counter and the type of the node builder:
     * <ul>
     * <li>value >= {@code 0} represents a root builder (builder keeps
     * counter updates)</li>
     * <li>value = {@code -1} represents a child builder (value doesn't
     * change, builder doesn't keep an updated counter)</li>
     * </ul>
     * 
     */
    private long updateCount;

    SegmentNodeBuilder(
        @NotNull SegmentNodeState base,
        @Nullable BlobStore blobStore,
        @NotNull SegmentReader reader,
        @NotNull SegmentWriter writer,
        MeterStats readStats
    ) {
        super(base);
        this.blobStore = blobStore;
        this.reader = reader;
        this.writer = checkNotNull(writer);
        this.updateCount = 0;
        this.readStats = readStats;
    }

    private SegmentNodeBuilder(
        @NotNull SegmentNodeBuilder parent,
        @NotNull String name,
        @Nullable BlobStore blobStore,
        @NotNull SegmentReader reader,
        @NotNull SegmentWriter writer,
        MeterStats readStats
    ) {
        super(parent, name);
        this.blobStore = blobStore;
        this.reader = reader;
        this.writer = checkNotNull(writer);
        this.updateCount = -1;
        this.readStats = readStats;
    }

    /**
     * @return  {@code true} iff this builder has been acquired from a root node state.
     */
    boolean isRootBuilder() {
        return isRoot();
    }

    //-------------------------------------------------< MemoryNodeBuilder >--

    @Override
    protected void updated() {
        if (isChildBuilder()) {
            super.updated();
        } else {
            updateCount++;
            if (updateCount > UPDATE_LIMIT) {
                getNodeState();
            }
        }
    }

    private boolean isChildBuilder() {
        return updateCount < 0;
    }

    //-------------------------------------------------------< NodeBuilder >--

    @NotNull
    @Override
    public SegmentNodeState getNodeState() {
        try {
            NodeState state = super.getNodeState();
            SegmentNodeState sState = new SegmentNodeState(reader, writer, blobStore, writer.writeNode(state), readStats);
            if (state != sState) {
                set(sState);
                if(!isChildBuilder()) {
                    updateCount = 0;
                }
            }
            return sState;
        } catch (IOException e) {
            LOG.error("Error flushing changes", e);
            throw new IllegalStateException("Unexpected IOException", e);
        }
    }

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new SegmentNodeBuilder(this, name, blobStore, reader, writer, readStats);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        return new SegmentBlob(blobStore, writer.writeStream(stream));
    }

}
