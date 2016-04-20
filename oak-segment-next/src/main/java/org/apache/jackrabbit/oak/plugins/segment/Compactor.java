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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private final SegmentTracker tracker;

    private final SegmentWriter writer;

    private final ProgressTracker progress = new ProgressTracker();

    /**
     * Allows the cancellation of the compaction process. If this {@code
     * Supplier} returns {@code true}, this compactor will cancel compaction and
     * return a partial {@code SegmentNodeState} containing the changes
     * compacted before the cancellation.
     */
    private final Supplier<Boolean> cancel;

    public Compactor(SegmentTracker tracker) {
        this(tracker, Suppliers.ofInstance(false));
    }

    Compactor(SegmentTracker tracker, Supplier<Boolean> cancel) {
        this.tracker = tracker;
        this.writer = tracker.getWriter();
        this.cancel = cancel;
    }

    public Compactor(SegmentTracker tracker, CompactionStrategy compactionStrategy, Supplier<Boolean> cancel) {
        this.tracker = tracker;
        this.writer = createSegmentWriter(tracker);
        this.cancel = cancel;
    }

    @Nonnull
    private static SegmentWriter createSegmentWriter(SegmentTracker tracker) {
        return new SegmentWriter(tracker.getStore(), tracker.getSegmentVersion(),
            new SegmentBufferWriter(tracker.getStore(), tracker.getSegmentVersion(), "c", tracker.getGcGen() + 1));
    }

    /**
     * Compact the differences between a {@code before} and a {@code after}
     * on top of an {@code onto} state.
     * @param before  the before state
     * @param after   the after state
     * @param onto    the onto state
     * @return  the compacted state
     */
    public SegmentNodeState compact(NodeState before, NodeState after, NodeState onto) throws IOException {
        progress.start();
        SegmentNodeBuilder builder = new SegmentNodeBuilder(writer.writeNode(onto), writer);
        new CompactDiff(builder).diff(before, after);
        SegmentNodeState compacted = builder.getNodeState();
        writer.flush();
        progress.stop();
        return compacted;
    }

    private class CompactDiff extends ApplyDiff {
        private IOException exception;

        /**
         * Current processed path, or null if the trace log is not enabled at
         * the beginning of the compaction call. The null check will also be
         * used to verify if a trace log will be needed or not
         */
        private final String path;

        CompactDiff(NodeBuilder builder) {
            super(builder);
            if (log.isTraceEnabled()) {
                this.path = "/";
            } else {
                this.path = null;
            }
        }

        private CompactDiff(NodeBuilder builder, String path, String childName) {
            super(builder);
            if (path != null) {
                this.path = concat(path, childName);
            } else {
                this.path = null;
            }
        }

        boolean diff(NodeState before, NodeState after) throws IOException {
            boolean success = after.compareAgainstBaseState(before, new CancelableDiff(this, cancel));
            if (exception != null) {
                throw new IOException(exception);
            }
            return success;
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            try {
                progress.onProperty("propertyAdded", path, after);
                return super.propertyAdded(writer.writeProperty(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            try {
                progress.onProperty("propertyChanged", path, after);
                return super.propertyChanged(before, writer.writeProperty(after));
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            try {
                progress.onNode("childNodeAdded", path, name);
                super.childNodeAdded(name, writer.writeNode(after));
                return true;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            try {
                progress.onNode("childNodeChanged", path, name);
                return new CompactDiff(builder.getChildNode(name), path, name).diff(before, after);
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

    }

    private static class ProgressTracker {
        private final long logAt = Long.getLong("compaction-progress-log",
                150000);

        private long start = 0;

        private long nodes = 0;
        private long properties = 0;
        private long binaries = 0;

        void start() {
            nodes = 0;
            properties = 0;
            binaries = 0;
            start = System.currentTimeMillis();
        }

        void onNode(String msg, String path, String nodeName) {
            if (path != null) {
                log.trace("{} {}/{}", msg, path, nodeName);
            }
            if (++nodes % logAt == 0) {
                logProgress(start, false);
                start = System.currentTimeMillis();
            }
        }

        void onProperty(String msg, String path, PropertyState propertyState) {
            if (path != null) {
                log.trace("{} {}/{}", msg, path, propertyState.getName());
            }
            properties++;
        }

        void onBinary() {
            binaries++;
        }

        void stop() {
            logProgress(start, true);
        }

        private void logProgress(long start, boolean done) {
            log.debug(
                    "Compacted {} nodes, {} properties, {} binaries in {} ms.",
                    nodes, properties, binaries, System.currentTimeMillis()
                            - start);
            if (done) {
                log.info(
                        "Finished compaction: {} nodes, {} properties, {} binaries.",
                        nodes, properties, binaries);
            }
        }
    }

}
