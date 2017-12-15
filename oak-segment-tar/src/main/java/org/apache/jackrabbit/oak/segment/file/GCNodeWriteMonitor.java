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
package org.apache.jackrabbit.oak.segment.file;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.gc.GCMonitor;

/**
 * Monitors the compaction cycle and keeps a compacted nodes counter, in order
 * to provide a best effort progress log based on extrapolating the previous
 * size and node count and current size to deduce current node count.
 */
public class GCNodeWriteMonitor {
    public static final GCNodeWriteMonitor EMPTY = new GCNodeWriteMonitor(
            -1, GCMonitor.EMPTY);

    /**
     * Number of nodes the monitor will log a message, -1 to disable
     */
    private final long gcProgressLog;

    private final GCMonitor gcMonitor;

    /**
     * Start timestamp of compaction (reset at each {@code init()} call).
     */
    private long start = 0;

    /**
     * Estimated nodes to compact per cycle (reset at each {@code init()} call).
     */
    private long estimated = -1;

    /**
     * Number of compacted nodes
     */
    private long nodes;

    /**
     * Number of compacted properties
     */
    private long properties;

    /**
     * Number of compacted binaries
     */
    private long binaries;

    private boolean running = false;

    public GCNodeWriteMonitor(long gcProgressLog, @Nonnull GCMonitor gcMonitor) {
        this.gcProgressLog = gcProgressLog;
        this.gcMonitor = gcMonitor;
    }

    /**
     * @param prevSize
     *            size from latest successful compaction
     * @param prevCompactedNodes
     *            number of nodes compacted during latest compaction operation
     * @param currentSize
     *            current repository size
     */
    public synchronized void init(long prevSize, long prevCompactedNodes, long currentSize) {
        if (prevCompactedNodes > 0) {
            estimated = (long) (((double) currentSize / prevSize) * prevCompactedNodes);
            gcMonitor.info(
                "estimated number of nodes to compact is {}, based on {} nodes compacted to {} bytes "
                    + "on disk in previous compaction and current size of {} bytes on disk.",
                estimated, prevCompactedNodes, prevSize, currentSize);
        } else {
            gcMonitor.info("unable to estimate number of nodes for compaction, missing gc history.");
        }
        nodes = 0;
        start = System.currentTimeMillis();
        running = true;
    }

    public synchronized void onNode() {
        nodes++;
        if (gcProgressLog > 0 && nodes % gcProgressLog == 0) {
            gcMonitor.info("compacted {} nodes, {} properties, {} binaries in {} ms. {}",
                nodes, properties, binaries, System.currentTimeMillis() - start, getPercentageDone());
        }
    }

    public synchronized void onProperty() {
        properties++;
    }

    public synchronized void onBinary() {
        binaries++;
    }

    public synchronized void finished() {
        running = false;
    }

    /**
     * Compacted nodes in current cycle
     */
    public synchronized long getCompactedNodes() {
        return nodes;
    }

    /**
     * Estimated nodes to compact in current cycle. Can be {@code -1} if the
     * estimation could not be performed.
     */
    public synchronized long getEstimatedTotal() {
        return estimated;
    }

    @Nonnull
    private String getPercentageDone() {
        return estimated > 0
            ? getEstimatedPercentage() + "% complete."
            : "";
    }

    /**
     * Estimated completion percentage. Can be {@code -1} if the estimation
     * could not be performed.
     */
    public synchronized int getEstimatedPercentage() {
        if (estimated > 0) {
            if (!running) {
                return 100;
            } else {
                return Math.min((int) (100 * ((double) nodes / estimated)), 99);
            }
        }
        return -1;
    }

    public synchronized boolean isCompactionRunning() {
        return running;
    }

    public long getGcProgressLog() {
        return gcProgressLog;
    }
}
