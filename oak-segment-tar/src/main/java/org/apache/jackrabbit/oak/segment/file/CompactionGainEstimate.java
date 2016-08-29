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

import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.io.File;
import java.util.UUID;

import com.google.common.base.Supplier;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.RecordIdSet;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;

class CompactionGainEstimate implements TarEntryVisitor, GCEstimation {

    private static final Funnel<UUID> UUID_FUNNEL = new Funnel<UUID>() {
        @Override
        public void funnel(UUID from, PrimitiveSink into) {
            into.putLong(from.getMostSignificantBits());
            into.putLong(from.getLeastSignificantBits());
        }
    };

    private final BloomFilter<UUID> uuids;

    private final int gainThreshold;

    private long totalSize = 0;

    private long reachableSize = 0;

    private boolean gcNeeded;

    private String gcInfo = "unknown";

    private boolean finished = false;

    /**
     * Create a new instance of gain estimator. The estimation process can be stopped
     * by switching the supplier {@code stop} to {@code true}, in which case the returned
     * estimates are undefined.
     *
     * @param node  root node state
     * @param estimatedBulkCount
     * @param stop  stop signal
     */
    CompactionGainEstimate(SegmentNodeState node, int estimatedBulkCount,
            Supplier<Boolean> stop, int gainThreshold) {
        uuids = BloomFilter.create(UUID_FUNNEL, estimatedBulkCount);
        this.gainThreshold = gainThreshold;
        collectReferencedSegments(node, new RecordIdSet(), stop);
    }

    private void collectReferencedSegments(SegmentNodeState node, RecordIdSet visited, Supplier<Boolean> stop) {
        if (!stop.get() && visited.addIfNotPresent(node.getRecordId())) {
            collectUUID(node.getRecordId().getSegmentId());
            for (PropertyState property : node.getProperties()) {
                if (property instanceof SegmentPropertyState) {
                    collectUUID(((SegmentPropertyState) property)
                            .getRecordId().getSegmentId());
                }

                // Get the underlying value as stream so we can collect
                // the segments ids involved in storing the value.
                // This works as primitives are stored as strings and strings
                // as binaries of their UTF-8 encoding.
                for (Blob blob : property.getValue(BINARIES)) {
                    for (SegmentId id : SegmentBlob.getBulkSegmentIds(blob)) {
                        collectUUID(id);
                    }
                }
            }
            for (ChildNodeEntry child : node.getChildNodeEntries()) {
                collectReferencedSegments((SegmentNodeState) child.getNodeState(),
                        visited, stop);
            }
        }
    }

    private void collectUUID(SegmentId segmentId) {
        uuids.put(new UUID(
            segmentId.getMostSignificantBits(),
            segmentId.getLeastSignificantBits()));
    }

    /**
     * Returns a percentage estimate (scale 0-100) for how much disk space
     * running compaction (and cleanup) could potentially release.
     *
     * @return percentage of disk space that could be freed with compaction
     */
    public long estimateCompactionGain() {
        if (totalSize == 0) {
            return 0;
        }
        return 100 * (totalSize - reachableSize) / totalSize;
    }

    private void run() {
        if (finished) {
            return;
        }
        long gain = estimateCompactionGain();
        gcNeeded = gain >= gainThreshold;
        if (gcNeeded) {
            gcInfo = String
                    .format("Gain is %s%% or %s/%s (%s/%s bytes), so running compaction",
                            gain, humanReadableByteCount(reachableSize),
                            humanReadableByteCount(totalSize),
                            reachableSize, totalSize);
        } else {
            if (totalSize == 0) {
                gcInfo = "Skipping compaction for now as repository consists of a single tar file only";
            } else {
                gcInfo = String
                        .format("Gain is %s%% or %s/%s (%s/%s bytes), so skipping compaction for now",
                                gain,
                                humanReadableByteCount(reachableSize),
                                humanReadableByteCount(totalSize),
                                reachableSize, totalSize);
            }
        }
        finished = true;
    }

    @Override
    public boolean gcNeeded() {
        if (!finished) {
            run();
        }
        return gcNeeded;
    }

    @Override
    public String gcLog() {
        if (!finished) {
            run();
        }
        return gcInfo;
    }

    // ---------------------------------------------------< TarEntryVisitor >--

    @Override
    public void visit(long msb, long lsb, File file, int offset, int size) {
        UUID uuid = new UUID(msb, lsb);
        int entrySize = TarReader.getEntrySize(size);
        totalSize += entrySize;
        if (uuids.mightContain(uuid)) {
            reachableSize += entrySize;
        }
    }

}
