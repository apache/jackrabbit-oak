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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static org.apache.jackrabbit.oak.api.Type.BINARIES;

import java.io.File;
import java.util.UUID;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.RecordIdSet;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;

class CompactionGainEstimate implements TarEntryVisitor {

    private static final Funnel<UUID> UUID_FUNNEL = new Funnel<UUID>() {
        @Override
        public void funnel(UUID from, PrimitiveSink into) {
            into.putLong(from.getMostSignificantBits());
            into.putLong(from.getLeastSignificantBits());
        }
    };

    private final BloomFilter<UUID> uuids;

    private long totalSize = 0;

    private long reachableSize = 0;

    CompactionGainEstimate(SegmentNodeState node, int estimatedBulkCount) {
        uuids = BloomFilter.create(UUID_FUNNEL, estimatedBulkCount);
        collectReferencedSegments(node, new RecordIdSet());
    }

    private void collectReferencedSegments(SegmentNodeState node, RecordIdSet visited) {
        if (visited.addIfNotPresent(node.getRecordId())) {
            collectUUID(node.getRecordId().getSegmentId());
            for (PropertyState property : node.getProperties()) {
                if (property instanceof SegmentPropertyState) {
                    collectUUID(((SegmentPropertyState) property)
                            .getRecordId().getSegmentId());
                }
                for (Blob blob : property.getValue(BINARIES)) {
                    for (SegmentId id : SegmentBlob.getBulkSegmentIds(blob)) {
                        collectUUID(id);
                    }
                }
            }
            for (ChildNodeEntry child : node.getChildNodeEntries()) {
                collectReferencedSegments((SegmentNodeState) child.getNodeState(),
                        visited);
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

    public long getTotalSize() {
        return totalSize;
    }

    public long getReachableSize() {
        return reachableSize;
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
