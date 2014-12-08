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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentPropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

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
        collectReferencedSegments(node, new HashSet<ThinRecordId>());
    }

    private void collectReferencedSegments(SegmentNodeState node,
            Set<ThinRecordId> visited) {
        ThinRecordId tr = ThinRecordId.apply(node.getRecordId());
        if (!visited.contains(tr)) {
            uuids.put(asUUID(node.getRecordId().getSegmentId()));
            for (PropertyState property : node.getProperties()) {
                if (property instanceof SegmentPropertyState) {
                    uuids.put(asUUID(((SegmentPropertyState) property)
                            .getRecordId().getSegmentId()));
                }
                for (Blob blob : property.getValue(BINARIES)) {
                    for (SegmentId id : SegmentBlob.getBulkSegmentIds(blob)) {
                        uuids.put(asUUID(id));
                    }
                }
            }
            for (ChildNodeEntry child : node.getChildNodeEntries()) {
                collectReferencedSegments((SegmentNodeState) child.getNodeState(),
                        visited);
            }
            visited.add(tr);
        }
    }

    private static UUID asUUID(SegmentId id) {
        return new UUID(id.getMostSignificantBits(),
                id.getLeastSignificantBits());
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
        int entrySize = TarReader.getEntrySize(size);
        totalSize += entrySize;
        if (uuids.mightContain(new UUID(msb, lsb))) {
            reachableSize += entrySize;
        }
    }

    private static class ThinRecordId {

        static ThinRecordId apply(RecordId r) {
            return new ThinRecordId(r.getOffset(), r.getSegmentId()
                    .getMostSignificantBits(), r.getSegmentId()
                    .getLeastSignificantBits());
        }

        private final int offset;

        private final long msb;

        private final long lsb;

        public ThinRecordId(int offset, long msb, long lsb) {
            super();
            this.offset = offset;
            this.msb = msb;
            this.lsb = lsb;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (lsb ^ (lsb >>> 32));
            result = prime * result + (int) (msb ^ (msb >>> 32));
            result = prime * result + offset;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            ThinRecordId other = (ThinRecordId) obj;
            if (lsb != other.lsb)
                return false;
            if (msb != other.msb)
                return false;
            if (offset != other.offset)
                return false;
            return true;
        }
    }

}
