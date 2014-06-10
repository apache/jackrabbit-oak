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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    public static CompactionMap compact(SegmentStore store) {
        SegmentWriter writer = store.getTracker().getWriter();
        Compactor compactor = new Compactor(writer);

        log.debug("TarMK compaction");

        SegmentNodeBuilder builder = writer.writeNode(EMPTY_NODE).builder();
        SegmentNodeState before = store.getHead();
        EmptyNodeState.compareAgainstEmptyState(
                before, compactor.newCompactDiff(builder));

        SegmentNodeState after = builder.getNodeState();
        while (!store.setHead(before, after)) {
            // Some other concurrent changes have been made.
            // Rebase (and compact) those changes on top of the
            // compacted state before retrying to set the head.
            SegmentNodeState head = store.getHead();
            head.compareAgainstBaseState(
                    before, compactor.newCompactDiff(builder));
            before = head;
            after = builder.getNodeState();
        }

        return new CompactionMap(compactor.compacted);
    }

    /**
     * Locks down the RecordId persistence structure
     */
    static long[] recordAsKey(RecordId r) {
        return new long[] { r.getSegmentId().getMostSignificantBits(),
                r.getSegmentId().getLeastSignificantBits(), r.getOffset() };
    }

    private final SegmentWriter writer;

    /**
     * Map from the identifiers of old records to the identifiers of their
     * compacted copies. Used to prevent the compaction code from duplicating
     * things like checkpoints that share most of their content with other
     * subtrees.
     */
    private final Map<RecordId, RecordId> compacted = newHashMap();

    /**
     * Map from {@link #getBlobKey(Blob) blob keys} to matching compacted
     * blob record identifiers. Used to de-duplicate copies of the same
     * binary values.
     */
    private final Map<String, List<RecordId>> binaries = newHashMap();

    private Compactor(SegmentWriter writer) {
        this.writer = writer;
    }

    private CompactDiff newCompactDiff(NodeBuilder builder) {
        return new CompactDiff(builder);
    }

    private class CompactDiff extends ApplyDiff {

        CompactDiff(NodeBuilder builder) {
            super(builder);
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            return super.propertyAdded(compact(after));
        }


        @Override
        public boolean propertyChanged(
                PropertyState before, PropertyState after) {
            return super.propertyChanged(before, compact(after));
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            RecordId id = null;
            if (after instanceof SegmentNodeState) {
                id = ((SegmentNodeState) after).getRecordId();
                RecordId compactedId = compacted.get(id);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(compactedId));
                    return true;
                }
            }

            NodeBuilder child = builder.setChildNode(name);
            boolean success = EmptyNodeState.compareAgainstEmptyState(
                    after, new CompactDiff(child));

            if (success && id != null && child.getChildNodeCount(1) > 0) {
                RecordId compactedId =
                        writer.writeNode(child.getNodeState()).getRecordId();
                compacted.put(id, compactedId);
            }

            return success;
        }

        @Override
        public boolean childNodeChanged(
                String name, NodeState before, NodeState after) {
            RecordId id = null;
            if (after instanceof SegmentNodeState) {
                id = ((SegmentNodeState) after).getRecordId();
                RecordId compactedId = compacted.get(id);
                if (compactedId != null) {
                    builder.setChildNode(name, new SegmentNodeState(compactedId));
                    return true;
                }
            }

            NodeBuilder child = builder.getChildNode(name);
            boolean success = after.compareAgainstBaseState(
                    before, new CompactDiff(child));

            if (success && id != null && child.getChildNodeCount(1) > 0) {
                RecordId compactedId =
                        writer.writeNode(child.getNodeState()).getRecordId();
                compacted.put(id, compactedId);
            }

            return success;
        }

    }

    private PropertyState compact(PropertyState property) {
        String name = property.getName();
        Type<?> type = property.getType();
        if (type == BINARY) {
            Blob blob = compact(property.getValue(Type.BINARY));
            return BinaryPropertyState.binaryProperty(name, blob);
        } else if (type == BINARIES) {
            List<Blob> blobs = new ArrayList<Blob>();
            for (Blob blob : property.getValue(BINARIES)) {
                blobs.add(compact(blob));
            }
            return MultiBinaryPropertyState.binaryPropertyFromBlob(name, blobs);
        } else {
            Object value = property.getValue(type);
            return PropertyStates.createProperty(name, value, type);
        }
    }

    /**
     * Compacts (and de-duplicates) the given blob.
     *
     * @param blob blob to be compacted
     * @return compacted blob
     */
    private Blob compact(Blob blob) {
        if (blob instanceof SegmentBlob) {
            SegmentBlob sb = (SegmentBlob) blob;

            try {
                // if the blob is inlined or external, just clone it
                if (sb.isExternal() || sb.length() < Segment.MEDIUM_LIMIT) {
                    return sb.clone(writer);
                }

                // else check if we've already cloned this specific record
                RecordId id = sb.getRecordId();
                RecordId compactedId = compacted.get(id);
                if (compactedId != null) {
                    return new SegmentBlob(compactedId);
                }

                // alternatively look if the exact same binary has been cloned
                String key = getBlobKey(blob);
                List<RecordId> ids = binaries.get(key);
                if (ids != null) {
                    for (RecordId duplicateId : ids) {
                        if (new SegmentBlob(duplicateId).equals(blob)) {
                            return new SegmentBlob(duplicateId);
                        }
                    }
                }

                // if not, clone the blob and keep track of the result
                sb = sb.clone(writer);
                compacted.put(id, sb.getRecordId());
                if (ids == null) {
                    ids = newArrayList();
                    binaries.put(key, ids);
                }
                ids.add(sb.getRecordId());

                return sb;
            } catch (IOException e) {
                log.warn("Failed to compcat a blob", e);
                // fall through
            }
        }

        // no way to compact this blob, so we'll just keep it as-is
        return blob;
    }

    private String getBlobKey(Blob blob) throws IOException {
        InputStream stream = blob.getNewStream();
        try {
            byte[] buffer = new byte[SegmentWriter.BLOCK_SIZE];
            int n = IOUtils.readFully(stream, buffer, 0, buffer.length);
            return blob.length() + ":" + Hashing.sha1().hashBytes(buffer, 0, n);
        } finally {
            stream.close();
        }
    }

}
