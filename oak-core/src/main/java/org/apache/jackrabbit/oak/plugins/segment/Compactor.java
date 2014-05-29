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

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.BINARY;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

/**
 * Tool for compacting segments.
 */
public class Compactor {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private final SegmentStore store;

    private final SegmentWriter writer;

    /**
     * Map from the identifiers of old records to the identifiers of their
     * compacted copies. Used to prevent the compaction code from duplicating
     * things like checkpoints that share most of their content with other
     * subtrees.
     */
    private final Map<RecordId, RecordId> compacted = newHashMap();

    public Compactor(SegmentStore store) {
        this.store = store;
        this.writer = store.getTracker().getWriter();
    }

    public void compact() throws IOException {
        log.debug("TarMK compaction");

        SegmentNodeBuilder builder = writer.writeNode(EMPTY_NODE).builder();
        SegmentNodeState before = store.getHead();
        EmptyNodeState.compareAgainstEmptyState(
                before, new CompactDiff(builder));
        System.out.println(compacted.size() + " nodes compacted");

        SegmentNodeState after = builder.getNodeState();
        while (!store.setHead(before, after)) {
            // Some other concurrent changes have been made.
            // Rebase (and compact) those changes on top of the
            // compacted state before retrying to set the head.
            SegmentNodeState head = store.getHead();
            head.compareAgainstBaseState(before, new CompactDiff(builder));
            System.out.println(compacted.size() + " nodes compacted");
            before = head;
            after = builder.getNodeState();
        }
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

            if (success && id != null) {
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

            if (success && id != null) {
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

    private Blob compact(Blob blob) {
        if (blob instanceof SegmentBlob) {
            try {
                return ((SegmentBlob) blob).clone(writer);
            } catch (IOException e) {
                Log.warn("Failed to clone a binary value", e);
            }
        }
        return blob;
    }

}
