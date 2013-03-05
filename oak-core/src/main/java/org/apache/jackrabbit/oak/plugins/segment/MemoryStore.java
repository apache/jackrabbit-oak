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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.RebaseDiff;

import com.google.common.collect.Maps;

public class MemoryStore implements SegmentStore {

    private static final class MemoryJournal implements Journal {

        private final SegmentStore store;

        private final Journal parent;

        private RecordId base;

        private RecordId head;

        MemoryJournal(SegmentStore store, NodeState root) {
            this.store = checkNotNull(store);
            this.parent = null;

            SegmentWriter writer =
                    new SegmentWriter(store, new SegmentReader(store));
            RecordId id = writer.writeNode(root).getRecordId();
            writer.flush();

            this.base = id;
            this.head = id;
        }

        MemoryJournal(SegmentStore store, String parent) {
            this.store = checkNotNull(store);
            this.parent = store.getJournal(checkNotNull(parent));
            this.base = this.parent.getHead();
            this.head = base;
        }

        @Override
        public synchronized RecordId getHead() {
            return head;
        }

        @Override
        public synchronized boolean setHead(RecordId base, RecordId head) {
            if (checkNotNull(base).equals(this.head)) {
                this.head = checkNotNull(head);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public synchronized void merge() {
            if (parent != null) {
                SegmentReader reader = new SegmentReader(store);
                NodeState before = new SegmentNodeState(reader, base);
                NodeState after = new SegmentNodeState(reader, head);

                SegmentWriter writer = new SegmentWriter(store, reader);
                while (!parent.setHead(base, head)) {
                    RecordId newBase = parent.getHead();
                    NodeBuilder builder =
                            new SegmentNodeState(reader, newBase).builder();
                    after.compareAgainstBaseState(
                            before, new RebaseDiff(builder));
                    NodeState state = builder.getNodeState();
                    RecordId newHead = writer.writeNode(state).getRecordId();
                    writer.flush();

                    base = newBase;
                    head = newHead;
                }

                base = head;
            }
        }

    }

    private final Map<String, Journal> journals = Maps.newHashMap();

    private final ConcurrentMap<UUID, Segment> segments =
            Maps.newConcurrentMap();

    public MemoryStore(NodeState root) {
        journals.put("root", new MemoryJournal(this, root));
    }

    public MemoryStore() {
        this(MemoryNodeState.EMPTY_NODE);
    }

    @Override
    public synchronized Journal getJournal(final String name) {
        Journal journal = journals.get(name);
        if (journal == null) {
            journal = new MemoryJournal(this, "root");
            journals.put(name, journal);
        }
        return journal;
    }

    @Override
    public Segment readSegment(UUID id) {
        Segment segment = segments.get(id);
        if (segment != null) {
            return segment;
        } else {
            throw new IllegalArgumentException("Segment not found: " + id);
        }
    }

    @Override
    public void createSegment(Segment segment) {
        if (segments.putIfAbsent(segment.getSegmentId(), segment) != null) {
            throw new IllegalStateException(
                    "Segment override: " + segment.getSegmentId());
        }
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        byte[] segment = new byte[length];
        System.arraycopy(data, offset, segment, 0, length);
        createSegment(new Segment(this, segmentId, segment, Collections.<UUID>emptySet()));
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments.remove(segmentId) == null) {
            throw new IllegalStateException("Missing segment: " + segmentId);
        }
    }

}
