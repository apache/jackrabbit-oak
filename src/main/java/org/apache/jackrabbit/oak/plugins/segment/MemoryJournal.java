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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

final class MemoryJournal implements Journal {

    private final SegmentStore store;

    private final Journal parent;

    private RecordId base;

    private RecordId head;

    MemoryJournal(SegmentStore store, NodeState root) {
        this.store = checkNotNull(store);
        this.parent = null;

        SegmentWriter writer = new SegmentWriter(store);
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
            NodeState before = new SegmentNodeState(store, base);
            NodeState after = new SegmentNodeState(store, head);

            SegmentWriter writer = new SegmentWriter(store);
            while (!parent.setHead(base, head)) {
                RecordId newBase = parent.getHead();
                NodeBuilder builder =
                        new SegmentNodeState(store, newBase).builder();
                after.compareAgainstBaseState(before, new MergeDiff(builder));
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
