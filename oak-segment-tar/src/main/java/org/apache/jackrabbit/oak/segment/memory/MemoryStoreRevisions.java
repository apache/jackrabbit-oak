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

package org.apache.jackrabbit.oak.segment.memory;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * This is a simple in memory {@code Revisions} implementation.
 * It is non blocking and does not support any {@link Option}s.
 */
public class MemoryStoreRevisions implements Revisions {
    private RecordId head;

    /**
     * Bind this instance to a {@code store}.
     */
    public void bind(MemoryStore store) throws IOException {
        if (head == null) {
            NodeBuilder builder = EMPTY_NODE.builder();
            builder.setChildNode("root", EMPTY_NODE);
            head = store.getWriter().writeNode(builder.getNodeState());
            store.getWriter().flush();
        }
    }

    private void checkBound() {
        checkState(head != null, "Revisions not bound to a store");
    }

    @Nonnull
    @Override
    public synchronized RecordId getHead() {
        checkBound();
        return head;
    }

    @Nonnull
    @Override
    public RecordId getPersistedHead() {
        return getHead();
    }
    
    @Override
    public synchronized boolean setHead(
            @Nonnull RecordId expected, @Nonnull RecordId head,
            @Nonnull Option... options) {
        checkBound();
        if (this.head.equals(expected)) {
            this.head = head;
            return true;
        } else {
            return false;
        }
    }

    /**
     * Not supported: throws {@code UnsupportedOperationException}
     * @throws UnsupportedOperationException always
     */
    @Override
    public RecordId setHead(
            @Nonnull Function<RecordId, RecordId> newHead,
            @Nonnull Option... options) throws InterruptedException {
        throw new UnsupportedOperationException();
    }
}
