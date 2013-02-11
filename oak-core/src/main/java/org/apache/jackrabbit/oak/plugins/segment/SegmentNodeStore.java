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

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;

public class SegmentNodeStore implements NodeStore {

    private final SegmentStore store;

    private final SegmentReader reader;

    private CommitHook hook = new EmptyHook();

    public SegmentNodeStore(SegmentStore store) {
        this.store = store;
        this.reader = new SegmentReader(store);
    }

    @Override @Nonnull
    public NodeState getRoot() {
        return new SegmentNodeState(reader, store.getJournalHead());
    }

    @Override @Nonnull
    public NodeStoreBranch branch() {
        return new SegmentNodeStoreBranch(store, reader, hook);
    }

    @Override
    public Blob createBlob(InputStream stream) throws IOException {
        SegmentWriter writer = new SegmentWriter(store);
        RecordId recordId = writer.writeStream(stream);
        writer.flush();
        return new SegmentBlob(reader, recordId);
    }

    public void setHook(CommitHook hook) {
        this.hook = hook;
    }

}
