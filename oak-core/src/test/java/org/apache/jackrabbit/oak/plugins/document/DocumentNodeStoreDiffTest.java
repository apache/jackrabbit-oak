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
package org.apache.jackrabbit.oak.plugins.document;

import com.mongodb.DB;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.junit.Assert.assertEquals;

public class DocumentNodeStoreDiffTest extends AbstractMongoConnectionTest {

    // OAK-2562
    @Test
    public void diff() throws Exception {
        DocumentNodeStore store = mk.getNodeStore();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("other");
        for (int i = 0; i < 10; i++) {
            builder.child("test").child("folder").child("node-" + i);
        }
        merge(store, builder);

        for (int i = 0; i < 50; i++) {
            builder = store.getRoot().builder();
            builder.child("other").child("node-" + i);
            merge(store, builder);
        }

        NodeState before = store.getRoot();

        builder = store.getRoot().builder();
        builder.child("test").child("folder").child("node-x").child("child");
        NodeState after = merge(store, builder);

        for (int i = 0; i < 10; i++) {
            builder = store.getRoot().builder();
            builder.child("test").child("folder").child("node-" + i).child("child");
            merge(store, builder);
        }

        Iterable<CacheStats> stats = store.getDiffCacheStats();
        for (CacheStats cs : stats) {
            cs.resetStats();
        }

        // must not cause cache misses
        Diff.perform(before, after);
        for (CacheStats cs : stats) {
            assertEquals(0, cs.getMissCount());
        }
    }

    @Override
    protected Clock getTestClock() throws InterruptedException {
        return new Clock.Virtual();
    }

    private static NodeState merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    private static class Diff extends DefaultNodeStateDiff {
        
        static void perform(NodeState before, NodeState after) {
            after.compareAgainstBaseState(before, new Diff());
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return after.compareAgainstBaseState(EMPTY_NODE, this);
        }

        @Override
        public boolean childNodeChanged(String name,
                                        NodeState before,
                                        NodeState after) {
            return after.compareAgainstBaseState(before, this);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            return MISSING_NODE.compareAgainstBaseState(before, this);
        }
    }
}
