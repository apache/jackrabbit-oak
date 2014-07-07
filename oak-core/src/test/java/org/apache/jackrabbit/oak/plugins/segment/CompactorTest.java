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

import junit.framework.Assert;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class CompactorTest {

    @Test
    public void testCompactor() throws Exception {
        MemoryStore source = new MemoryStore();
        try {
            NodeStore store = new SegmentNodeStore(source);
            init(store);

            Compactor compactor = new Compactor(source.getTracker().getWriter());
            addTestContent(store, 0);

            NodeState initial = store.getRoot();
            SegmentNodeState after = compactor
                    .compact(initial, store.getRoot());
            Assert.assertEquals(store.getRoot(), after);

            addTestContent(store, 1);
            after = compactor.compact(initial, store.getRoot());
            Assert.assertEquals(store.getRoot(), after);

        } finally {
            source.close();
        }

    }

    private static void init(NodeStore store) {
        new Oak(store).with(new OpenSecurityProvider())
                .createContentRepository();
    }

    private static void addTestContent(NodeStore store, int index)
            throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("test" + index);
        builder.child("child" + index);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
