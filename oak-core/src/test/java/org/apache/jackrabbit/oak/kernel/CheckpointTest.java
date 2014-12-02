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

package org.apache.jackrabbit.oak.kernel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.NodeStoreFixture;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class CheckpointTest {

    @Parameters
    public static Collection<Object[]> fixtures() {
        Object[][] fixtures = new Object[][] {
                {NodeStoreFixture.MONGO_NS},
                {NodeStoreFixture.SEGMENT_MK},
                {NodeStoreFixture.MEMORY_NS},
        };
        return Arrays.asList(fixtures);
    }

    private final NodeStoreFixture fixture;

    private NodeStore store;

    private NodeState root;

    public CheckpointTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Before
    public void setUp() throws Exception {
        store = fixture.createNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder test = builder.child("test");
        test.setProperty("a", 1);
        test.setProperty("b", 2);
        test.setProperty("c", 3);
        test.child("x");
        test.child("y");
        test.child("z");
        root = store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose(store);
    }

    @Test
    public void checkpoint() throws CommitFailedException {
        String cp = store.checkpoint(Long.MAX_VALUE);

        NodeBuilder builder = store.getRoot().builder();
        builder.setChildNode("new");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(root.equals(store.getRoot()));
        assertEquals(root, store.retrieve(cp));

        assertTrue(store.release(cp));
        assertNull(store.retrieve(cp));
    }

    @Test
    public void checkpointInfo() throws CommitFailedException {
        ImmutableMap<String, String> props = ImmutableMap.of(
                "one", "1", "two", "2", "three", "2");
        String cp = store.checkpoint(Long.MAX_VALUE, props);
        assertEquals(props, store.checkpointInfo(cp));
    }

}
