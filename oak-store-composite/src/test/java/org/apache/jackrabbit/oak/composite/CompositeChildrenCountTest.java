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
package org.apache.jackrabbit.oak.composite;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.newHashMap;
import static java.lang.Long.MAX_VALUE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

public class CompositeChildrenCountTest {

    @Test
    public void singleContributingStore() {
        MountInfoProvider mip = Mounts.newBuilder().build();
        NodeStore globalStore = new MemoryNodeStore();
        CompositeNodeStore compositeNodeStore = new CompositeNodeStore.Builder(mip, globalStore).build();

        CompositeNodeStoreBuilder b = new CompositeNodeStoreBuilder(compositeNodeStore.ctx);
        b.configureMount("/", MAX_VALUE);
        assertEquals(MAX_VALUE, b.getNodeState().getChildNodeCount(123));

        b.clear().configureMount("/", 10);
        assertEquals(10, b.getNodeState().getChildNodeCount(200));
    }

    @Test
    public void multipleContributingStores() {
        MountInfoProvider mip = Mounts.newBuilder().mount("libs", "/libs", "/libs1", "/libs2", "/libs3", "/libs4").build();
        NodeStore globalStore = new MemoryNodeStore();
        NodeStore libsStore = new MemoryNodeStore();

        List<MountedNodeStore> mounts = Lists.newArrayList(); 
        mounts.add(new MountedNodeStore(mip.getMountByName("libs"), libsStore));
        CompositeNodeStore compositeNodeStore = new CompositeNodeStore(mip, globalStore, mounts);

        CompositeNodeStoreBuilder b = new CompositeNodeStoreBuilder(compositeNodeStore.ctx);
        TestingNodeState globalTestingNS = b.configureMount("/", 5);
        TestingNodeState libsTestingNS = b.configureMount("/libs", "libs", "libs1", "libs2");

        CompositeNodeState mns = b.getNodeState();

        assertEquals(8, mns.getChildNodeCount(9));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(3, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(MAX_VALUE, mns.getChildNodeCount(8));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(3, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(MAX_VALUE, mns.getChildNodeCount(7));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(2, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(8, mns.builder().getChildNodeCount(9));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(3, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(MAX_VALUE, mns.builder().getChildNodeCount(8));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(3, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(MAX_VALUE, mns.builder().getChildNodeCount(7));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(2, libsTestingNS.fetchedChildren);
    }

    @Test
    public void contributingStoreReturnsInfinity() {
        MountInfoProvider mip = Mounts.newBuilder().mount("libs", "/libs", "/libs1", "/libs2", "/libs3", "/libs4").build();
        NodeStore globalStore = new MemoryNodeStore();
        NodeStore libsStore = new MemoryNodeStore();

        List<MountedNodeStore> mounts = Lists.newArrayList(); 
        mounts.add(new MountedNodeStore(mip.getMountByName("libs"), libsStore));
        CompositeNodeStore compositeNodeStore = new CompositeNodeStore(mip, globalStore, mounts);

        CompositeNodeStoreBuilder b = new CompositeNodeStoreBuilder(compositeNodeStore.ctx);
        TestingNodeState globalTestingNS = b.configureMount("/", 5);
        TestingNodeState libsTestingNS = b.configureMount("/libs", MAX_VALUE);

        CompositeNodeState mns = b.getNodeState();

        assertEquals(MAX_VALUE, mns.getChildNodeCount(100));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(0, libsTestingNS.fetchedChildren);
        globalTestingNS.fetchedChildren = 0;
        libsTestingNS.fetchedChildren = 0;

        assertEquals(MAX_VALUE, mns.builder().getChildNodeCount(100));
        assertEquals(5, globalTestingNS.fetchedChildren);
        assertEquals(0, libsTestingNS.fetchedChildren);
    }

    private static class CompositeNodeStoreBuilder {

        private final Map<MountedNodeStore, NodeState> rootStates = newHashMap();

        private final CompositionContext ctx;

        public CompositeNodeStoreBuilder(CompositionContext ctx) {
            this.ctx = ctx;
        }

        public TestingNodeState configureMount(String mountPath, long children) {
            TestingNodeState nodeState = new TestingNodeState(children);
            rootStates.put(ctx.getOwningStore(mountPath), nodeState);
            return nodeState;
        }

        public TestingNodeState configureMount(String mountPath, String... children) {
            TestingNodeState nodeState = new TestingNodeState(children);
            rootStates.put(ctx.getOwningStore(mountPath), nodeState);
            return nodeState;
        }

        public CompositeNodeState getNodeState() {
            return ctx.createRootNodeState(rootStates);
        }

        public CompositeNodeStoreBuilder clear() {
            rootStates.clear();
            return this;
        }
    }

    private static class TestingNodeState extends AbstractNodeState {

        private final long childrenCount;

        private final String[] children;

        private long fetchedChildren = 0;

        private TestingNodeState(long childrenCount) {
            this.children = null;
            this.childrenCount = childrenCount;
        }

        private TestingNodeState(String... children) {
            this.children = children;
            this.childrenCount = children.length;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Nonnull
        @Override
        public Iterable<? extends PropertyState> getProperties() {
            return emptyList();
        }

        @Override
        public boolean hasChildNode(@Nonnull String name) {
            return false;
        }

        @Nonnull
        @Override
        public NodeState getChildNode(@Nonnull String name) throws IllegalArgumentException {
            return EmptyNodeState.MISSING_NODE;
        }

        @Nonnull
        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            if (children == null) {
                Iterable<? extends ChildNodeEntry> childrenIterable = cycle(new MemoryChildNodeEntry("child", EMPTY_NODE));
                return asCountingIterable(limit(childrenIterable, childrenCount == MAX_VALUE ? 1000 : (int) childrenCount));
            } else {
                return asCountingIterable(transform(asList(children), new Function<String, ChildNodeEntry>() {
                    @Nullable
                    @Override
                    public ChildNodeEntry apply(@Nullable String input) {
                        return new MemoryChildNodeEntry(input, EMPTY_NODE);
                    }
                }));
            }
        }

        @Override
        public long getChildNodeCount(long max) {
            return childrenCount;
        }

        @Nonnull
        @Override
        public NodeBuilder builder() {
            return new MemoryNodeBuilder(this);
        }

        private <T> Iterable<T> asCountingIterable(Iterable<T> input) {
            return Iterables.transform(input, new Function<T, T>() {
                @Nullable
                @Override
                public T apply(@Nullable T input) {
                    fetchedChildren++;
                    return input;
                }
            });
        }
    }
}
