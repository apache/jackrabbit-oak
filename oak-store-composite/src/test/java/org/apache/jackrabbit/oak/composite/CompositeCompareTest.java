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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompositeCompareTest {

    @Test
    public void reportedNodesAreWrapped() {
        Mounts.Builder mipBuilder = Mounts.newBuilder();
        mipBuilder.readOnlyMount("libs", "/libs");
        MountInfoProvider mip = mipBuilder.build();

        NodeStore globalStore = new MemoryNodeStore();
        CompositeNodeStore.Builder nsBuilder = new CompositeNodeStore.Builder(mip, globalStore);
        nsBuilder.addMount("libs", new MemoryNodeStore());
        CompositeNodeStore compositeNodeStore = nsBuilder.build();

        NodeBuilder builder = compositeNodeStore.getRoot().builder();
        builder.child("changed");
        builder.child("deleted");
        NodeState base = builder.getNodeState();

        builder.getChildNode("changed").setProperty("newProp", "xyz", Type.STRING);
        builder.getChildNode("deleted").remove();
        builder.child("added");
        final NodeState modified = builder.getNodeState();

        final Set<String> modifiedNodes = newHashSet();
        modified.compareAgainstBaseState(base, new DefaultNodeStateDiff() {
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                assertTrue(after instanceof CompositeNodeState);
                assertEquals(name, "added");
                modifiedNodes.add(name);
                return true;
            }

            @Override
            public boolean childNodeChanged(String name, NodeState before, NodeState after) {
                assertTrue(before instanceof CompositeNodeState);
                assertTrue(after instanceof CompositeNodeState);
                assertEquals(name, "changed");
                modifiedNodes.add(name);
                return true;
            }

            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                assertTrue(before instanceof CompositeNodeState);
                assertEquals(name, "deleted");
                modifiedNodes.add(name);
                return true;
            }
        });
        assertEquals(ImmutableSet.of("added", "changed", "deleted"), modifiedNodes);
    }

    @Test
    public void onlyPropertiesOnMainNodesAreCompared() throws CommitFailedException {
        MountInfoProvider mip = Mounts.newBuilder().mount("libs", "/libs").build();
        NodeStore globalStore = new MemoryNodeStore();
        NodeStore libsStore = new MemoryNodeStore();

        List<MountedNodeStore> mounts = Lists.newArrayList(); 
        mounts.add(new MountedNodeStore(mip.getMountByName("libs"), libsStore));
        CompositeNodeStore compositeNodeStore = new CompositeNodeStore(mip, globalStore, mounts);

        NodeState empty = compositeNodeStore.getRoot();

        NodeBuilder builder = globalStore.getRoot().builder();
        builder.setProperty("global-prop-1", "val");
        builder.setProperty("global-prop-2", "val");
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder libsBuilder = libsStore.getRoot().builder();
        libsBuilder.setProperty("libs-prop-1", "val");
        libsBuilder.setProperty("libs-prop-2", "val");
        libsStore.merge(libsBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState modified = compositeNodeStore.getRoot();

        final Set<String> addedProperties = newHashSet();
        modified.compareAgainstBaseState(empty, new DefaultNodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                addedProperties.add(after.getName());
                return true;
            }
        });
        assertEquals(ImmutableSet.of("global-prop-1", "global-prop-2"), addedProperties);
    }

    @Test
    public void nodesOutsideTheMountsAreIgnored() throws CommitFailedException {
        MountInfoProvider mip = Mounts.newBuilder().mount("libs", "/libs").build();
        NodeStore globalStore = new MemoryNodeStore();
        NodeStore libsStore = new MemoryNodeStore();

        List<MountedNodeStore> mounts = Lists.newArrayList();
        mounts.add(new MountedNodeStore(mip.getMountByName("libs"), libsStore));
        CompositeNodeStore compositeNodeStore = new CompositeNodeStore(mip, globalStore, mounts);

        NodeState empty = compositeNodeStore.getRoot();

        NodeBuilder builder = globalStore.getRoot().builder();
        builder.child("global-child-1");
        builder.child("global-child-2");
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder libsBuilder = libsStore.getRoot().builder();
        libsBuilder.child("libs");
        libsBuilder.child("libs-child-1");
        libsBuilder.child("libs-child-2");
        libsStore.merge(libsBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState modified = compositeNodeStore.getRoot();

        final Set<String> addedChildren = newHashSet();
        modified.compareAgainstBaseState(empty, new DefaultNodeStateDiff() {
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                addedChildren.add(name);
                return true;
            }
        });
        assertEquals(ImmutableSet.of("global-child-1", "global-child-2", "libs"), addedChildren);

    }
}
