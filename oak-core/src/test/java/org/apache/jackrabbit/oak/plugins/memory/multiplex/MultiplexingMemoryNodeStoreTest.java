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
package org.apache.jackrabbit.oak.plugins.memory.multiplex;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class MultiplexingMemoryNodeStoreTest {

    private MultiplexingMemoryNodeStore store;
    private MemoryNodeStore globalStore;
    private MemoryNodeStore mountedStore;
    private MemoryNodeStore deepMountedStore;

    @Before
    public void initStore() throws CommitFailedException {
        
        MountInfoProvider mip = new SimpleMountInfoProvider.Builder()
                .mount("temp", "/tmp")
                .mount("deep", "/libs/mount")
                .build();
        
        globalStore = new MemoryNodeStore();
        mountedStore = new MemoryNodeStore();
        deepMountedStore = new MemoryNodeStore();

        // create a property on the root node
        NodeBuilder builder = globalStore.getRoot().builder();
        builder.setProperty("prop", "val");
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue(globalStore.getRoot().hasProperty("prop"));
        
        // create a different sub-tree on the root store
        builder = globalStore.getRoot().builder();
        NodeBuilder libsBuilder = builder.child("libs");
        libsBuilder.child("first");
        libsBuilder.child("second");
        
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertThat(globalStore.getRoot().getChildNodeCount(10), equalTo(1l));
        
        // create a /tmp child on the mounted store and set a property
        builder = mountedStore.getRoot().builder();
        NodeBuilder tmpBuilder = builder.child("tmp");
        tmpBuilder.setProperty("prop1", "val1");
        tmpBuilder.child("child1");
        tmpBuilder.child("child2");

        mountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue(mountedStore.getRoot().hasChildNode("tmp"));
        assertThat(mountedStore.getRoot().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));
        
        // populate /libs/mount/third in the deep mount, and include a property
        
        builder = deepMountedStore.getRoot().builder();
        builder.child("libs").child("mount").child("third").setProperty("mounted", "true");
        
        deepMountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(deepMountedStore.getRoot().getChildNode("libs").getChildNode("mount").getChildNode("third").hasProperty("mounted"));
        
        store = new MultiplexingMemoryNodeStore.Builder(mip, globalStore)
                .addMount("temp", mountedStore)
                .addMount("deep", deepMountedStore)
                .build();
    }
    
    @Test
    public void rootExists() {
        
        assertThat("root exists", store.getRoot().exists(), equalTo(true));
    }
    
    @Test
    public void rootPropertyIsSet() {
        
        assertThat("root[prop]", store.getRoot().hasProperty("prop"), equalTo(true));
        assertThat("root[prop] = val", store.getRoot().getProperty("prop").getValue(Type.STRING), equalTo("val"));
    }
    
    @Test
    public void nonMountedChildIsFound() {
        
        assertThat("root.libs", store.getRoot().hasChildNode("libs"), equalTo(true));
    }
    
    @Test
    public void nestedMountNodeIsVisible() {
        assertThat("root.libs(childCount)", store.getRoot().getChildNode("libs").getChildNodeCount(10), equalTo(3l));
    }
    
    @Test
    public void mixedMountsChildNodes() {
        
        assertThat("root(childCount)", store.getRoot().getChildNodeCount(100), equalTo(2l));
    }
    
    @Test
    public void mountedChildIsFound() {
        
        assertThat("root.tmp", store.getRoot().hasChildNode("tmp"), equalTo(true));
    }
    
    @Test
    public void childrenUnderMountAreFound() {
        
        assertThat("root.tmp(childCount)", store.getRoot().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));
    }
    
    @Test
    public void childNodeEntryForMountIsMultiplexed() {
        
        ChildNodeEntry libsNode = Iterables.find(store.getRoot().getChildNodeEntries(), new Predicate<ChildNodeEntry>() {

            @Override
            public boolean apply(ChildNodeEntry input) {
                return input.getName().equals("libs");
            }
        });
        
        assertThat("root.libs(childCount)", libsNode.getNodeState().getChildNodeCount(10), equalTo(3l));
    }
    
    @Test
    public void contentBelongingToAnotherMountIsIgnored() throws Exception {
        
        // create a /tmp/oops child on the root store
        // these two nodes must be ignored
        NodeBuilder builder = globalStore.getRoot().builder();
        builder.child("tmp").child("oops");
        
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue(globalStore.getRoot().getChildNode("tmp").hasChildNode("oops"));

        assertFalse(store.getRoot().getChildNode("tmp").hasChildNode("oops"));
    }
    
    @Test
    public void checkpoint() throws Exception {
        
        String checkpoint = store.checkpoint(TimeUnit.DAYS.toMillis(1));
        
        assertNotNull("checkpoint reference is null", checkpoint);
        
        // create a new child /new in the root store
        NodeBuilder globalBuilder = globalStore.getRoot().builder();
        globalBuilder.child("new");
        globalStore.merge(globalBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        // create a new child /tmp/new in the mounted store
        NodeBuilder mountedBuilder = mountedStore.getRoot().builder();
        mountedBuilder.getChildNode("tmp").child("new");
        mountedStore.merge(mountedBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        // create a new child /libs/mount/new in the deeply mounted store
        NodeBuilder deepMountBuilder = deepMountedStore.getRoot().builder();
        deepMountBuilder.getChildNode("libs").getChildNode("mount").child("new");
        deepMountedStore.merge(deepMountBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertFalse("store incorrectly exposes child at /new", store.retrieve(checkpoint).hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /tmp/new", store.retrieve(checkpoint).getChildNode("tmp").hasChildNode("new"));
        assertFalse("store incorrectly exposes child at /libs/mount/new", store.retrieve(checkpoint).getChildNode("libs").getChildNode("mount").hasChildNode("new"));
    }
}