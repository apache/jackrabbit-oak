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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class MultiplexedMemoryNodeStoreTest {

    private MultiplexingMemoryNodeStore store;
    private MemoryNodeStore globalStore;
    private MemoryNodeStore mountedStore;

    @Before
    public void initStore() throws CommitFailedException {
        
        MountInfoProvider mip = new SimpleMountInfoProvider.Builder().mount("temp", "/tmp").build();
        globalStore = new MemoryNodeStore();
        mountedStore = new MemoryNodeStore();

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
        
        // create a /tmp child on the mounted store and set a property
        builder = mountedStore.getRoot().builder();
        builder.child("tmp").setProperty("prop1", "val1");
        mountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        assertTrue(mountedStore.getRoot().hasChildNode("tmp"));
        
        store = new MultiplexingMemoryNodeStore.Builder(mip, globalStore).addMount("temp", mountedStore).build();
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
        assertThat("root.libs(childCount)", store.getRoot().getChildNode("libs").getChildNodeCount(10), equalTo(2l));
    }
    
    @Test
    public void mixedMountsChildNodeCount() {
        
        assertThat("root(childCount)", store.getRoot().getChildNodeCount(100), equalTo(2l));
    }
    
    @Test
    public void mountedChildIsFound() {
        
        assertThat("root.tmp", store.getRoot().hasChildNode("tmp"), equalTo(true));
    }
}