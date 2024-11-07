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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.TestNodeObserver;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class CompositeNodeStoreClusterObservationTest {

    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private CompositeNodeStore store;
    private DocumentNodeStore remote;
    private DocumentNodeStore globalStore;

    private TestNodeObserver observer;

    @Before
    public void initStore() {

        remote = createNodeStore(1);
        globalStore = createNodeStore(2);

        MountInfoProvider mip = Mounts.newBuilder().build();

        List<MountedNodeStore> nonDefaultStores = new ArrayList<>();
        store = new CompositeNodeStore(mip, globalStore, nonDefaultStores);

        observer = new TestNodeObserver("/test");
    }

    @Test
    public void localObserver() throws CommitFailedException {
        store.addObserver(observer);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").setProperty("foo", "bar");
        merge(store, builder);

        assertTrue("Node added event not observed for local change", observer.added.containsKey("/test"));
    }

    @Test
    public void remoteObserver() throws CommitFailedException {
        store.addObserver(observer);

        NodeBuilder builder = remote.getRoot().builder();
        builder.child("test").setProperty("foo", "bar");
        merge(remote, builder);

        remote.runBackgroundOperations();
        globalStore.runBackgroundOperations();

        assertTrue("Node added event not observed for remote change", observer.added.containsKey("/test"));
    }

    @Test
    public void mixedObservation() throws CommitFailedException {
        store.addObserver(observer);

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child("test1").setProperty("foo", "bar");
        merge(store, builder);

        // public local changes
        globalStore.runBackgroundOperations();
        remote.runBackgroundOperations();

        builder = remote.getRoot().builder();
        builder.getChildNode("test").child("test2").setProperty("foo", "bar");
        merge(remote, builder);

        // read in remote changes
        remote.runBackgroundOperations();
        globalStore.runBackgroundOperations();

        builder = store.getRoot().builder();
        builder.getChildNode("test").child("test3").setProperty("foo", "bar");
        merge(store, builder);

        assertTrue("Node added event not observed for local change before remote change", observer.added.containsKey("/test"));
        assertTrue("Node added event not observed for local change before remote change", observer.added.containsKey("/test/test1"));
        assertTrue("Node added event not observed for remote change", observer.added.containsKey("/test/test2"));
        assertTrue("Node added event not observed for local change after remote change", observer.added.containsKey("/test/test3"));
    }

    private static void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private DocumentNodeStore createNodeStore(int clusterId) {
        if (ds == null) {
            ds = new MemoryDocumentStore();
        }
        if (bs == null) {
            bs = new MemoryBlobStore();
        }
        return createNodeStore(clusterId, ds, bs);
    }

    private DocumentNodeStore createNodeStore(int clusterId,
                                              DocumentStore ds, BlobStore bs) {
        return builderProvider.newBuilder().setDocumentStore(ds)
                .setBlobStore(bs).setClusterId(clusterId)
                .setAsyncDelay(0).build();
    }
}
