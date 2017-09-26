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
package org.apache.jackrabbit.oak.plugins.blob;

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import junit.framework.Assert;

/**
 * Tests the ClusterRepositoryInfo unique cluster repository id.
 */
public class ClusterRepositoryInfoTest {
    static BlobStore blobStore;

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    
    /**
     * test method to change the clusterId explicitly
     * 
     * @throws CommitFailedException
     **/
    private static void setId(NodeStore store, String clusterId) throws CommitFailedException {
        NodeState root = store.getRoot();
        NodeBuilder builder = root.builder();
        builder.child(ClusterRepositoryInfo.CLUSTER_CONFIG_NODE)
            .setProperty(ClusterRepositoryInfo.CLUSTER_ID_PROP, clusterId);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    @BeforeClass
    public static void setup() {
        try {
            blobStore = DataStoreUtils.getBlobStore();
            Assume.assumeThat(blobStore, instanceOf(SharedDataStore.class));
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }
    }

    @Test
    public void differentCluster() throws Exception {
        DocumentNodeStore ds1 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobStore)
                .getNodeStore();
        String repoId1 = ClusterRepositoryInfo.getOrCreateId(ds1);

        DocumentNodeStore ds2 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(new MemoryDocumentStore())
                .setBlobStore(blobStore)
                .getNodeStore();
        String repoId2 = ClusterRepositoryInfo.getOrCreateId(ds2);

        Assert.assertNotSame(repoId1, repoId2);
    }

    @Test
    public void sameCluster() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ds1 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setClusterId(1)
                .setBlobStore(blobStore)
                .getNodeStore();
        String repoId1 = ClusterRepositoryInfo.getOrCreateId(ds1);
        ds1.runBackgroundOperations();

        DocumentNodeStore ds2 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setClusterId(2)
                .setBlobStore(blobStore)
                .getNodeStore();
        String repoId2 = ClusterRepositoryInfo.getOrCreateId(ds2);

        // Since the same cluster the ids should be equal
        Assert.assertEquals(repoId1, repoId2);
    }

    // below test doesn't make sense anymore in the context
    // of getOrCreateId (OAK-4006) where that never returns null
//    @Test
//    public void checkGetIdWhenNotRegistered() {
//        MemoryDocumentStore store = new MemoryDocumentStore();
//        DocumentNodeStore ds1 = builderProvider.newBuilder()
//            .setAsyncDelay(0)
//            .setDocumentStore(store)
//            .setClusterId(1)
//            .getNodeStore();
//        // Should be null and no NPE
//        String id = ClusterRepositoryInfo.getOrCreateId(ds1);
//        Assert.assertNull(id);
//    }

    @Test
    public void checkCustomId() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ds1 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setClusterId(1)
                .getNodeStore();
        String repoId1 = "yyyyyyy";
        setId(ds1, repoId1);
        ds1.runBackgroundOperations();

        String id = ClusterRepositoryInfo.getOrCreateId(ds1);
        Assert.assertEquals(id, repoId1);
    }

    @Test
    public void checkChangeId() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ds1 = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setClusterId(1)
                .getNodeStore();
        String repoId1 = ClusterRepositoryInfo.getOrCreateId(ds1);
        ds1.runBackgroundOperations();

        // Change with a custom ID
        setId(ds1, "xxxxxxxx");

        String id = ClusterRepositoryInfo.getOrCreateId(ds1);
        Assert.assertNotNull(id);
        Assert.assertEquals(id, "xxxxxxxx");
    }


    @After
    public void close() throws IOException {
        FileUtils.cleanDirectory(new File(DataStoreUtils.getHomeDir()));
    }
}

