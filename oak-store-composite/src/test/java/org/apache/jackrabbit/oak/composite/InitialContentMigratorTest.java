/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo.CLUSTER_CONFIG_NODE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

public class InitialContentMigratorTest {

    @Test
    public void migrateContentWithCheckpoints() throws IOException, CommitFailedException {

        // 1. populate the seed store with
        // .
        // \- first
        // \- second
        // \- third
        //
        // 2. checkpoint before adding the third node
        //
        // 3. the mount only includes the '/first' path, so only the
        // 'second' and 'third' nodes should be available

        MemoryNodeStore seed = new MemoryNodeStore();
        NodeBuilder root = seed.getRoot().builder();
        root.child("first");
        root.child("second");
        seed.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        String checkpoint1 = seed.checkpoint(TimeUnit.MINUTES.toMillis(10));
        
        root = seed.getRoot().builder();
        root.child("third");
        seed.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        
        MemoryNodeStore target = new MemoryNodeStore();
        NodeBuilder targetRootBuilder = target.getRoot().builder();
        targetRootBuilder.child(CLUSTER_CONFIG_NODE);
        target.merge(targetRootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        MountInfoProvider mip = Mounts.newBuilder().mount("seed", "/first").build();
        
        // perform migration
        InitialContentMigrator icm = new InitialContentMigrator(target, seed, mip.getMountByName("seed"));
        icm.migrate();
        
        NodeState targetRoot = target.getRoot();
        
        // verify that the 'second' and 'third' nodes are visible in the migrated store
        assertFalse("Node /first should not have been migrated", targetRoot.hasChildNode("first"));
        assertTrue("Node /second should have been migrated", targetRoot.hasChildNode("second"));
        assertTrue("Node /third should have been migrated", targetRoot.hasChildNode("third"));
        assertTrue(CLUSTER_CONFIG_NODE  + " should be retained", targetRoot.hasChildNode(CLUSTER_CONFIG_NODE));

        // verify that the 'second' node is visible in the migrated store when retrieving the checkpoint
        NodeState checkpointTargetRoot = target.retrieve(checkpoint1);
        assertFalse("Node /first should not have been migrated", checkpointTargetRoot.hasChildNode("first"));
        assertTrue("Node /second should have been migrated", checkpointTargetRoot.hasChildNode("second"));
        assertFalse("Node /third should not be visible from the migrated checkpoint", checkpointTargetRoot.hasChildNode("third"));

    }

    @Test
    public void clusterInitialization() throws CommitFailedException, InterruptedException {
        MemoryNodeStore seed = new MemoryNodeStore();
        NodeBuilder root = seed.getRoot().builder();
        root.child("first");
        root.child("second");
        root.child("third");
        for (int i = 0; i < 10; i++) {
            NodeBuilder b = root.child("third").child("a-" + i);
            for (int j = 0; j < 50; j++) {
                b.child(("b-") + j);
            }
        }
        seed.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        MountInfoProvider mip = Mounts.newBuilder().mount("seed", "/first").build();

        DocumentStore sharedStore = new MemoryDocumentStore();
        List<DocumentNodeStore> stores = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            stores.add(new DocumentMK.Builder()
                    .setDocumentStore(sharedStore)
                    .setClusterId(i + 1)
                    .build());
        }

        List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean migrated = new AtomicBoolean();
        List<Thread> threads = stores.stream()
                .map(dns -> (Runnable) () -> runMigration(dns, seed, mip.getMountByName("seed"), exceptions, migrated))
                .map(Thread::new)
                .collect(Collectors.toList());

        threads.stream().forEach(Thread::start);
        for (Thread t : threads) {
            t.join();
        }

        assertTrue("Exception list should be empty: " + exceptions, exceptions.isEmpty());

        for (DocumentNodeStore dns : stores) {
            NodeState targetRoot = dns.getRoot();

            // verify that the 'second' and 'third' nodes are visible in the migrated store
            assertFalse("Node /first should not have been migrated", targetRoot.hasChildNode("first"));
            assertTrue("Node /second should have been migrated", targetRoot.hasChildNode("second"));
            assertTrue("Node /third should have been migrated", targetRoot.hasChildNode("third"));

            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 10; j++) {
                    assertTrue("Node /third/" + i + "/" + j + " should have been migrated",
                            targetRoot.getChildNode("third").getChildNode("a-" + i).hasChildNode("b-" + j));
                }
            }

            dns.dispose();
        }
    }

    private void runMigration(NodeStore target, NodeStore seed, Mount seedMount, List<Throwable> exceptions, AtomicBoolean migrated) {
        try {
            InitialContentMigrator icm = new InitialContentMigrator(target, seed, seedMount) {
                protected void doMigrate() throws CommitFailedException {
                    if (migrated.getAndSet(true)) {
                        fail("doMigrate() has been called more than once.");
                    }
                    super.doMigrate();
                }
            };
            icm.migrate();
        } catch (Throwable e) {
            exceptions.add(e);
        }
    }
}
