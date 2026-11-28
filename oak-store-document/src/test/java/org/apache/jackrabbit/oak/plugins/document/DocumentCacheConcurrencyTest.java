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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests cross-cache loading in {@link DocumentNodeStore}. */
public class DocumentCacheConcurrencyTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    /** Verifies a node read does not wait for an in-flight children load. */
    @Test
    public void coldNodeReadCompletesDuringChildrenLoad() throws Exception {
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get();
        try {
            for (boolean caffeine : new boolean[] {false, true}) {
                DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
                verifyColdNodeRead(caffeine);
            }
        } finally {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
        }
    }

    private void verifyColdNodeRead(boolean caffeine) throws Exception {
        PausingDocumentStore documents = new PausingDocumentStore();
        DocumentNodeStoreBuilder<?> builder = new DocumentNodeStoreBuilder<>()
                .setDocumentStore(documents)
                .setAsyncDelay(0)
                .setPersistentCache(temporaryFolder.newFolder().getAbsolutePath());
        DocumentNodeStore store = builder.build();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            NodeBuilder root = store.getRoot().builder();
            root.child("a");
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            DocumentNodeState parent = store.getRoot();
            store.getNodeCache().invalidateAll();
            store.getNodeChildrenCache().invalidateAll();
            documents.pause = true;
            Future<DocumentNodeState.Children> children = executor.submit(() -> store.getChildren(parent, "", 10));
            Assert.assertTrue("Children loader did not start: " + caffeine,
                    documents.entered.await(10, TimeUnit.SECONDS));
            Future<DocumentNodeState> node = executor.submit(() ->
                    store.getNode(Path.fromString("/a"), parent.getLastRevision()));
            Assert.assertNotNull("Node read must complete while children load is paused: " + caffeine,
                    node.get(5, TimeUnit.SECONDS));
            documents.release.countDown();
            Assert.assertTrue(children.get(10, TimeUnit.SECONDS).children.contains("a"));
        } finally {
            // Stop the paused loader without entering the reverse dependency if the assertion fails.
            documents.abort = true;
            documents.release.countDown();
            executor.shutdownNow();
            Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            store.dispose();
        }
    }

    private static final class PausingDocumentStore extends MemoryDocumentStore {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private volatile boolean pause;
        private volatile boolean abort;

        @Override
        public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
            if (pause && collection == Collection.NODES) {
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting to release children loader");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Children loader interrupted", e);
                }
                if (abort) {
                    return Collections.emptyList();
                }
            }
            return super.query(collection, fromKey, toKey, limit);
        }
    }
}
