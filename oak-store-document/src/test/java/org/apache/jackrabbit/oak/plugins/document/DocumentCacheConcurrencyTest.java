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
import org.apache.jackrabbit.oak.cache.api.CacheBuilder.MaintenanceMode;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/** Tests cross-cache loading in {@link DocumentNodeStore}. */
public class DocumentCacheConcurrencyTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    /** Verifies a node read does not wait for an in-flight children load. */
    @Test
    public void coldNodeReadCompletesDuringChildrenLoad() throws Exception {
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.get();
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.getAndSet(false);
        try {
            for (boolean caffeine : new boolean[] {false, true}) {
                DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(caffeine);
                verifyColdNodeRead(MaintenanceMode.SYNC, MaintenanceMode.SYNC);
            }
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(true);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(true);
            verifyColdNodeRead(MaintenanceMode.ASYNC, MaintenanceMode.ASYNC);
            verifyColdNodeRead(MaintenanceMode.ASYNC, MaintenanceMode.SYNC);
            verifyColdNodeRead(MaintenanceMode.SYNC, MaintenanceMode.ASYNC);
        } finally {
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    /** ASYNC avoids the disk-backed children lookup while SYNC retains that optimization. */
    @Test
    public void persistentChildrenLookupIsBypassedOnlyForEntryOwnedCaches() throws Exception {
        try (AutoCloseable features = DocumentCacheFeatureTestSupport.enableAsyncMaintenance()) {
            for (MaintenanceMode nodeMode : MaintenanceMode.values()) {
                for (MaintenanceMode childrenMode : MaintenanceMode.values()) {
                    MemoryDocumentStore documents = Mockito.spy(new MemoryDocumentStore());
                    DocumentNodeStoreBuilder<?> builder = new DocumentNodeStoreBuilder<>()
                            .setDocumentStore(documents).setAsyncDelay(0)
                            .setPersistentCache(temporaryFolder.newFolder().getAbsolutePath() + ",-async")
                            .setCacheMaintenanceMode(CacheType.NODE, nodeMode)
                            .setCacheMaintenanceMode(CacheType.CHILDREN, childrenMode);
                    DocumentNodeStore store = builder.build();
                    try {
                        RevisionVector revision = store.getRoot().getLastRevision();
                        NamePathRev key = new NamePathRev("", Path.ROOT, revision);
                        DocumentNodeState.Children children = new DocumentNodeState.Children();
                        store.getNodeChildrenCache().put(key, children);
                        store.getNodeChildrenCache().asMap().remove(key);
                        Assert.assertNull(store.getNodeChildrenCache().asMap().get(key));
                        Mockito.clearInvocations(documents);
                        Path missing = Path.fromString("/missing");
                        Assert.assertNull(store.getNode(missing, revision));
                        boolean bypass = nodeMode == MaintenanceMode.ASYNC || childrenMode == MaintenanceMode.ASYNC;
                        Mockito.verify(documents, Mockito.times(bypass ? 1 : 0))
                                .find(Collection.NODES, Utils.getIdFromPath(missing));
                        Assert.assertEquals(children, store.getNodeChildrenCache().getIfPresent(key));
                    } finally {
                        store.dispose();
                    }
                }
            }
        }
    }

    private void verifyColdNodeRead(MaintenanceMode mode, MaintenanceMode childrenMode) throws Exception {
        PausingDocumentStore documents = new PausingDocumentStore();
        DocumentNodeStoreBuilder<?> builder = new DocumentNodeStoreBuilder<>()
                .setDocumentStore(documents)
                .setAsyncDelay(0)
                .setPersistentCache(temporaryFolder.newFolder().getAbsolutePath())
                .setCacheMaintenanceMode(CacheType.NODE, mode)
                .setCacheMaintenanceMode(CacheType.CHILDREN, childrenMode);
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
            Assert.assertTrue("Children loader did not start: " + mode,
                    documents.entered.await(10, TimeUnit.SECONDS));
            Future<DocumentNodeState> node = executor.submit(() ->
                    store.getNode(Path.fromString("/a"), parent.getLastRevision()));
            Assert.assertNotNull("Node read must complete while children load is paused: " + mode,
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
