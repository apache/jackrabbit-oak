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
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.cache.api.CacheBuilder.MaintenanceMode;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Loader behavior with entry-owned ASYNC metadata. */
public class AsyncMemoryDiffCacheTest {
    private AutoCloseable features;

    @Before
    public void enableAsyncMaintenance() {
        features = DocumentCacheFeatureTestSupport.enableAsyncMaintenance();
    }

    @After
    public void restoreFeatures() throws Exception {
        features.close();
    }

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    /** A loaded diff remains readable after eviction and reopen. */
    @Test
    public void loadedDiffShouldSurviveAsyncEvictionAndReopen() throws Exception {
        boolean previous = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.getAndSet(true);
        boolean previousCaffeine = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        try {
            for (MaintenanceMode mode : new MaintenanceMode[] {MaintenanceMode.ASYNC}) {
                assertLoadedDiffSurvivesEviction(mode);
            }
        } finally {
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previous);
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previousCaffeine);
        }
    }

    private void assertLoadedDiffSurvivesEviction(MaintenanceMode mode) throws Exception {
        String directory = temporaryFolder.newFolder().getAbsolutePath() + ",+asyncDiff";
        RevisionVector from = new RevisionVector(new Revision(1, 0, 1));
        RevisionVector to = new RevisionVector(new Revision(2, 0, 1));
        String changes = "^\"child\":{}";
        DocumentNodeStoreBuilder<?> builder = builderProvider.newBuilder().setPersistentCache(directory)
                .setCacheMaintenanceMode(CacheType.DIFF, mode);
        PersistentCache persistence = builder.getPersistentCache();
        try {
            MemoryDiffCache cache = new MemoryDiffCache(builder);
            Assert.assertEquals(changes, cache.getChanges(from, to, Path.ROOT, () -> changes));
            cache.diffCache.setMaximumWeight(0);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (cache.getChanges(from, to, Path.ROOT, null) == null && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertEquals("Evicted loaded diff must reach persistent storage", changes,
                    cache.getChanges(from, to, Path.ROOT, null));
        } finally {
            persistence.close();
        }
        DocumentNodeStoreBuilder<?> reopened = builderProvider.newBuilder().setPersistentCache(directory)
                .setCacheMaintenanceMode(CacheType.DIFF, mode);
        try {
            Assert.assertEquals(changes, new MemoryDiffCache(reopened).getChanges(from, to, Path.ROOT, null));
        } finally {
            reopened.getPersistentCache().close();
        }
    }

    /** Recursive loading must not overwrite the first published diff. */
    @Test
    public void recursiveDiffLoaderShouldKeepFirstCachedValue() throws Exception {
        MemoryDiffCache cache = new MemoryDiffCache(builderProvider.newBuilder()
                .setCacheMaintenanceMode(CacheType.DIFF, MaintenanceMode.ASYNC)
                .setPersistentCache(temporaryFolder.newFolder().getAbsolutePath() + ",+asyncDiff"));
        RevisionVector from = new RevisionVector(new Revision(1, 0, 1));
        RevisionVector to = new RevisionVector(new Revision(2, 0, 1));
        String outer = "^\"outer\":{}";
        String inner = "^\"inner\":{}";

        Assert.assertEquals(outer, cache.getChanges(from, to, Path.ROOT, () -> {
            Assert.assertEquals(inner, cache.getChanges(from, to, Path.ROOT, () -> inner));
            return outer;
        }));
        Assert.assertEquals(inner, cache.getChanges(from, to, Path.ROOT, null));
    }

}
