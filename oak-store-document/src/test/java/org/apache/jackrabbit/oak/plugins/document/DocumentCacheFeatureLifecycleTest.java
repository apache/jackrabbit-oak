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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineCacheAdapter;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests the registered cache toggle in {@link DocumentNodeStoreService}. */
public class DocumentCacheFeatureLifecycleTest {
    @Rule
    public OsgiContext context = new OsgiContext();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /** Existing caches retain their policy and a restarted service preserves the toggle state. */
    @Test
    public void registeredToggleIsSampledAtConstructionAndSurvivesReactivation() throws Exception {
        JdbcConnectionPool dataSource = JdbcConnectionPool.create(
                "jdbc:h2:" + folder.newFolder().getAbsolutePath() + "/repository", "sa", "");
        DocumentNodeStoreService service = null;
        boolean previous = DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.getAndSet(true);
        boolean previousAsync = DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.getAndSet(false);
        try {
            context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
            context.registerInjectActivateService(new DocumentNodeStoreService.Preset());
            context.registerService(DataSource.class, dataSource, Collections.singletonMap("datasource.name", "oak"));
            Map<String, Object> config = new HashMap<>();
            config.put("documentStoreType", "RDB");
            config.put("persistentCache", "-");
            config.put("journalCache", "-");
            config.put("repository.home", folder.newFolder().getAbsolutePath());
            MockOsgi.setConfigForPid(context.bundleContext(), DocumentNodeStoreService.class.getName(), config);
            service = activateService();
            Assert.assertFalse(asyncToggle().isEnabled());
            asyncToggle().setEnabled(true);
            Cache<PathRev, DocumentNodeState> existing = context.getService(DocumentNodeStore.class).getNodeCache();
            Assert.assertTrue(existing instanceof CaffeineCacheAdapter);
            toggle().setEnabled(false);
            Assert.assertSame(existing, context.getService(DocumentNodeStore.class).getNodeCache());
            Assert.assertTrue(existing instanceof CaffeineCacheAdapter);
            Cache<CacheValue, NodeDocument> newlyConstructed = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder()
                    .buildDocumentCache(new MemoryDocumentStore());
            Assert.assertEquals("LirsLoadingCacheAdapter", newlyConstructed.getClass().getSimpleName());
            asyncToggle().setEnabled(true);
            MockOsgi.deactivate(service, context.bundleContext());
            service = null;
            Assert.assertFalse(Arrays.stream(context.getServices(FeatureToggle.class, null))
                    .anyMatch(candidate -> DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE.equals(candidate.getName())));
            Assert.assertFalse(Arrays.stream(context.getServices(FeatureToggle.class, null))
                    .anyMatch(candidate -> DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE
                            .equals(candidate.getName())));
            service = activateService();
            Assert.assertFalse(toggle().isEnabled());
            Assert.assertTrue(asyncToggle().isEnabled());
            Assert.assertEquals("LirsLoadingCacheAdapter",
                    context.getService(DocumentNodeStore.class).getNodeCache().getClass().getSimpleName());
        } finally {
            if (service != null) {
                MockOsgi.deactivate(service, context.bundleContext());
            }
            dataSource.dispose();
            DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE_ENABLED.set(previous);
            DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE_ENABLED.set(previousAsync);
        }
    }

    private DocumentNodeStoreService activateService() {
        DocumentNodeStoreService service = new DocumentNodeStoreService();
        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext());
        return service;
    }

    private FeatureToggle toggle() {
        return Arrays.stream(context.getServices(FeatureToggle.class, null))
                .filter(candidate -> DocumentNodeStoreBuilder.FT_CAFFEINE_CACHE.equals(candidate.getName()))
                .findFirst().orElseThrow(() -> new AssertionError("Cache feature toggle is not registered"));
    }

    private FeatureToggle asyncToggle() {
        return Arrays.stream(context.getServices(FeatureToggle.class, null))
                .filter(candidate -> DocumentNodeStoreBuilder.FT_DOCUMENT_CACHE_ASYNC_MAINTENANCE
                        .equals(candidate.getName()))
                .findFirst().orElseThrow(() -> new AssertionError("Async cache maintenance toggle is not registered"));
    }
}
