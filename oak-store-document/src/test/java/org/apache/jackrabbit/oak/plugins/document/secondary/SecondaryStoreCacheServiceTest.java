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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStateCache;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SecondaryStoreCacheServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private SecondaryStoreCacheService cacheService = new SecondaryStoreCacheService();
    private NodeStore secondaryStore = new MemoryNodeStore();

    @Before
    public void configureDefaultServices(){
        context.registerService(BlobStore.class, new MemoryBlobStore());
        context.registerService(NodeStoreProvider.class, new NodeStoreProvider() {
            @Override
            public NodeStore getNodeStore() {
                return secondaryStore;
            }
        }, ImmutableMap.<String, Object>of("role", "secondary"));
        context.registerService(Executor.class, Executors.newSingleThreadExecutor());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        MockOsgi.injectServices(cacheService, context.bundleContext());
    }

    @Test
    public void defaultSetup() throws Exception{
        MockOsgi.activate(cacheService, context.bundleContext(), new HashMap<String, Object>());

        assertNotNull(context.getService(Observer.class));
        assertNotNull(context.getService(BackgroundObserverMBean.class));
        assertNotNull(context.getService(DocumentNodeStateCache.class));

        MockOsgi.deactivate(cacheService, context.bundleContext());

        assertNull(context.getService(Observer.class));
        assertNull(context.getService(BackgroundObserverMBean.class));
        assertNull(context.getService(DocumentNodeStateCache.class));
    }

    @Test
    public void disableBackground() throws Exception{
        Map<String, Object> config = new HashMap<>();
        config.put("enableAsyncObserver", "false");
        MockOsgi.activate(cacheService, context.bundleContext(), config);

        assertNotNull(context.getService(Observer.class));
        assertNull(context.getService(BackgroundObserverMBean.class));
        assertNotNull(context.getService(DocumentNodeStateCache.class));
    }

    @Test
    public void configurePathFilter() throws Exception{
        Map<String, Object> config = new HashMap<>();
        config.put("includedPaths", new String[] {"/a"});
        MockOsgi.activate(cacheService, context.bundleContext(), config);

        assertEquals(PathFilter.Result.INCLUDE, cacheService.getPathFilter().filter("/a"));
        assertEquals(PathFilter.Result.EXCLUDE, cacheService.getPathFilter().filter("/b"));
    }

}