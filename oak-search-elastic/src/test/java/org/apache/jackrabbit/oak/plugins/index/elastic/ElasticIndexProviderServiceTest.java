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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_ELASTIC_API_KEY_ID;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_ELASTIC_API_KEY_SECRET;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_ELASTIC_HOST;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_ELASTIC_PORT;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_ELASTIC_SCHEME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_INDEX_PREFIX;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexProviderService.PROP_LOCAL_TEXT_EXTRACTION_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class ElasticIndexProviderServiceTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    private final ElasticIndexProviderService service = new ElasticIndexProviderService();

    private Whiteboard wb;

    @Before
    public void setUp() {
        MountInfoProvider mip = Mounts.newBuilder().build();
        context.registerService(MountInfoProvider.class, mip);
        context.registerService(NodeStore.class, new MemoryNodeStore());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        context.registerService(AsyncIndexInfoService.class, mock(AsyncIndexInfoService.class));

        wb = new OsgiWhiteboard(context.bundleContext());
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @Test
    public void defaultSetup() {
        MockOsgi.activate(service, context.bundleContext());

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(IndexEditorProvider.class));

        // With the default setup and no elastic cluster available at localhost:9200 the index cleaner will not be registered.
        // This check can potentially fail if a local cluster is up and running.
        assertEquals(0, WhiteboardUtils.getServices(wb, Runnable.class).size());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void withElasticSetup() {
        MockOsgi.activate(service, context.bundleContext(), getElasticConfig());

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(IndexEditorProvider.class));

        assertEquals(0, WhiteboardUtils.getServices(wb, Runnable.class).size());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void withIndexCleanerSetup() {
        Map<String, Object> props = new HashMap<>(getElasticConfig());
        props.put("remoteIndexCleanupFrequency", 600);
        MockOsgi.activate(service, context.bundleContext(), props);

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(IndexEditorProvider.class));

        assertEquals(1, WhiteboardUtils.getServices(wb, Runnable.class).size());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void disabled() {
        MockOsgi.activate(service, context.bundleContext(), Map.of(PROP_DISABLED, true));

        assertNull(context.getService(QueryIndexProvider.class));
        assertNull(context.getService(IndexEditorProvider.class));

        assertEquals(0, WhiteboardUtils.getServices(wb, Runnable.class).size());

        MockOsgi.deactivate(service, context.bundleContext());
    }

    private HashMap<String, Object> getElasticConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put(PROP_INDEX_PREFIX, "elastic");
        config.put(PROP_ELASTIC_SCHEME, elasticRule.getElasticConnectionModel().getScheme());
        config.put(PROP_ELASTIC_HOST, elasticRule.getElasticConnectionModel().getElasticHost());
        config.put(PROP_ELASTIC_PORT, elasticRule.getElasticConnectionModel().getElasticPort());
        config.put(PROP_ELASTIC_API_KEY_ID, elasticRule.getElasticConnectionModel().getElasticApiKey());
        config.put(PROP_ELASTIC_API_KEY_SECRET, elasticRule.getElasticConnectionModel().getElasticApiSecret());
        try {
            config.put(PROP_LOCAL_TEXT_EXTRACTION_DIR, folder.newFolder("localTextExtractionDir").getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return config;
    }

}
