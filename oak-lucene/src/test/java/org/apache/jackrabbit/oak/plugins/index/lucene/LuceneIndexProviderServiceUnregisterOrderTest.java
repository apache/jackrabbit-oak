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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceEvent;
import org.osgi.framework.ServiceListener;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService.FT_OAK_12469;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

// OAK-12469
public class LuceneIndexProviderServiceUnregisterOrderTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    private LuceneIndexProviderService service;

    @Before
    public void setUp() {
        context.registerService(MountInfoProvider.class, Mounts.newBuilder().build());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        context.registerService(IndexAugmentorFactory.class, new IndexAugmentorFactory());
        context.registerService(NodeStore.class, new MemoryNodeStore(INITIAL_CONTENT));
        context.registerService(IndexPathService.class, mock(IndexPathService.class));
        context.registerService(AsyncIndexInfoService.class, mock(AsyncIndexInfoService.class));
        context.registerService(CheckpointMBean.class, mock(CheckpointMBean.class));

        service = new LuceneIndexProviderService();
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @After
    public void tearDown() {
        FT_OAK_12469.set(true);
    }

    @Test
    public void queryIndexProviderOutlivesIndexEditorProviderOnDeactivate() throws Exception {
        activate();
        List<Boolean> queryProviderPresent = recordQueryProviderPresenceWhenEditorUnregisters();

        MockOsgi.deactivate(service, context.bundleContext());

        // The repository depends on the IndexEditorProvider only, so it must stop
        // before the QueryIndexProvider goes away; otherwise its queries traverse.
        assertEquals(List.of(true), queryProviderPresent);
    }

    @Test
    public void queryIndexProviderGoesFirstWhenToggleDisabled() throws Exception {
        FT_OAK_12469.set(false);
        activate();
        List<Boolean> queryProviderPresent = recordQueryProviderPresenceWhenEditorUnregisters();

        MockOsgi.deactivate(service, context.bundleContext());

        assertEquals(List.of(false), queryProviderPresent);
    }

    private void activate() {
        MockOsgi.activate(service, context.bundleContext(),
                Map.of("localIndexDir", folder.getRoot().getAbsolutePath()));
    }

    private List<Boolean> recordQueryProviderPresenceWhenEditorUnregisters() {
        BundleContext bc = context.bundleContext();
        List<Boolean> result = new ArrayList<>();
        ServiceListener listener = (ServiceEvent event) -> {
            Object service = bc.getService(event.getServiceReference());
            if (event.getType() == ServiceEvent.UNREGISTERING && service instanceof IndexEditorProvider) {
                result.add(bc.getServiceReference(QueryIndexProvider.class) != null);
            }
        };
        bc.addServiceListener(listener);
        return result;
    }
}
