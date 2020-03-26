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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;

public class ElasticsearchIndexProviderServiceTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final ElasticsearchIndexProviderService service = new ElasticsearchIndexProviderService();

    private Whiteboard wb;

    @Before
    public void setUp() {
        MountInfoProvider mip = Mounts.newBuilder().build();
        context.registerService(MountInfoProvider.class, mip);
        context.registerService(NodeStore.class, new MemoryNodeStore());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);

        wb = new OsgiWhiteboard(context.bundleContext());
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @Test
    public void defaultSetup() throws Exception {
        MockOsgi.activate(service, context.bundleContext(),
                Collections.singletonMap("localTextExtractionDir", folder.newFolder("localTextExtractionDir").getAbsolutePath())
        );

        assertNotNull(context.getService(QueryIndexProvider.class));
        assertNotNull(context.getService(IndexEditorProvider.class));

        assertNotNull(WhiteboardUtils.getServices(wb, Runnable.class));

        MockOsgi.deactivate(service, context.bundleContext());
    }

}
