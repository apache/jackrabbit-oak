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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdateTest.CommitInfoCollector;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexerService.AsyncConfig;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.plugins.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.*;

public class AsyncIndexerServiceTest {
    @Rule
    public final OsgiContext context = new OsgiContext();

    private MemoryNodeStore nodeStore = new MemoryNodeStore();
    private AsyncIndexerService service = new AsyncIndexerService();

    @Before
    public void setUp() {
        context.registerService(NodeStore.class, nodeStore);
        context.registerService(ValidatorProvider.class, new ChangeCollectorProvider());
        MockOsgi.injectServices(service, context.bundleContext());
    }

    @Test
    public void asyncReg() throws Exception{
        Map<String,Object> config = ImmutableMap.<String, Object>of(
                "asyncConfigs", new String[] {"async:5"}
        );
        MockOsgi.activate(service, context.bundleContext(), config);
        assertNotNull(context.getService(Runnable.class));
        assertEquals(TimeUnit.MINUTES.toMillis(15), getIndexUpdate("async").getLeaseTimeOut());

        MockOsgi.deactivate(service);
        assertNull(context.getService(Runnable.class));
    }

    @Test
    public void leaseTimeout() throws Exception{
        Map<String,Object> config = ImmutableMap.<String, Object>of(
                "asyncConfigs", new String[] {"async:5"},
                "leaseTimeOutMinutes" , "20"
        );
        MockOsgi.activate(service, context.bundleContext(), config);
        AsyncIndexUpdate indexUpdate = getIndexUpdate("async");
        assertEquals(TimeUnit.MINUTES.toMillis(20), indexUpdate.getLeaseTimeOut());
    }

    @Test
    public void changeCollectionEnabled() throws Exception{
        Map<String,Object> config = ImmutableMap.<String, Object>of(
                "asyncConfigs", new String[] {"async:5"}
        );
        context.registerService(IndexEditorProvider.class, new PropertyIndexEditorProvider());
        MockOsgi.activate(service, context.bundleContext(), config);

        NodeBuilder builder = nodeStore.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        CommitInfoCollector infoCollector = new CommitInfoCollector();
        nodeStore.addObserver(infoCollector);

        AsyncIndexUpdate indexUpdate = getIndexUpdate("async");
        indexUpdate.run();

        CommitContext commitContext = (CommitContext) infoCollector.infos.get(0).getInfo().get(CommitContext.NAME);
        assertNotNull(commitContext);
        ChangeSet changeSet = (ChangeSet) commitContext.get(ChangeCollectorProvider.COMMIT_CONTEXT_OBSERVATION_CHANGESET);
        assertNotNull(changeSet);
    }

    private AsyncIndexUpdate getIndexUpdate(String name) {
        return (AsyncIndexUpdate) context.getServices(Runnable.class, "(oak.async="+name+")")[0];
    }

    @Test
    public void configParsing() throws Exception{
        List<AsyncConfig> configs = AsyncIndexerService.getAsyncConfig(new String[]{"async:15"});
        assertEquals(1, configs.size());
        assertEquals("async", configs.get(0).name);
        assertEquals(15, configs.get(0).timeIntervalInSecs);

        configs = AsyncIndexerService.getAsyncConfig(new String[]{"async:15", "foo:23"});
        assertEquals(2, configs.size());
        assertEquals("foo", configs.get(1).name);
        assertEquals(23, configs.get(1).timeIntervalInSecs);
    }
}