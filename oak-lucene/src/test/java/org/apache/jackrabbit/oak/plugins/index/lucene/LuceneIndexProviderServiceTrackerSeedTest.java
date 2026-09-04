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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService.FT_SYNC_TRACKER_INIT_OAK_12173_DISABLE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

// OAK-12173
public class LuceneIndexProviderServiceTrackerSeedTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    private static NodeState rootWithFullyBuiltLuceneIndex() throws Exception {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        newLucenePropertyIndexDefinition(index, "lucene", Set.of("foo"), "async");

        NodeState before = builder.getNodeState();
        builder.setProperty("foo", "bar");
        NodeState after = builder.getNodeState();

        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new LuceneIndexEditorProvider(), "async", false));
        // Runs the index editor synchronously so the returned NodeState already
        // has a ":data" child under /oak:index/lucene - i.e. an index that is
        // already fully built by the time the repository "starts up" in this test.
        return hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    private void registerCommonServices(NodeStore nodeStore) {
        context.registerService(MountInfoProvider.class, Mounts.newBuilder().build());
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        context.registerService(IndexAugmentorFactory.class, new IndexAugmentorFactory());
        context.registerService(NodeStore.class, nodeStore);
        context.registerService(IndexPathService.class, mock(IndexPathService.class));
        context.registerService(AsyncIndexInfoService.class, mock(AsyncIndexInfoService.class));
        context.registerService(CheckpointMBean.class, mock(CheckpointMBean.class));
    }

    private Map<String, Object> defaultConfig() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("localIndexDir", folder.getRoot().getAbsolutePath());
        return config;
    }

    @After
    public void tearDown() {
        FT_SYNC_TRACKER_INIT_OAK_12173_DISABLE.set(false);
    }

    @Test
    public void indexIsQueryableImmediatelyAfterActivation() throws Exception {
        NodeState prebuilt = rootWithFullyBuiltLuceneIndex();
        registerCommonServices(new MemoryNodeStore(prebuilt));

        LuceneIndexProviderService service = new LuceneIndexProviderService();
        MockOsgi.injectServices(service, context.bundleContext());
        // enableOpenIndexAsync defaults to true, i.e. the Observer is wrapped in a
        // BackgroundObserver whose queue is never drained in this test - if the
        // tracker were only ever seeded through that Observer, acquireIndexNode()
        // below would see an empty tracker and return null.
        MockOsgi.activate(service, context.bundleContext(), defaultConfig());

        IndexTracker tracker = (IndexTracker) FieldUtils.readDeclaredField(service, "tracker", true);
        LuceneIndexNode indexNode = tracker.acquireIndexNode("/oak:index/lucene");

        assertNotNull("Index built before startup must be usable immediately after "
                + "activate(), without waiting for the BackgroundObserver thread", indexNode);
        if (indexNode != null) {
            indexNode.release();
        }

        MockOsgi.deactivate(service, context.bundleContext());
    }

    @Test
    public void indexIsNotQueryableAfterActivationWhenToggleDisabled() throws Exception {
        // Reproduces the race this fix closes: with the synchronous seed
        // turned off, the tracker is only ever seeded through the Observer -
        // which, exactly as in the test above, is queued behind a
        // BackgroundObserver that is never drained here. This is the
        // pre-fix/toggled-off behavior, and it must actually fail to find the
        // index (proving the toggle is load-bearing, not a no-op).
        FT_SYNC_TRACKER_INIT_OAK_12173_DISABLE.set(true);

        NodeState prebuilt = rootWithFullyBuiltLuceneIndex();
        registerCommonServices(new MemoryNodeStore(prebuilt));

        LuceneIndexProviderService service = new LuceneIndexProviderService();
        MockOsgi.injectServices(service, context.bundleContext());
        MockOsgi.activate(service, context.bundleContext(), defaultConfig());

        IndexTracker tracker = (IndexTracker) FieldUtils.readDeclaredField(service, "tracker", true);
        LuceneIndexNode indexNode = tracker.acquireIndexNode("/oak:index/lucene");

        assertNull("With the synchronous seed disabled, an index built before startup "
                + "must NOT be usable immediately after activate() - this is the race "
                + "the toggle exists to close", indexNode);
        if (indexNode != null) {
            indexNode.release();
        }

        MockOsgi.deactivate(service, context.bundleContext());
    }
}
