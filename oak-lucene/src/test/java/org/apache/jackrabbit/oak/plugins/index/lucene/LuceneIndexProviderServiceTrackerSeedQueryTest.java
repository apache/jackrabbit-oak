/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexInfoService;
import org.apache.jackrabbit.oak.plugins.index.IndexPathService;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
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
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProviderService.FT_OAK_12173;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper.newLucenePropertyIndexDefinition;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Same test as {@link LuceneIndexProviderServiceTrackerSeedTest}, but end-to-end:
 * instead of calling {@link IndexTracker#acquireIndexNode(String)} directly, this
 * runs a real query and checks the query plan - the way a real caller would see
 * the fix (or the bug, when the toggle is off).
 */
public class LuceneIndexProviderServiceTrackerSeedQueryTest {

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
        // Indexes the content right away, so the index is already built
        // (has a ":data" child) before this test "starts up" the repository.
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
        FT_OAK_12173.set(true);
    }

    /**
     * Starts a real {@link LuceneIndexProviderService} on a NodeStore that already
     * has a fully-built lucene index (as if it was built before this process
     * started), builds a queryable repository from it, and right away runs an
     * {@code explain} query - returning the resulting query plan as a string.
     */
    private String explainQueryImmediatelyAfterActivation(boolean toggleEnabled) throws Exception {
        FT_OAK_12173.set(toggleEnabled);

        NodeState prebuilt = rootWithFullyBuiltLuceneIndex();
        NodeStore nodeStore = new MemoryNodeStore(prebuilt);
        registerCommonServices(nodeStore);

        LuceneIndexProviderService service = new LuceneIndexProviderService();
        MockOsgi.injectServices(service, context.bundleContext());
        // Same setup as LuceneIndexProviderServiceTrackerSeedTest: this test
        // never processes the background queue updates normally go through.
        MockOsgi.activate(service, context.bundleContext(), defaultConfig());
        try {
            QueryIndexProvider indexProvider =
                    (QueryIndexProvider) FieldUtils.readDeclaredField(service, "indexProvider", true);

            ContentRepository repo = new Oak(nodeStore)
                    .with(new OpenSecurityProvider())
                    .with(indexProvider)
                    .createContentRepository();

            ContentSession session = repo.login(null, null);
            try {
                Root root = session.getLatestRoot();
                QueryEngine qe = root.getQueryEngine();
                Result result = qe.executeQuery(
                        "explain select [jcr:path] from [nt:base] where [foo] = 'bar'",
                        "JCR-SQL2", QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
                ResultRow row = result.getRows().iterator().next();
                return row.getValue("plan").getValue(Type.STRING);
            } finally {
                session.close();
            }
        } finally {
            MockOsgi.deactivate(service, context.bundleContext());
        }
    }

    @Test
    public void queryUsesIndexImmediatelyAfterActivation() throws Exception {
        String plan = explainQueryImmediatelyAfterActivation(true);
        assertThat("A query right after activate() should use the already-built lucene "
                        + "index, not fall back to a full scan - actual plan: " + plan,
                plan, containsString("lucene:lucene"));
    }

    @Test
    public void queryTraversesImmediatelyAfterActivationWhenToggleDisabled() throws Exception {
        String plan = explainQueryImmediatelyAfterActivation(false);
        assertThat("With the fix turned off, a query right after activate() should fall "
                        + "back to a full scan even though the index is already built - "
                        + "this is the bug the toggle fixes - actual plan: " + plan,
                plan, not(containsString("lucene:lucene")));
    }
}
