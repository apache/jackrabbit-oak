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

package org.apache.jackrabbit.oak.plugins.index;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexerInfo;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Exercises the OAK-12307 feature-toggle wiring in {@link IndexerMBeanImpl}: the toggle is created
 * on activate, consulted on {@code importIndex}, and closed on deactivate. Both toggle states are
 * covered; the import outcome itself is asserted through the MBean. The toggle-specific branching
 * inside {@code IndexImporter} is covered separately in {@code IndexImporterTest}.
 */
public class IndexerMBeanImplTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private final MemoryNodeStore store = new MemoryNodeStore();
    private final PropertyIndexEditorProvider provider = new PropertyIndexEditorProvider();

    @Test
    public void importUsesNewFlowByDefault() throws Exception {
        String indexDir = prepareImportDir();
        IndexerMBeanImpl mbean = createAndActivateMBean();

        assertTrue(mbean.importIndex(indexDir, true));
        assertImported();

        MockOsgi.deactivate(mbean, context.bundleContext());
    }

    @Test
    public void importUsesLegacyFlowWhenToggleEnabled() throws Exception {
        String indexDir = prepareImportDir();
        IndexerMBeanImpl mbean = createAndActivateMBean();

        FeatureToggle toggle = context.getService(FeatureToggle.class);
        assertNotNull(toggle);
        assertEquals(IndexerMBeanImpl.LEGACY_INDEX_IMPORT_TOGGLE, toggle.getName());
        toggle.setEnabled(true);

        assertTrue(mbean.importIndex(indexDir, true));
        assertImported();

        MockOsgi.deactivate(mbean, context.bundleContext());
    }

    @Test
    public void deactivateWithoutActivateIsNullSafe() {
        // Never activated: mbeanReg, providerTracker and the feature toggle are all null;
        // deactivate must tolerate that without failing.
        MockOsgi.deactivate(new IndexerMBeanImpl(), context.bundleContext());
    }

    private IndexerMBeanImpl createAndActivateMBean() {
        context.registerService(NodeStore.class, store);
        context.registerService(AsyncIndexInfoService.class, mock(AsyncIndexInfoService.class));
        context.registerService(IndexEditorProvider.class, provider);
        context.registerService(IndexImporterProvider.class, importerProvider());

        IndexerMBeanImpl mbean = new IndexerMBeanImpl();
        MockOsgi.injectServices(mbean, context.bundleContext());
        MockOsgi.activate(mbean, context.bundleContext(), Map.of());
        return mbean;
    }

    private void assertImported() {
        NodeState fooIndex = store.getRoot().getChildNode("oak:index").getChildNode("fooIndex");
        assertEquals(2, fooIndex.getLong("reindexCount"));
        assertNotNull(fooIndex.getProperty(ASYNC_PROPERTY_NAME));
    }

    private String prepareImportDir() throws Exception {
        NodeBuilder builder = store.getRoot().builder();
        builder.child("oak:index");
        builder.child("a").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        String json = "{\"/oak:index/fooIndex\": {\n" +
                "    \"reindexCount\": 1,\n" +
                "    \"reindex\": false,\n" +
                "    \"type\": \"property\",\n" +
                "    \"async\" : \"async\",\n" +
                "    \"propertyNames\": [\"foo\"],\n" +
                "    \"jcr:primaryType\": \"oak:QueryIndexDefinition\"\n" +
                "  }\n" +
                "}";

        File indexFolder = temporaryFolder.getRoot();
        String checkpoint = store.checkpoint(1000000);
        new IndexerInfo(indexFolder, checkpoint).save();
        Files.writeString(indexFolder.toPath().resolve(INDEX_DEFINITIONS_JSON), json);
        writeIndexMetadata(indexFolder, "fooIndex", "/oak:index/fooIndex");

        builder = store.getRoot().builder();
        builder.child("c").setProperty("foo", "abc");
        builder.child("d").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        return indexFolder.getAbsolutePath();
    }

    private static void writeIndexMetadata(File indexFolder, String dirName, String indexPath) throws Exception {
        File indexDir = new File(indexFolder, dirName);
        indexDir.mkdir();
        Properties p = new Properties();
        p.setProperty(IndexerInfo.PROP_INDEX_PATH, indexPath);
        try (OutputStream os = Files.newOutputStream(new File(indexDir, IndexerInfo.INDEX_METADATA_FILE_NAME).toPath())) {
            p.store(os, "index info");
        }
    }

    private IndexImporterProvider importerProvider() {
        return new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) throws CommitFailedException {
                defn.setChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME,
                        fooIndexNodeState().getChildNode(":index"));
            }

            @Override
            public String getType() {
                return "property";
            }
        };
    }

    private static NodeState fooIndexNodeState() throws CommitFailedException {
        NodeBuilder builder = INITIAL_CONTENT.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex", true, false, Set.of("foo"), null);
        builder.child("a").setProperty("foo", "abc");
        EditorHook hook = new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider()));
        NodeState indexed = hook.processCommit(EMPTY_NODE, builder.getNodeState(), CommitInfo.EMPTY);
        return indexed.getChildNode("oak:index").getChildNode("fooIndex");
    }
}
