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

package org.apache.jackrabbit.oak.plugins.index.importer;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.inventory.IndexDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.importer.AsyncIndexerLock.NOOP_LOCK;
import static org.apache.jackrabbit.oak.plugins.index.importer.IndexDefinitionUpdater.INDEX_DEFINITIONS_JSON;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class IndexImporterTest {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeStore store = new MemoryNodeStore();
    private IndexEditorProvider provider = new PropertyIndexEditorProvider();

    @Test(expected = IllegalArgumentException.class)
    public void importIndex_NoMeta() throws Exception{
        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
    }

    @Test(expected = NullPointerException.class)
    public void lostCheckpoint() throws Exception{
        IndexerInfo info = new IndexerInfo(temporaryFolder.getRoot(), "unknown-checkpoint");
        info.save();
        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
    }

    @Test
    public void switchLanes() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        builder.child("idx-a").setProperty("type", "property");

        builder.child("idx-b").setProperty("type", "property");
        builder.child("idx-b").setProperty("async", "async");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        createIndexDirs("/idx-a", "/idx-b");

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
        importer.switchLanes();

        NodeState idxa = NodeStateUtils.getNode(store.getRoot(), "/idx-a");
        assertEquals(AsyncLaneSwitcher.getTempLaneName(IndexImporter.ASYNC_LANE_SYNC), idxa.getString(ASYNC_PROPERTY_NAME));

        NodeState idxb = NodeStateUtils.getNode(store.getRoot(), "/idx-b");
        assertEquals(AsyncLaneSwitcher.getTempLaneName("async"), idxb.getString(ASYNC_PROPERTY_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void importData_NoProvider() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        builder.child("idx-a").setProperty("type", "property");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        createIndexDirs("/idx-a");

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
        importer.switchLanes();
        importer.importIndexData();
    }

    @Test
    public void importData_CallbackInvoked() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        builder.child("idx-a").setProperty("type", "property");
        builder.child("idx-a").setProperty("foo", "bar");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        createIndexDirs("/idx-a");

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);

        IndexImporterProvider provider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) {
                assertEquals("bar", defn.getString("foo"));
                assertEquals("idx-a", indexDir.getName());
                defn.setProperty("imported", true);
            }

            @Override
            public String getType() {
                return "property";
            }
        };
        importer.addImporterProvider(provider);
        importer.switchLanes();
        importer.importIndexData();

        assertTrue(store.getRoot().getChildNode("idx-a").getBoolean("imported"));
    }

    @Test
    public void importData_UpdatedIndexDefinition() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        builder.child("idx-a").setProperty("type", "property");
        builder.child("idx-a").setProperty("foo", "bar");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        createIndexDirs("/idx-a");

        //We remove foo property here
        builder = store.getRoot().builder();
        builder.child("idx-a").removeProperty("foo");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);

        IndexImporterProvider provider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) {
                //Foo property should be set by virtue of import from json
                assertEquals("bar", defn.getString("foo"));
            }

            @Override
            public String getType() {
                return "property";
            }
        };
        importer.addImporterProvider(provider);
        importer.switchLanes();
        importer.importIndexData();
    }

    @Test
    public void importData_IncrementalUpdate() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        String checkpoint = createIndexDirs("/oak:index/fooIndex");

        builder = store.getRoot().builder();
        builder.child("c").setProperty("foo", "abc");
        builder.child("d").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        FilterImpl f = createFilter(store.getRoot(), NT_BASE);
        PropertyIndexLookup lookup = new PropertyIndexLookup(store.getRoot());
        assertEquals(of("a", "b", "c", "d"), find(lookup, "foo", "abc", f));

        IndexImporterProvider importerProvider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) {
                assertEquals("fooIndex", indexDir.getName());
                assertEquals(2, defn.getProperty(REINDEX_COUNT).getValue(Type.LONG).longValue());
                defn.getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME).remove();

                NodeState cpState = store.retrieve(checkpoint);
                NodeState indexData = NodeStateUtils.getNode(cpState, "/oak:index/fooIndex/:index");
                defn.setChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME, indexData);
            }

            @Override
            public String getType() {
                return "property";
            }
        };

        builder = store.getRoot().builder();
        builder.child("e").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
        importer.addImporterProvider(importerProvider);
        importer.addImporterProvider(importerProvider);

        importer.importIndex();

        NodeState idx = store.getRoot().getChildNode("oak:index").getChildNode("fooIndex");
        assertEquals("async", idx.getString("async"));

        lookup = new PropertyIndexLookup(store.getRoot());
        //It would not pickup /e as thats not yet indexed as part of last checkpoint
        assertEquals(of("a", "b", "c", "d"), find(lookup, "foo", "abc", f));
        assertNull(store.retrieve(checkpoint));
    }

    @Test
    public void importIndex_newIndex() throws Exception{
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

        //Create checkpoint file
        String checkpoint = store.checkpoint(1000000);
        IndexerInfo info = new IndexerInfo(indexFolder, checkpoint);
        info.save();

        //Create index definitions json
        Files.write(json, new File(indexFolder, INDEX_DEFINITIONS_JSON), UTF_8);

        createIndexFolder(indexFolder, "/oak:index/fooIndex");

        //Prepare importer
        IndexImporterProvider importerProvider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir)
                    throws CommitFailedException {
                NodeState fooIndex = getFooIndexNodeState();
                defn.setChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME,
                        fooIndex.getChildNode(":index"));
            }

            @Override
            public String getType() {
                return "property";
            }
        };

        IndexImporter importer = new IndexImporter(store, indexFolder, provider, NOOP_LOCK);
        importer.addImporterProvider(importerProvider);

        //Add some more indexable data
        builder = store.getRoot().builder();
        builder.child("c").setProperty("foo", "abc");
        builder.child("d").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        //Now perform import
        importer.importIndex();

        FilterImpl f = createFilter(store.getRoot(), NT_BASE);
        PropertyIndexLookup lookup = new PropertyIndexLookup(store.getRoot());
        assertEquals(of("a", "c", "d"), find(lookup, "foo", "abc", f));

        NodeState idx = store.getRoot().getChildNode("oak:index").getChildNode("fooIndex");
        assertEquals(2, idx.getLong("reindexCount"));
    }

    @Test
    public void importData_DisabledIndexes() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder idxa = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null);
        idxa.setProperty(ASYNC_PROPERTY_NAME, "async");
        idxa.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS, asList("/oak:index/barIndex"), Type.STRINGS);

        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        String checkpoint = createIndexDirs("/oak:index/fooIndex");

        builder = store.getRoot().builder();

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "barIndex", true, false, ImmutableSet.of("foo"), null);

        builder.child("c").setProperty("foo", "abc");
        builder.child("d").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        IndexImporterProvider importerProvider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) {
            }

            @Override
            public String getType() {
                return "property";
            }
        };

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, NOOP_LOCK);
        importer.addImporterProvider(importerProvider);

        importer.importIndex();

        NodeState idx = store.getRoot().getChildNode("oak:index").getChildNode("barIndex");
        assertEquals(TYPE_DISABLED, idx.getString(TYPE_PROPERTY_NAME));
    }

    private NodeState getFooIndexNodeState() throws CommitFailedException {
        NodeState root = INITIAL_CONTENT;
        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "fooIndex",
                true, false, ImmutableSet.of("foo"), null);
        builder.child("a").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();
        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new PropertyIndexEditorProvider()));
        NodeState indexed = hook.processCommit(EMPTY_NODE, after, CommitInfo.EMPTY);
        return indexed.getChildNode("oak:index").getChildNode("fooIndex");
    }

    @Test
    public void laneName() throws Exception{
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, "async");
        assertEquals("async", IndexImporter.getAsyncLaneName("foo", builder.getNodeState()));

        builder = EMPTY_NODE.builder();
        builder.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, asList("async", "nrt"), Type.STRINGS);
        assertEquals("async", IndexImporter.getAsyncLaneName("foo", builder.getNodeState()));

        builder = EMPTY_NODE.builder();
        builder.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, asList("async", "nrt"), Type.STRINGS);
        AsyncLaneSwitcher.switchLane(builder, "tmp-async");
        assertEquals("async", IndexImporter.getAsyncLaneName("foo", builder.getNodeState()));

        builder = EMPTY_NODE.builder();
        builder.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, asList("async", "nrt"), Type.STRINGS);
        AsyncLaneSwitcher.switchLane(builder, "tmp-async");
        assertEquals("async", IndexImporter.getAsyncLaneName("foo", builder.getNodeState()));
    }

    @Test
    public void laneUnlockedInCaseOfFailure() throws Exception{
        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        String checkpoint = createIndexDirs("/oak:index/fooIndex");

        builder = store.getRoot().builder();
        builder.child("c").setProperty("foo", "abc");
        builder.child("d").setProperty("foo", "abc");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        new AsyncIndexUpdate("async", store, provider).run();

        IndexImporterProvider importerProvider = new IndexImporterProvider() {
            @Override
            public void importIndex(NodeState root, NodeBuilder defn, File indexDir) {

            }

            @Override
            public String getType() {
                return "property";
            }
        };

        final String exceptionMessage = "TEST MESSAGE";
        ClusterNodeStoreLock lock = new ClusterNodeStoreLock(store);
        provider = new PropertyIndexEditorProvider() {
            @Override
            public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder definition,
                                         @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback) {
                throw new RuntimeException(exceptionMessage);
            }
        };

        IndexImporter importer = new IndexImporter(store, temporaryFolder.getRoot(), provider, lock);
        importer.addImporterProvider(importerProvider);

        try {
            importer.importIndex();
            fail();
        } catch (RuntimeException ignore) {

        }

        assertFalse(lock.isLocked("async"));

        AsyncIndexerLock lock2 = new AsyncIndexerLock() {
            @Override
            public LockToken lock(String asyncIndexerLane) throws CommitFailedException {
                return mock(LockToken.class);
            }

            @Override
            public void unlock(LockToken token) throws CommitFailedException {
                throw new IllegalStateException("Exception in unlock");
            }
        };
        IndexImporter importer2 = new IndexImporter(store, temporaryFolder.getRoot(), provider, lock2);
        importer2.addImporterProvider(importerProvider);

        try {
            importer2.importIndex();
            fail();
        } catch (RuntimeException ignore) {
            assertEquals(exceptionMessage, ignore.getMessage());
        }
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]",
                new QueryEngineSettings());
    }

    private static Set<String> find(PropertyIndexLookup lookup, String name,
                                    String value, Filter filter) {
        return Sets.newHashSet(lookup.query(filter, name, value == null ? null
                : PropertyValues.newString(value)));
    }

    private String createIndexDirs(String... indexPaths) throws IOException, CommitFailedException {
        String checkpoint = store.checkpoint(1000000);
        IndexerInfo info = new IndexerInfo(temporaryFolder.getRoot(), checkpoint);
        info.save();

        for (String indexPath : indexPaths){
            createIndexFolder(temporaryFolder.getRoot(), indexPath);
        }
        dumpIndexDefinitions(indexPaths);
        return checkpoint;
    }

    private void createIndexFolder(File indexFolder, String indexPath) throws IOException {
        String dirName = PathUtils.getName(indexPath);
        File indexDir = new File(indexFolder, dirName);
        File indexMeta = new File(indexDir, IndexerInfo.INDEX_METADATA_FILE_NAME);
        Properties p = new Properties();
        p.setProperty(IndexerInfo.PROP_INDEX_PATH, indexPath);
        indexDir.mkdir();
        PropUtils.writeTo(p, indexMeta, "index info");
    }

    private void dumpIndexDefinitions(String... indexPaths) throws IOException, CommitFailedException {
        IndexDefinitionPrinter printer = new IndexDefinitionPrinter(store, () -> asList(indexPaths));
        printer.setFilter("{\"properties\":[\"*\", \"-:childOrder\"],\"nodes\":[\"*\", \"-:index-definition\"]}");
        File file = new File(temporaryFolder.getRoot(), INDEX_DEFINITIONS_JSON);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printer.print(pw, Format.JSON, false);
        Files.write(sw.toString(), file, UTF_8);
    }
}