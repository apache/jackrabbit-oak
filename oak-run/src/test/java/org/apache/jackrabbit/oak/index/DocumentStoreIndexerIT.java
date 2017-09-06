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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MongoConnectionFactory;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledTypesRegistry;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.createChild;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class DocumentStoreIndexerIT extends AbstractIndexCommandTest {
    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
    }

    @Test
    public void indexMongoRepo() throws Exception{
        DocumentNodeStore dns = getNodeStore();
        fixture = new RepositoryFixture(temporaryFolder.getRoot(), dns);
        createTestData(false);
        String checkpoint = fixture.getNodeStore().checkpoint(TimeUnit.HOURS.toMillis(24));
        fixture.close();
        dns.dispose();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-paths=/oak:index/fooIndex",
                "--doc-traversal-mode",
                "--checkpoint="+checkpoint,
                "--reindex",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                MongoUtils.URL
        };

        command.execute(args);

        File indexes = new File(outDir, IndexerSupport.LOCAL_INDEX_ROOT_DIR);
        assertTrue(indexes.exists());

        IndexRootDirectory idxRoot = new IndexRootDirectory(indexes);
        List<LocalIndexDir> idxDirs = idxRoot.getAllLocalIndexes();

        assertEquals(1, idxDirs.size());
    }

    @Test
    public void bundling() throws Exception{
        DocumentMK.Builder docBuilder = builderProvider.newBuilder().setMongoDB(getConnection().getDB());
        DocumentNodeStore store = docBuilder.getNodeStore();

        Whiteboard wb = new DefaultWhiteboard();
        MongoDocumentStore ds = (MongoDocumentStore) docBuilder.getDocumentStore();
        wb.register(MongoDocumentStore.class, ds, emptyMap());
        wb.register(StatisticsProvider.class, StatisticsProvider.NOOP, emptyMap());

        configureIndex(store);
        configureBundling(store);

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder appNB = newNode("app:Asset");
        createChild(appNB,
                "jcr:content",
                "jcr:content/comments", //not bundled
                "jcr:content/metadata",
                "jcr:content/metadata/xmp", //not bundled
                "jcr:content/renditions", //includes all
                "jcr:content/renditions/original",
                "jcr:content/renditions/original/jcr:content"
        );
        builder.child("test").setChildNode("book.jpg", appNB.getNodeState());
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Check that bundling is working
        assertNull(getNodeDocument(ds, "/test/book.jpg/jcr:content"));
        assertNotNull(getNodeDocument(ds, "/test/book.jpg"));

        String checkpoint = store.checkpoint(100000);

        IndexHelper helper = new IndexHelper(store, store.getBlobStore(), wb, temporaryFolder.newFolder(),
                temporaryFolder.newFolder(), asList(TEST_INDEX_PATH));
        IndexerSupport support = new IndexerSupport(helper, checkpoint);

        CollectingIndexer testIndexer = new CollectingIndexer(p -> p.startsWith("/test"));
        DocumentStoreIndexer index = new DocumentStoreIndexer(helper, support) {
            @Override
            protected CompositeIndexer prepareIndexers(NodeStore nodeStore, NodeBuilder builder) {
                return new CompositeIndexer(asList(testIndexer));
            }
        };


        index.reindex();

        assertThat(testIndexer.paths, containsInAnyOrder(
                "/test",
                "/test/book.jpg",
                "/test/book.jpg/jcr:content",
                "/test/book.jpg/jcr:content/comments",
                "/test/book.jpg/jcr:content/metadata",
                "/test/book.jpg/jcr:content/metadata/xmp",
                "/test/book.jpg/jcr:content/renditions",
                "/test/book.jpg/jcr:content/renditions/original",
                "/test/book.jpg/jcr:content/renditions/original/jcr:content"
        ));

        store.dispose();

    }

    private void configureIndex(DocumentNodeStore store) throws CommitFailedException {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder idxb = childBuilder(builder, TEST_INDEX_PATH);

        IndexDefinitionBuilder defnb = new IndexDefinitionBuilder(idxb);
        defnb.indexRule("nt:base").property("foo").propertyIndex();
        defnb.build();

        merge(store, builder);
    }

    private DocumentNodeStore getNodeStore(){
        return builderProvider.newBuilder().setMongoDB(getConnection().getDB()).getNodeStore();
    }

    private MongoConnection getConnection(){
        MongoConnection conn = connectionFactory.getConnection();
        assumeNotNull(conn);
        MongoUtils.dropCollections(conn.getDB());
        return conn;
    }

    private static void configureBundling(DocumentNodeStore store) throws CommitFailedException {
        NodeState registryState = BundledTypesRegistry.builder()
                .forType("app:Asset")
                .include("jcr:content")
                .include("jcr:content/metadata")
                .include("jcr:content/renditions")
                .include("jcr:content/renditions/**")
                .build();
        NodeBuilder builder = store.getRoot().builder();
        new InitialContent().initialize(builder);
        builder.getChildNode("jcr:system")
                .getChildNode(DOCUMENT_NODE_STORE)
                .getChildNode(BUNDLOR)
                .setChildNode("app:Asset", registryState.getChildNode("app:Asset"));
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeDocument getNodeDocument(DocumentStore ds, String path) {
        return ds.find(Collection.NODES, Utils.getIdFromPath(path));
    }

    private static NodeBuilder newNode(String typeName){
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(JCR_PRIMARYTYPE, typeName);
        return builder;
    }

    private static class CollectingIndexer implements NodeStateIndexer {
        private final Predicate<String> p;
        List<String> paths = new ArrayList<>();

        private CollectingIndexer(Predicate<String> p) {
            this.p = p;
        }

        @Override
        public boolean shouldInclude(String path) {
            return true;
        }

        @Override
        public boolean shouldInclude(NodeDocument doc) {
            return true;
        }

        @Override
        public void index(NodeStateEntry entry) throws IOException, CommitFailedException {
            if (p.test(entry.getPath())) {
                paths.add(entry.getPath());
            }
        }

        @Override
        public void close() throws IOException {

        }
    }

}
