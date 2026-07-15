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
package org.apache.jackrabbit.oak.composite;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jcr.Repository;
import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexTracker;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.DefaultIndexReaderFactory;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.FileBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for testing indexing and queries when using the composite node
 * store.
 */
@RunWith(Parameterized.class)
public class CompositeNodeStoreQueryTestBase {

    protected final NodeStoreKind nodeStoreRoot;
    protected final NodeStoreKind mounts;

    private final List<NodeStoreRegistration> registrations = new ArrayList<>();

    private NodeStore mountedStore;
    private NodeStore deepMountedStore;

    protected NodeStore readOnlyStore;

    // the composite store (containing read-write and read-only stores)
    protected CompositeNodeStore store;

    // the global store (read-write)
    protected NodeStore globalStore;

    protected NodeStore emptyStore;

    protected MountInfoProvider mip;

    protected QueryEngine qe;
    protected ContentSession session;
    protected Root root;

    protected ExecutorService executorService = Executors.newFixedThreadPool(2);

    protected IndexTracker indexTracker;
    protected IndexCopier indexCopier;
    protected Oak oak;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Parameters(name = "Root: {0}, Mounts: {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {NodeStoreKind.MEMORY, NodeStoreKind.MEMORY},
                {NodeStoreKind.SEGMENT, NodeStoreKind.SEGMENT},
                {NodeStoreKind.DOCUMENT_H2, NodeStoreKind.DOCUMENT_H2},
                {NodeStoreKind.DOCUMENT_H2, NodeStoreKind.SEGMENT},
                {NodeStoreKind.DOCUMENT_MEMORY, NodeStoreKind.DOCUMENT_MEMORY}
        });
    }

    public CompositeNodeStoreQueryTestBase(NodeStoreKind root, NodeStoreKind mounts) {
        this.nodeStoreRoot = root;
        this.mounts = mounts;
    }

    @Before
    public void initStore() throws Exception {
        globalStore = register(nodeStoreRoot.create(null));
        mountedStore = register(mounts.create("temp"));
        deepMountedStore = register(mounts.create("deep"));
        readOnlyStore = register(mounts.create("readOnly"));
        emptyStore = register(mounts.create("empty")); // this NodeStore will always be empty

        // create a property on the root node
        NodeBuilder builder = globalStore.getRoot().builder();
        builder.setProperty("prop", "val");
        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(globalStore.getRoot().hasProperty("prop"));

        // create a different sub-tree on the root store
        builder = globalStore.getRoot().builder();
        NodeBuilder libsBuilder = builder.child("libs");
        libsBuilder.child("first");
        libsBuilder.child("second");

        // create an empty /apps node with a property
        builder.child("apps").setProperty("prop", "val");

        globalStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertThat(globalStore.getRoot().getChildNodeCount(10), equalTo(2l));

        // create a /tmp child on the mounted store and set a property
        builder = mountedStore.getRoot().builder();
        NodeBuilder tmpBuilder = builder.child("tmp");
        tmpBuilder.setProperty("prop1", "val1");
        tmpBuilder.child("child1").setProperty("prop1", "val1");
        tmpBuilder.child("child2");

        mountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(mountedStore.getRoot().hasChildNode("tmp"));
        assertThat(mountedStore.getRoot().getChildNode("tmp").getChildNodeCount(10), equalTo(2l));

        // populate /libs/mount/third in the deep mount, and include a property

        builder = deepMountedStore.getRoot().builder();
        builder.child("libs").child("mount").child("third").setProperty("mounted", "true");

        deepMountedStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertTrue(deepMountedStore.getRoot().getChildNode("libs").getChildNode("mount").getChildNode("third").hasProperty("mounted"));

        // populate /readonly with a single node
        builder = readOnlyStore.getRoot().builder();
        new InitialContent().initialize(builder);
        builder.child("readOnly");

        readOnlyStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        initMounts();
    }

    void initMounts() throws Exception {
        mip = Mounts.newBuilder()
                .readOnlyMount("temp", "/tmp")
                .readOnlyMount("deep", "/libs/mount")
                .readOnlyMount("empty", "/nowhere")
                .readOnlyMount("readOnly", "/readOnly")
                .build();

        // don't use the builder since it would fail due to too many read-write stores
        // but for the purposes of testing the general correctness it's fine
        List<MountedNodeStore> nonDefaultStores = new ArrayList<>();
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("temp"), mountedStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("deep"), deepMountedStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("empty"), emptyStore));
        nonDefaultStores.add(new MountedNodeStore(mip.getMountByName("readOnly"), readOnlyStore));
        store = new CompositeNodeStore(mip, globalStore, nonDefaultStores);

        session = createRepository(store).login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }

    protected ContentRepository createRepository(NodeStore store) {
        return getOakRepo(store).createContentRepository();
    }

    Oak getOakRepo(NodeStore store) {
        if (oak != null) {
            return oak;
        }
        try {
            indexCopier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException();
        }
        DefaultIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, indexCopier);
        indexTracker = new IndexTracker(indexReaderFactory);

        LuceneIndexProvider luceneIndexProvider =
                new LuceneIndexProvider(indexTracker);
        LuceneIndexEditorProvider luceneIndexEditor =
                new LuceneIndexEditorProvider(indexCopier, indexTracker, null, null, mip);
        oak = new Oak(store).with(new InitialContent())
            .with(new OpenSecurityProvider())
            .with(new PropertyIndexEditorProvider().with(mip))
            .with(new NodeCounterEditorProvider().with(mip))
            .with(new PropertyIndexProvider().with(mip))
            .with(luceneIndexEditor)
            .with((QueryIndexProvider) luceneIndexProvider)
            .with((Observer) luceneIndexProvider)
            .with(new NodeTypeIndexProvider().with(mip))
            .with(new ReferenceEditorProvider().with(mip))
            .with(new ReferenceIndexProvider().with(mip));
        return oak;
    }

    protected ContentRepository createRepository(NodeStore store, MountInfoProvider mip) {
        return getOakRepo(store, mip).createContentRepository();
    }

    protected Repository createJCRRepository(NodeStore store, MountInfoProvider mip) {
        return new Jcr(getOakRepo(store, mip)).createRepository();
    }

    Oak getOakRepo(NodeStore store, MountInfoProvider mip) {

        try {
            indexCopier = new IndexCopier(executorService, temporaryFolder.getRoot());
        } catch (IOException e) {
            throw new RuntimeException();
        }
        DefaultIndexReaderFactory indexReaderFactory = new DefaultIndexReaderFactory(mip, indexCopier);
        indexTracker = new IndexTracker(indexReaderFactory);

        LuceneIndexProvider luceneIndexProvider =
                new LuceneIndexProvider(indexTracker);
        LuceneIndexEditorProvider luceneIndexEditor;

        if (mip == null ){
            luceneIndexEditor = new LuceneIndexEditorProvider();
            return new Oak(store).with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .with(luceneIndexEditor)
                    .with((QueryIndexProvider) luceneIndexProvider)
                    .with((Observer) luceneIndexProvider);
        } else {
            luceneIndexEditor = new LuceneIndexEditorProvider(indexCopier, indexTracker, null, null, mip);
            return new Oak(store).with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .with(new PropertyIndexEditorProvider().with(mip))
                    .with(new NodeCounterEditorProvider().with(mip))
                    .with(new PropertyIndexProvider().with(mip))
                    .with(luceneIndexEditor)
                    .with((QueryIndexProvider) luceneIndexProvider)
                    .with((Observer) luceneIndexProvider)
                    .with(new NodeTypeIndexProvider().with(mip))
                    .with(new ReferenceEditorProvider().with(mip))
                    .with(new ReferenceIndexProvider().with(mip));
        }

    }

    protected List<String> executeQuery(String query, String language) {
        boolean pathsOnly = false;
        if (language.equals("xpath")) {
            pathsOnly = true;
        }
        return executeQuery(query, language, pathsOnly);
    }

    protected List<String> executeQuery(String query, String language, boolean pathsOnly) {
        return executeQuery(query, language, pathsOnly, false);
    }

    protected List<String> executeQuery(String query, String language, boolean pathsOnly, boolean skipSort) {
        long time = System.currentTimeMillis();
        List<String> lines = new ArrayList<String>();
        try {
            Result result = executeQuery(query, language, NO_BINDINGS);
            for (ResultRow row : result.getRows()) {
                String r = readRow(row, pathsOnly);
                if (query.startsWith("explain ")) {
                    r = formatPlan(r);
                }
                lines.add(r);
            }
            if (!query.contains("order by") && !skipSort) {
                Collections.sort(lines);
            }
        } catch (ParseException e) {
            lines.add(e.toString());
        } catch (IllegalArgumentException e) {
            lines.add(e.toString());
        }
        time = System.currentTimeMillis() - time;
        if (time > 5 * 60 * 1000 && !isDebugModeEnabled()) {
            // more than 5 minutes
            fail("Query took too long: " + query + " took " + time + " ms");
        }
        return lines;
    }

    /**
     * Check whether the test is running in debug mode.
     *
     * @return true if debug most is (most likely) enabled
     */
    protected static boolean isDebugModeEnabled() {
        return java.lang.management.ManagementFactory.getRuntimeMXBean()
                .getInputArguments().toString().indexOf("-agentlib:jdwp") > 0;
    }

    static String formatPlan(String plan) {
        plan = plan.replaceAll(" where ", "\n  where ");
        plan = plan.replaceAll(" inner join ", "\n  inner join ");
        plan = plan.replaceAll(" on ", "\n  on ");
        plan = plan.replaceAll(" and ", "\n  and ");
        return plan;
    }

    protected static String readRow(ResultRow row, boolean pathOnly) {
        if (pathOnly) {
            return row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING);
        }
        StringBuilder buff = new StringBuilder();
        PropertyValue[] values = row.getValues();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                buff.append(", ");
            }
            PropertyValue v = values[i];
            if (v == null) {
                buff.append("null");
            } else if (v.isArray()) {
                buff.append('[');
                for (int j = 0; j < v.count(); j++) {
                    buff.append(v.getValue(Type.STRING, j));
                    if (j > 0) {
                        buff.append(", ");
                    }
                }
                buff.append(']');
            } else {
                buff.append(v.getValue(Type.STRING));
            }
        }
        return buff.toString();
    }

    protected Result executeQuery(String statement, String language,
            Map<String, PropertyValue> sv) throws ParseException {
        return qe.executeQuery(statement, language, sv, NO_MAPPINGS);
    }

    @After
    public final void baseTearDown() throws Exception {
        for ( NodeStoreRegistration reg : registrations ) {
            reg.close();
        }
    }

    enum NodeStoreKind {
        MEMORY {
            @Override
            public NodeStoreRegistration create(String name) {
                return new NodeStoreRegistration() {

                    private MemoryNodeStore instance;

                    @Override
                    public NodeStore get(TemporaryFolder temporaryFolder) {

                        if (instance != null) {
                            throw new IllegalStateException("instance already created");
                        }

                        instance = new MemoryNodeStore();

                        return instance;
                    }

                    @Override
                    public void close() throws Exception {
                        // does nothing

                    }
                };
            }

            public boolean supportsBlobCreation() {
                return false;
            }
        }, SEGMENT {
            @Override
            public NodeStoreRegistration create(final String name) {
                return new NodeStoreRegistration() {

                    private SegmentNodeStore instance;
                    private FileStore store;
                    private File storePath;
                    private String blobStorePath;

                    @Override
                    public NodeStore get(TemporaryFolder temporaryFolder) throws Exception {

                        if (instance != null) {
                            throw new IllegalStateException("instance already created");
                        }

                        String directoryName = name != null ? "segment-" + name : "segment";
                        storePath = temporaryFolder.newFolder(directoryName);

                        //String blobStoreDirectoryName = name != null ? "blob-" + name : "blob";
                        String blobStoreDirectoryName = "blob" ;
                        blobStorePath = temporaryFolder.getRoot().getAbsolutePath() + blobStoreDirectoryName;

                        BlobStore blobStore = new FileBlobStore(blobStorePath);

                        store = FileStoreBuilder.fileStoreBuilder(storePath).withBlobStore(blobStore).build();
                        instance = SegmentNodeStoreBuilders.builder(store).build();

                        return instance;
                    }

                    @Override
                    public void close() throws Exception {
                        store.close();

                        FileUtils.deleteQuietly(storePath);
                        FileUtils.deleteQuietly(new File(blobStorePath));
                    }
                };
            }
        }, DOCUMENT_H2 {

            @Override
            public NodeStoreRegistration create(final String name) {

                return new NodeStoreRegistration() {

                    private DocumentNodeStore instance;
                    private String dbPath;

                    // TODO - copied from DocumentRdbFixture

                    private DataSource ds;

                    @Override
                    public NodeStore get(TemporaryFolder temporaryFolder) throws Exception {
                        RDBOptions options = new RDBOptions().dropTablesOnClose(true);
                        dbPath = temporaryFolder.getRoot().getAbsolutePath() + "/document";
                        if ( name != null ) {
                            dbPath += "-" + name;
                        }
                        ds = RDBDataSourceFactory.forJdbcUrl("jdbc:h2:file:" + dbPath, "sa", "");

                        instance = new RDBDocumentNodeStoreBuilder()
                                .setRDBConnection(ds, options).build();
                        instance.setMaxBackOffMillis(0);

                        return instance;

                    }

                    @Override
                    public void close() throws Exception {
                        instance.dispose();
                        if ( ds instanceof Closeable ) {
                            ((Closeable) ds).close();
                        }
                        FileUtils.deleteQuietly(new File(dbPath));
                    }

                };

            }
        }, DOCUMENT_MEMORY {
            @Override
            public NodeStoreRegistration create(final String name) {

                return new NodeStoreRegistration() {

                    private DocumentNodeStore instance;

                    @Override
                    public NodeStore get(TemporaryFolder temporaryFolder) throws Exception {
                        DocumentNodeStoreBuilder<?> documentNodeStoreBuilder = DocumentNodeStoreBuilder.newDocumentNodeStoreBuilder();

                        instance = documentNodeStoreBuilder.build();

                        return instance;
                    }

                    @Override
                    public void close() {
                        instance.dispose();
                    }
                };

            }
        };

        public abstract NodeStoreRegistration create(@Nullable String name);

        public boolean supportsBlobCreation() {
            return true;
        }
    }

    private interface NodeStoreRegistration {
        NodeStore get(TemporaryFolder temporaryFolder) throws Exception;

        void close() throws Exception;
    }

    protected NodeStore register(NodeStoreRegistration reg) throws Exception {
        registrations.add(reg);

        return reg.get(temporaryFolder);
    }


}
