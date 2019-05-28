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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Tests marking index as corrupt if blob is missing.
 * {@link org.apache.jackrabbit.oak.segment.SegmentNodeStore}.
 */
@RunWith(Parameterized.class)
public class AsyncIndexUpdateCorruptMarkingTest {

    private static final File DIRECTORY = new File("target/fs");
    private static String FOO = "foo";
    private static final String FOO_QUERY = "select [jcr:path] from [nt:base] where contains('foo', '*')";
    private final long INDEX_CORRUPT_INTERVAL_IN_SECONDS = 2;
    private long INDEX_ERROR_WARN_INTERVAL_IN_SECONDS = 1;

    private final boolean copyOnRW;
    private final String codec;
    private final boolean indexOnFS;
    private final int minRecordLength;
    private final String mergePolicy;
    protected ContentSession session;
    protected Root root;

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    private ExecutorService executorService = Executors.newFixedThreadPool(2);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private String corDir = null;
    private String cowDir = null;

    private TestUtil.OptionalEditorProvider optionalEditorProvider = new TestUtil.OptionalEditorProvider();
    private FileStore fileStore;
    private DataStoreBlobStore dataStoreBlobStore;
    private DefaultStatisticsProvider statisticsProvider;
    private String fdsDir;
    private String indexPath;
    private AsyncIndexUpdate asyncIndexUpdate;


    public AsyncIndexUpdateCorruptMarkingTest(boolean copyOnRW, String codec, boolean indexOnFS, int minRecordLength, String mergePolicy) {
        this.copyOnRW = copyOnRW;
        this.codec = codec;
        this.indexOnFS = indexOnFS;
        this.minRecordLength = minRecordLength;
        this.mergePolicy = mergePolicy;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false, "oakCodec", false, 4096, "tiered"},
        });
    }

    @Before
    public void setUp() throws Exception {
        if (!DIRECTORY.exists()) {
            assert DIRECTORY.mkdirs();
        }
    }

    @After
    public void after() {
        new ExecutorCloser(executorService).close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
        fileStore.close();
        if (DIRECTORY.exists()) {
            try {
                FileUtils.deleteDirectory(DIRECTORY);
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    protected ContentRepository createRepository() {
        LuceneIndexEditorProvider editorProvider;
        LuceneIndexProvider provider;
        if (copyOnRW) {
            IndexCopier copier = createIndexCopier();
            editorProvider = new LuceneIndexEditorProvider(copier, new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
            provider = new LuceneIndexProvider(copier);
        } else {
            editorProvider = new LuceneIndexEditorProvider();
            provider = new LuceneIndexProvider();
        }

        NodeStore nodeStore;
        try {
            statisticsProvider = new DefaultStatisticsProvider(scheduledExecutorService);
            fileStore = FileStoreBuilder.fileStoreBuilder(DIRECTORY)
                    .withStatisticsProvider(statisticsProvider)
                    .withBlobStore(createBlobStore())
                    .build();
            nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }

        asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, editorProvider);
        TrackingCorruptIndexHandler trackingCorruptIndexHandler = new TrackingCorruptIndexHandler();
        trackingCorruptIndexHandler.setCorruptInterval(INDEX_CORRUPT_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
        trackingCorruptIndexHandler.setErrorWarnInterval(INDEX_ERROR_WARN_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
        return new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) provider)
                .with((Observer) provider)
                .with(editorProvider)
                .with(optionalEditorProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider())
                .createContentRepository();
    }

    private BlobStore createBlobStore() {
        FileDataStore fds = new OakFileDataStore();
        fdsDir = "target/fds-" + codec + copyOnRW + minRecordLength + mergePolicy;
        fds.setPath(fdsDir);
        if (minRecordLength > 0) {
            fds.setMinRecordLength(minRecordLength);
        }
        fds.init(null);
        dataStoreBlobStore = new DataStoreBlobStore(fds);
        StatisticsProvider sp = new DefaultStatisticsProvider(scheduledExecutorService);
        BlobStatsCollector collector = new BlobStoreStats(sp);
        dataStoreBlobStore.setBlobStatsCollector(collector);
        return dataStoreBlobStore;
    }

    private IndexCopier createIndexCopier() {
        try {
            return new IndexCopier(executorService, temporaryFolder.getRoot()) {
                @Override
                public Directory wrapForRead(String indexPath, LuceneIndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(LuceneIndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName, COWDirectoryTracker cowDirectoryTracker) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName, cowDirectoryTracker);
                    cowDir = getFSDirPath(ret);
                    return ret;
                }

                private String getFSDirPath(Directory dir) {
                    if (dir instanceof CopyOnReadDirectory) {
                        dir = ((CopyOnReadDirectory) dir).getLocal();
                    }

                    dir = unwrap(dir);

                    if (dir instanceof FSDirectory) {
                        return ((FSDirectory) dir).getDirectory().getAbsolutePath();
                    }
                    return null;
                }

                private Directory unwrap(Directory dir) {
                    if (dir instanceof FilterDirectory) {
                        return unwrap(((FilterDirectory) dir).getDelegate());
                    }
                    return dir;
                }

            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void shutdownExecutor() {
        executorService.shutdown();
        scheduledExecutorService.shutdown();
    }

    private void deleteBlobs(String path) {
        File file = new File(path);
        while (file.listFiles().length > 0) {
            File folder = file.listFiles()[0];
            try {
                FileUtils.deleteDirectory(folder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testLuceneIndexSegmentStats() throws Exception {
        root.commit();
        root.getTree("/oak:index/counter").remove();
        root.commit();

        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder()
                //.noAsync()
                .codec(codec)
                .mergePolicy(mergePolicy);
        idxb.indexRule("nt:base").property(FOO).analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        idxb.indexRule("nt:base").property("bin").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("lucenePropertyIndex");
        Tree idxDef = idxb.build(idx);
        if (!codec.equals("oakCodec") && indexOnFS) {
            idxDef.setProperty("persistence", "file");
            indexPath = "target/index-" + codec + copyOnRW;
            idxDef.setProperty("path", indexPath);
        }
        System.out.println("***");
        System.out.println(codec + "," + copyOnRW + "," + indexOnFS + "," + minRecordLength + "," + mergePolicy);
        root.getTree("/").addChild("content");
        ContentCreator contentCreator = new ContentCreator();
        contentCreator.run();
        root.commit();
        asyncIndexUpdate.run();
        ScheduledExecutorService executorService = Executors
                .newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(contentCreator, 0, 10, TimeUnit.MILLISECONDS);

        Thread.sleep(200);
        contentCreator.setStopContentCreator();
        Thread.sleep(50);
        deleteBlobs(fdsDir);
        asyncIndexUpdate.run(); // As blobs are deleted at this point index will be marked as bad.
        Thread.sleep(100);
        Thread.sleep(INDEX_CORRUPT_INTERVAL_IN_SECONDS * 1000);
        asyncIndexUpdate.run(); // after corrupt interval index will be marked as corrupt.
        Thread.sleep(100);
        assertTrue(null != root.getTree("/oak:index/lucenePropertyIndex").getProperty("corrupt"));

    }

    private class ContentCreator implements Runnable {
        private static final String STRINGSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        private volatile boolean stopContentCreator = false;

        private long numberOfNodes = 100;
        private int randomStringLength = 100;
        private int randomNodeNameLength = 8;

        private String randomString(int count) {
            StringBuilder builder = new StringBuilder();
            while (count-- != 0) {
                int character = (int) (Math.random() * STRINGSET.length());
                builder.append(STRINGSET.charAt(character));
            }
            return builder.toString();
        }

        public void setStopContentCreator() {
            stopContentCreator = true;
        }

        public void run() {
            if (!stopContentCreator) {
                Tree rootTree = root.getTree("/content");
                for (int i = 0; i < numberOfNodes; i++) {
                    String text = randomString(randomStringLength);
                    Tree tree = rootTree.addChild(String.valueOf(randomString(randomNodeNameLength).trim() + i));
                    tree.setProperty(FOO, text);
                }
            } else {
                try {
                    root.commit();
                    fileStore.flush();
                } catch (IOException | CommitFailedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
