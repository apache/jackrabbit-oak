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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStoreStats;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checking impacts of Lucene writes wrt storage / configuration adjustments on the
 * {@link org.apache.jackrabbit.oak.segment.SegmentNodeStore}.
 */
@Ignore("this is meant to be a benchmark, it shouldn't be part of everyday builds")
@RunWith(Parameterized.class)
public class LuceneWritesOnSegmentStatsTest extends AbstractQueryTest {

    private static final File DIRECTORY = new File("target/fs");
    private static final String FOO_QUERY = "select [jcr:path] from [nt:base] where contains('foo', '*')";

    private final boolean copyOnRW;
    private final String codec;
    private final boolean indexOnFS;
    private final int minRecordLength;
    private final String mergePolicy;

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


    public LuceneWritesOnSegmentStatsTest(boolean copyOnRW, String codec, boolean indexOnFS, int minRecordLength, String mergePolicy) {
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
                {false, "oakCodec", false, 4096, "mitigated"},
                {false, "oakCodec", false, 4096, "no"},
                {false, "Lucene46", false, 4096, "tiered"},
                {false, "Lucene46", false, 4096, "mitigated"},
                {false, "Lucene46", false, 4096, "no"},
                {false, "oakCodec", false, 100, "tiered"},
                {false, "oakCodec", false, 100, "mitigated"},
                {false, "oakCodec", false, 100, "no"},
                {false, "Lucene46", false, 100, "tiered"},
                {false, "Lucene46", false, 100, "mitigated"},
                {false, "Lucene46", false, 100, "no"},
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

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
    }

    @Override
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
                public Directory wrapForRead(String indexPath, IndexDefinition definition,
                                             Directory remote, String dirName) throws IOException {
                    Directory ret = super.wrapForRead(indexPath, definition, remote, dirName);
                    corDir = getFSDirPath(ret);
                    return ret;
                }

                @Override
                public Directory wrapForWrite(IndexDefinition definition,
                                              Directory remote, boolean reindexMode, String dirName) throws IOException {
                    Directory ret = super.wrapForWrite(definition, remote, reindexMode, dirName);
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

    @Test
    public void testLuceneIndexSegmentStats() throws Exception {
        IndexDefinitionBuilder idxb = new IndexDefinitionBuilder().noAsync().codec(codec).mergePolicy(mergePolicy);
        idxb.indexRule("nt:base").property("foo").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        idxb.indexRule("nt:base").property("bin").analyzed().nodeScopeIndex().ordered().useInExcerpt().propertyIndex();
        Tree idx = root.getTree("/").getChild("oak:index").addChild("lucenePropertyIndex");
        Tree idxDef = idxb.build(idx);
        if (!codec.equals("oakCodec") && indexOnFS) {
            idxDef.setProperty("persistence", "file");
            indexPath = "target/index-" + codec + copyOnRW;
            idxDef.setProperty("path", indexPath);
        }


        Random r = new Random();

        System.out.println("***");
        System.out.println(codec + "," + copyOnRW + "," + indexOnFS + "," + minRecordLength + "," + mergePolicy);

        long start = System.currentTimeMillis();
        int multiplier = 5;
        for (int n = 0; n < multiplier; n++) {
            System.err.println("iteration " + (n + 1));

            Tree rootTree = root.getTree("/").addChild("content");
            byte[] bytes = new byte[10240];
            Charset charset = Charset.defaultCharset();
            String text = "";
            for (int i = 0; i < 1000; i++) {
                r.nextBytes(bytes);
                text = new String(bytes, charset);
                Tree tree = rootTree.addChild(String.valueOf(n + i));
                tree.setProperty("foo", text);
                tree.setProperty("bin", bytes);
            }
            root.commit();

            printStats();

            System.out.println("reindex");

            // do nothing, reindex and measure
            idx = root.getTree("/oak:index/lucenePropertyIndex");
            idx.setProperty("reindex", true);
            root.commit();

            printStats();

            System.out.println("add and delete");

            // add and delete some content and measure
            for (int i = 0; i < 1000; i++) {
                r.nextBytes(bytes);
                text = new String(bytes, charset);
                Tree tree = rootTree.addChild(String.valueOf(n + 100 + i));
                tree.setProperty("foo", text);
                tree.setProperty("bin", bytes);
                if (n + i % 3 == 0) { // delete one of the already existing nodes every 3
                    assert rootTree.getChild(String.valueOf(n + i)).remove();
                }
            }
            root.commit();

            printStats();
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("finished in " + (time / (60000)) + " minutes");
        System.out.println("***");
    }

    private double evaluateQuery(String fooQuery) {
        long q1Start = System.currentTimeMillis();
        List<String> res1 = executeQuery(fooQuery, SQL2);
        long q1End = System.currentTimeMillis();
        double time =  (q1End - q1Start) / 1000d;
        assertNotNull(res1);
        assertTrue(res1.size() > 0);
        return time;
    }

    private void printStats() throws IOException {
        fileStore.flush();

        FileStoreStats stats = fileStore.getStats();

        long sizeOfDirectory = FileUtils.sizeOfDirectory(new File(fdsDir));
        String fdsSize = (sizeOfDirectory / (1024 * 1000)) + " MB";

        double time = evaluateQuery(FOO_QUERY);

        System.err.println("||codec||min record length||merge policy||segment size||FDS size||query time||");
        System.err.println("|" + codec + "|" + minRecordLength + "|" + mergePolicy + "|" + IOUtils.humanReadableByteCount(
                stats.getApproximateSize()) + "|" + fdsSize + "|" + time + " s|");

        if (indexOnFS) {
            long sizeOfFSIndex = FileUtils.sizeOfDirectory(new File(indexPath));
            System.out.println("Index on FS size : " + FileUtils.byteCountToDisplaySize(sizeOfFSIndex));
        }
    }

    private long dumpFileStoreTo(File to) throws IOException {
        if (!to.exists()) {
            assert to.mkdirs();
        }
        for (File f : DIRECTORY.listFiles()) {
            Files.copy(f, new File(to.getPath(), f.getName()));
        }

        long sizeOfDirectory = FileUtils.sizeOfDirectory(to);

        to.deleteOnExit();
        return sizeOfDirectory;
    }
}
