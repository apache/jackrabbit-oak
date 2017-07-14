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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsolidatedDataStoreStatsTest extends AbstractDataStoreCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConsolidatedDataStoreStatsTest.class);
    private static final String ID_PREFIX = "12345";
    private static String testNodePathName = "test/node/path/name";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private final Closer closer = Closer.create();
    private File root;
    private File testFile;

    private CountDownLatch taskLatch;
    private CountDownLatch callbackLatch;
    private CountDownLatch afterExecuteLatch;
    private TestExecutor executor;
    private StatisticsProvider statsProvider;
    private ScheduledExecutorService scheduledExecutor;
    private ConsolidatedDataStoreCacheStats stats;
    private NodeStore nodeStore;
    private AbstractSharedCachingDataStore dataStore;
    private static Blob mockBlob;

    @Before
    public void setup() throws Exception {
        root = folder.newFolder();
        init(1);
    }

    private void init(int i) throws Exception {
        testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 16384), testFile);
        String testNodeId = getIdForInputStream(new FileInputStream(testFile));
        mockBlob = mock(Blob.class);
        when(mockBlob.getContentIdentity()).thenReturn(testNodeId);

        nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.<List<Blob>>absent());

        // create executor
        taskLatch = new CountDownLatch(1);
        callbackLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(i);
        executor = new TestExecutor(1, taskLatch, callbackLatch, afterExecuteLatch);

        // stats
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(statsExecutor, 500, TimeUnit.MILLISECONDS));
        statsProvider = new DefaultStatisticsProvider(statsExecutor);

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(scheduledExecutor, 500, TimeUnit.MILLISECONDS));

        final File datastoreRoot = folder.newFolder();
        dataStore = new AbstractSharedCachingDataStore() {
            @Override protected AbstractSharedBackend createBackend() {
                return new TestMemoryBackend(datastoreRoot);
            }

            @Override public int getMinRecordLength() {
                return 0;
            }
        };
        dataStore.setStatisticsProvider(statsProvider);
        dataStore.listeningExecutor = executor;
        dataStore.schedulerExecutor = scheduledExecutor;
        dataStore.init(root.getAbsolutePath());

        stats = new ConsolidatedDataStoreCacheStats();
        stats.nodeStore = nodeStore;
        stats.cachingDataStore = dataStore;
    }

    @After
    public void tear() throws Exception {
        closer.close();
        dataStore.close();
    }

    private String getIdForInputStream(final InputStream in)
        throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
        try {
            IOUtils.copyLarge(in, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
        return encodeHexString(digest.digest());
    }

    private static NodeStore initNodeStore(final Optional<Blob> blobProp1,
        final Optional<Blob> blobProp2,
        final Optional<String> stringProp,
        final Optional<Integer> intProp,
        final Optional<List<Blob>> blobPropList)
        throws CommitFailedException {
        final NodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder builder = initNodeBuilder(rootBuilder);

        if (blobProp1.isPresent()) {
            builder.setProperty("blobProp1", blobProp1.get());
        }
        if (blobProp2.isPresent()) {
            builder.setProperty("blobProp2", blobProp2.get());
        }
        if (stringProp.isPresent()) {
            builder.setProperty("stringProp", stringProp.get());
        }
        if (intProp.isPresent()) {
            builder.setProperty("intProp", intProp.get());
        }
        if (blobPropList.isPresent()) {
            builder.setProperty(MultiBinaryPropertyState
                .binaryPropertyFromBlob("blobPropList", blobPropList.get()));
        }

        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        return nodeStore;
    }

    private static NodeBuilder initNodeBuilder(final NodeBuilder rootBuilder) {
        NodeBuilder builder = rootBuilder;
        for (final String nodeName : PathUtils.elements(testNodePathName)) {
            builder = builder.child(nodeName);
        }
        return builder;
    }

    @Test
    public void noPath()
        throws Exception {
        assertFalse(stats.isFileSynced(testNodePathName));
    }

    @Test
    public void nullString()
        throws Exception {
        assertFalse(stats.isFileSynced(null));
    }

    @Test
    public void emptyString()
        throws Exception {
        assertFalse(stats.isFileSynced(""));
    }

    @Test
    public void differentPaths() throws Exception {
        init(4);
        final NodeStore nodeStore = new MemoryNodeStore();
        stats.nodeStore = nodeStore;

        final String path1 = "path/to/node/1";
        final String path2 = "path/to/node/2";
        final String path3 = "shortpath";
        final String path4 = "a/very/very/long/path/leads/to/node/4";
        final List<String> paths = Lists.newArrayList(path1, path2, path3, path4);
        final String leadingSlashPath = "/" + path1;

        final List<String> blobContents = Lists.newArrayList("1", "2", "3", "4");
        final List<Blob> blobs = Lists.newArrayList(
            mock(Blob.class),
            mock(Blob.class),
            mock(Blob.class),
            mock(Blob.class)
        );
        final List<String> blobIds = Lists.newArrayList(
            getIdForInputStream(getStream(blobContents.get(0))),
            getIdForInputStream(getStream(blobContents.get(1))),
            getIdForInputStream(getStream(blobContents.get(2))),
            getIdForInputStream(getStream(blobContents.get(3)))
        );
        when(blobs.get(0).getContentIdentity()).thenReturn(blobIds.get(0));
        when(blobs.get(1).getContentIdentity()).thenReturn(blobIds.get(1));
        when(blobs.get(2).getContentIdentity()).thenReturn(blobIds.get(2));
        when(blobs.get(3).getContentIdentity()).thenReturn(blobIds.get(3));

        final NodeBuilder rootBuilder = nodeStore.getRoot().builder();

        final List<NodeBuilder> builders = Lists.newArrayList();
        for (final String path : paths) {
            NodeBuilder builder = rootBuilder;
            for (final String nodeName : PathUtils.elements(path)) {
                builder = builder.child(nodeName);
            }
            builders.add(builder);
        }
        builders.get(0).setProperty("blob1", blobs.get(0));
        builders.get(1).setProperty("blob2", blobs.get(1));
        builders.get(2).setProperty("blob3", blobs.get(2));
        builders.get(3).setProperty("blob4", blobs.get(3));

        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final List<DataRecord> records = Lists.newArrayList();
        try {
            for (final String s : blobContents) {
                records.add(dataStore.addRecord(getStream(s)));
            }

            taskLatch.countDown();
            callbackLatch.countDown();
            waitFinish();

            for (final String path : Lists
                .newArrayList(path1, path2, path3, path4, leadingSlashPath)) {
                assertTrue(stats.isFileSynced(path));
            }

            for (final String invalidPath : Lists
                .newArrayList(path1 + "/", "/" + path1 + "/", "/path//to/node///1")) {
                try {
                    stats.isFileSynced(invalidPath);
                    assertFalse(false); // shouldn't get here on an invalid path
                } catch (AssertionError e) {
                    // expected
                }
            }
        }
        finally {
            delete(dataStore, records);
        }
    }

    @Test
    public void multiplePropertiesAndBinarySynced() throws Exception {
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.of("abc"),
            Optional.of(123),
            Optional.<List<Blob>>absent());

        assertSyncedTrue(stats, dataStore, new FileInputStream(testFile));
    }

    @Test
    public void multipleBinaryPropsAllSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.of(mockBlob2),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.<List<Blob>>absent());

        assertSyncedTrue(stats, dataStore, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void multipleBinaryPropsNotAllSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.of(mockBlob2),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.<List<Blob>>absent());

        assertSyncedFalse(stats, dataStore, new FileInputStream(testFile));
    }

    @Test
    public void binaryPropSingle() throws Exception {
        List<Blob> blobPropList = Lists.newArrayList(mockBlob);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        assertSyncedTrue(stats, dataStore, new FileInputStream(testFile));
    }

    @Test
    public void binariesPropertyMultiple() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob, mockBlob2);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        assertSyncedTrue(stats, dataStore, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void binariesPropertyNotAllSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob, mockBlob2);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        assertSyncedFalse(stats, dataStore, new FileInputStream(testFile));
    }

    @Test
    public void binarySyncedAndBinariesNotSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        Blob mockBlob3 = mock(Blob.class);
        final String id3 = getIdForInputStream(getStream("testContents3"));
        when(mockBlob2.getContentIdentity()).thenReturn(id3);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob2, mockBlob3);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        assertSyncedFalse(stats, dataStore, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void binaryNotSyncedAndBinariesSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        Blob mockBlob3 = mock(Blob.class);
        final String id3 = getIdForInputStream(getStream("testContents3"));
        when(mockBlob2.getContentIdentity()).thenReturn(id3);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob2, mockBlob3);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));
        assertSyncedFalse(stats, dataStore, getStream("testContents2"),
            getStream("testContents3"));
    }

    @Test
    public void binaryAndBinariesSynced() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        Blob mockBlob3 = mock(Blob.class);
        final String id3 = getIdForInputStream(getStream("testContents3"));
        when(mockBlob3.getContentIdentity()).thenReturn(id3);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob2, mockBlob3);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        assertSyncedFalse(stats, dataStore, new FileInputStream(testFile),
            getStream("testContents2"), getStream("testContents3"));
    }

    private static void delete(AbstractSharedCachingDataStore s3ds, List<DataRecord> recs)
        throws DataStoreException {
        for (DataRecord rec : recs) {
            if (null != rec) {
                s3ds.deleteRecord(rec.getIdentifier());
            }
        }
    }

    private void assertSyncedFalse(ConsolidatedDataStoreCacheStats mBean,
        AbstractSharedCachingDataStore s3ds, InputStream... streams) throws DataStoreException {

        List<DataRecord> recs = Lists.newArrayList();
        try {
            for (InputStream is : streams) {
                recs.add(s3ds.addRecord(is));
                IOUtils.closeQuietly(is);
            }
            assertFalse(mBean.isFileSynced(testNodePathName));
            taskLatch.countDown();
            callbackLatch.countDown();
            waitFinish();
        } finally {
            delete(s3ds, recs);
        }
    }

    private void assertSyncedTrue(ConsolidatedDataStoreCacheStats mBean,
        AbstractSharedCachingDataStore s3ds, InputStream... streams) throws DataStoreException {
        taskLatch.countDown();
        callbackLatch.countDown();

        List<DataRecord> recs = Lists.newArrayList();
        try {
            for (InputStream is : streams) {
                recs.add(s3ds.addRecord(is));
                IOUtils.closeQuietly(is);
            }
            waitFinish();
            assertTrue(mBean.isFileSynced(testNodePathName));
        } finally {
            delete(s3ds, recs);
        }
    }

    private void waitFinish() {
        try {
            // wait for upload finish
            afterExecuteLatch.await();
            // Force execute removal from staging cache
            ScheduledFuture<?> scheduledFuture = scheduledExecutor
                .schedule(dataStore.getCache().getStagingCache().new RemoveJob(), 0,
                    TimeUnit.MILLISECONDS);
            scheduledFuture.get();
            LOG.info("After jobs completed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static InputStream getStream(String str) {
        return new ByteArrayInputStream(str.getBytes(Charsets.UTF_8));
    }
}
