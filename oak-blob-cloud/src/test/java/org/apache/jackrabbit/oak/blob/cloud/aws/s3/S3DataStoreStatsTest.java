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
package org.apache.jackrabbit.oak.blob.cloud.aws.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.AsyncTouchCallback;
import org.apache.jackrabbit.core.data.AsyncTouchResult;
import org.apache.jackrabbit.core.data.AsyncUploadCallback;
import org.apache.jackrabbit.core.data.AsyncUploadResult;
import org.apache.jackrabbit.core.data.Backend;
import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3DataStoreStatsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private static NodeStore nodeStore;
    private static Blob mockBlob;

    private static String testNodePathName = "test/node/path/name";
    private File testFile;

    private SharedS3DataStore defaultS3ds;
    private SharedS3DataStore autoSyncMockS3ds;
    private SharedS3DataStore manualSyncMockS3ds;
    private S3DataStoreStats stats;

    @Before
    public void setup() throws Exception {
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

        defaultS3ds = mock(SharedS3DataStore.class);
        autoSyncMockS3ds = new CustomBackendS3DataStore(new TestMemoryBackend());
        autoSyncMockS3ds.init(folder.newFolder().getAbsolutePath());
        manualSyncMockS3ds = new CustomBackendS3DataStore(new ManuallySyncingInMemoryBackend());
        manualSyncMockS3ds.init(folder.newFolder().getAbsolutePath());
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

    @After
    public void teardown() throws Exception {
    }

    @Test public void testGetActiveS3FileSyncMetricExists()
        throws Exception {

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = defaultS3ds;

        assertTrue(0 == stats.getActiveSyncs());
    }

    @Test
    public void testGetSingleActiveS3FileSyncMetric()
        throws Exception {

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = manualSyncMockS3ds;

        DataRecord record = null;
        try {
            record = manualSyncMockS3ds.addRecord(getStream("test"));
            assertTrue(1 == stats.getActiveSyncs());
        } finally {
            if (null != record) {
                manualSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }

        ((ManuallySyncingInMemoryBackend) manualSyncMockS3ds.getBackend()).clearInProgressWrites();

        assertTrue(0 == stats.getActiveSyncs());
    }

    @Test
    public void testGetMultilpleActiveS3FileSyncMetric()
        throws Exception {

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = manualSyncMockS3ds;

        final Set<DataRecord> records = Sets.newHashSet();
        try {
            records.add(manualSyncMockS3ds.addRecord(getStream("test1")));
            records.add(manualSyncMockS3ds.addRecord(getStream("test2")));
            records.add(manualSyncMockS3ds.addRecord(getStream("test3")));

            assertTrue(3 == stats.getActiveSyncs());
        } finally {
            for (final DataRecord record : records) {
                manualSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }

        ((ManuallySyncingInMemoryBackend) manualSyncMockS3ds.getBackend()).clearInProgressWrites();

        assertTrue(0 == stats.getActiveSyncs());
    }

    @Test
    public void testIsFileSyncedMetricExists()
        throws Exception {

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = defaultS3ds;

        assertFalse(stats.isFileSynced(testNodePathName));
    }

    @Test
    public void testIsFileSyncedNullFileReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = defaultS3ds;

        assertFalse(stats.isFileSynced(null));
    }

    @Test
    public void testIsFileSyncedEmptyStringReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = defaultS3ds;

        assertFalse(stats.isFileSynced(""));
    }

    @Test
    public void testIsFileSyncedInvalidFilenameReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = defaultS3ds;

        assertFalse(stats.isFileSynced("invalid"));
    }

    @Test
    public void testIsFileSyncedFileNotAddedReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertFalse(stats.isFileSynced(testNodePathName));
    }

    @Test
    public void testIsFileSyncedSyncIncompleteReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = manualSyncMockS3ds;

        assertSyncedFalse(stats, manualSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedSyncCompleteReturnsTrue()
        throws Exception {

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedTrue(stats, autoSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedFileDeletedReturnsFalse()
        throws Exception {
        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        DataRecord record = null;
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(testFile);
            record = autoSyncMockS3ds.addRecord(stream);
        } finally {
            IOUtils.closeQuietly(stream);
            delete(autoSyncMockS3ds, Lists.<DataRecord>newArrayList(record));
        }
        assertFalse(stats.isFileSynced(testNodePathName));
    }

    @Test
    public void testIsFileSyncedDifferentPaths() throws Exception {
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

        final NodeStore nodeStore = new MemoryNodeStore();
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

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        final List<DataRecord> records = Lists.newArrayList();
        try {
            for (final String s : blobContents) {
                records.add(autoSyncMockS3ds.addRecord(getStream(s)));
            }

            for (final String path : Lists.newArrayList(path1, path2, path3, path4, leadingSlashPath)) {
                assertTrue(stats.isFileSynced(path));
            }

            for (final String invalidPath : Lists.newArrayList(path1 + "/", "/" + path1 + "/", "/path//to/node///1")) {
                try {
                    stats.isFileSynced(invalidPath);
                    assertFalse(false); // shouldn't get here on an invalid path
                }
                catch (AssertionError e) {
                    // expected
                }
            }
        }
        finally {
            delete(autoSyncMockS3ds, records);
        }
    }

    @Test
    public void testIsFileSyncedMultiplePropertiesReturnsTrue() throws Exception {
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.<Blob>absent(),
            Optional.of("abc"),
            Optional.of(123),
            Optional.<List<Blob>>absent());

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedTrue(stats, autoSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedMultipleBinaryPropertiesAllSyncedReturnsTrue() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.of(mockBlob2),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.<List<Blob>>absent());

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedTrue(stats, autoSyncMockS3ds, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void testIsFileSyncedMultipleBinaryPropertiesNotAllSyncedReturnsFalse() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        NodeStore nodeStore = initNodeStore(Optional.of(mockBlob),
            Optional.of(mockBlob2),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.<List<Blob>>absent());

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedFalse(stats, autoSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedBinariesPropertySingleReturnsTrue() throws Exception {
        List<Blob> blobPropList = Lists.newArrayList(mockBlob);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedTrue(stats, autoSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedBinariesPropertyMultipleReturnsTrue() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob, mockBlob2);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedTrue(stats, autoSyncMockS3ds, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void testIsFileSyncedBinariesPropertyNotAllSyncedReturnsFalse() throws Exception {
        Blob mockBlob2 = mock(Blob.class);
        final String id2 = getIdForInputStream(getStream("testContents2"));
        when(mockBlob2.getContentIdentity()).thenReturn(id2);
        List<Blob> blobPropList = Lists.newArrayList(mockBlob, mockBlob2);
        NodeStore nodeStore = initNodeStore(Optional.<Blob>absent(),
            Optional.<Blob>absent(),
            Optional.<String>absent(),
            Optional.<Integer>absent(),
            Optional.of(blobPropList));

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedFalse(stats, autoSyncMockS3ds, new FileInputStream(testFile));
    }

    @Test
    public void testIsFileSyncedBinarySyncedAndBinariesNotSyncedReturnsFalse() throws Exception {
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

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedFalse(stats, autoSyncMockS3ds, new FileInputStream(testFile),
            getStream("testContents2"));
    }

    @Test
    public void testIsFileSyncedBinaryNotSyncedAndBinariesSyncedReturnsFalse() throws Exception {
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

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedFalse(stats, autoSyncMockS3ds, getStream("testContents2"),
            getStream("testContents3"));
    }

    @Test
    public void testIsFileSyncedBinaryAndBinariesSyncedReturnsTrue() throws Exception {
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

        stats = new S3DataStoreStats();
        stats.nodeStore = nodeStore;
        stats.s3ds = autoSyncMockS3ds;

        assertSyncedFalse(stats, autoSyncMockS3ds, new FileInputStream(testFile),
            getStream("testContents2"), getStream("testContents3"));
    }

    private static void delete(SharedS3DataStore s3ds, List<DataRecord> recs) throws DataStoreException {
        for (DataRecord rec : recs) {
            if (null != rec) {
                s3ds.deleteRecord(rec.getIdentifier());
            }
        }
    }

    private static void assertSyncedFalse(S3DataStoreStats mBean, SharedS3DataStore s3ds,
        InputStream... streams) throws DataStoreException {

        List<DataRecord> recs = Lists.newArrayList();
        try {
            for (InputStream is : streams) {
                recs.add(s3ds.addRecord(is));
                IOUtils.closeQuietly(is);
            }
            assertFalse(mBean.isFileSynced(testNodePathName));
        } finally {
            delete(s3ds, recs);
        }
    }

    private static void assertSyncedTrue(S3DataStoreStats mBean, SharedS3DataStore s3ds,
        InputStream... streams) throws DataStoreException {

        List<DataRecord> recs = Lists.newArrayList();
        try {
            for (InputStream is : streams) {
                recs.add(s3ds.addRecord(is));
                IOUtils.closeQuietly(is);
            }
            assertTrue(mBean.isFileSynced(testNodePathName));
        } finally {
            delete(s3ds, recs);
        }
    }

    private static InputStream getStream(String str) {
        return new ByteArrayInputStream(str.getBytes(Charsets.UTF_8));
    }

    // A mock S3DataStore that allows us to replace the default
    // S3Backend with our own backend, for test purposes only.
    private class CustomBackendS3DataStore extends SharedS3DataStore {
        private Backend _localBackend;

        CustomBackendS3DataStore(final Backend backend) {
            _localBackend = backend;
        }

        @Override protected Backend createBackend() {
            return _localBackend;
        }
    }


    // A mock Backend implementation that uses a Map to keep track of what
    // records have been added and removed, for test purposes only.
    private class TestMemoryBackend implements Backend {
        final Map<DataIdentifier, File> _backend = Maps.newHashMap();

        @Override public void init(CachingDataStore store, String homeDir, String config)
            throws DataStoreException {

        }

        @Override public InputStream read(DataIdentifier identifier) throws DataStoreException {
            try {
                return new FileInputStream(_backend.get(identifier));
            } catch (FileNotFoundException e) {
                throw new DataStoreException(e);
            }
        }

        @Override public long getLength(DataIdentifier identifier) throws DataStoreException {
            return _backend.get(identifier).length();
        }

        @Override public long getLastModified(DataIdentifier identifier) throws DataStoreException {
            return _backend.get(identifier).lastModified();
        }

        @Override public void write(DataIdentifier identifier, File file)
            throws DataStoreException {
            _backend.put(identifier, file);
        }

        @Override public void writeAsync(final DataIdentifier identifier, final File file,
            AsyncUploadCallback callback) throws DataStoreException {
            write(identifier, file);
            callback.onSuccess(new AsyncUploadResult(identifier, file));
        }

        @Override public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
            return _backend.keySet().iterator();
        }

        @Override public boolean exists(DataIdentifier identifier, boolean touch)
            throws DataStoreException {
            if (_backend.containsKey(identifier) && touch) {
                touch(identifier, new DateTime().getMillis());
            }
            return exists(identifier);
        }

        @Override public boolean exists(DataIdentifier identifier) throws DataStoreException {
            return _backend.containsKey(identifier);
        }

        @Override public void touch(DataIdentifier identifier, long minModifiedDate)
            throws DataStoreException {

        }

        @Override public void touchAsync(DataIdentifier identifier, long minModifiedDate,
            AsyncTouchCallback callback) throws DataStoreException {
            callback.onSuccess(new AsyncTouchResult(identifier));
        }

        @Override public void close() throws DataStoreException {

        }

        @Override public Set<DataIdentifier> deleteAllOlderThan(long timestamp)
            throws DataStoreException {
            final Set<DataIdentifier> toDelete = Sets.newHashSet();
            for (final DataIdentifier identifier : _backend.keySet()) {
                if (_backend.get(identifier).lastModified() < timestamp) {
                    toDelete.add(identifier);
                }
            }
            for (final DataIdentifier identifier : toDelete) {
                _backend.remove(identifier);
            }
            return toDelete;
        }

        @Override public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
            if (_backend.containsKey(identifier)) {
                _backend.remove(identifier);
            }
        }
    }


    // A modified TestMemoryBackend that, when writeAsync() is called, does not
    // actually store the record but keeps track that it was intended to be
    // stored, and allows the test to tell it when it expects the record
    // to be "synced".
    private class ManuallySyncingInMemoryBackend extends TestMemoryBackend {
        final Map<DataIdentifier, File> inProgessWrites = Maps.newHashMap();
        final Map<DataIdentifier, AsyncUploadCallback> asyncCallbacks = Maps.newHashMap();

        @Override public void writeAsync(final DataIdentifier identifier, final File file,
            AsyncUploadCallback callback) throws DataStoreException {
            inProgessWrites.put(identifier, file);
            asyncCallbacks.put(identifier, callback);
        }

        void clearInProgressWrites() throws DataStoreException {
            for (final DataIdentifier identifier : inProgessWrites.keySet()) {
                final File file = inProgessWrites.get(identifier);
                asyncCallbacks.get(identifier).onSuccess(new AsyncUploadResult(identifier, file));
                write(identifier, file);
            }
            inProgessWrites.clear();
        }
    }

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}
