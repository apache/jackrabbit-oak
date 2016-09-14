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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.amazonaws.util.StringInputStream;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3DataStoreStatsTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder(new File("target")) {
        @Override public void delete() {
        }
    };

    private NodeStore mockNodeStore;

    private static String testNodePathName = "test/node/path/name";
    private File testFile;

    private SharedS3DataStore defaultS3ds;
    private SharedS3DataStore autoSyncMockS3ds;
    private SharedS3DataStore manualSyncMockS3ds;
    private S3DataStoreStats mBean;

    @Before
    public void setup() throws Exception {
        testFile = folder.newFile();
        copyInputStreamToFile(randomStream(0, 16384), testFile);
        mockNodeStore = mock(NodeStore.class);
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        OutputStream output = new DigestOutputStream(new FileOutputStream(testFile), digest);
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(testFile);
            IOUtils.copyLarge(inputStream, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(inputStream);
        }
        String testNodeId = encodeHexString(digest.digest());

        mockNodeStore = mock(NodeStore.class);
        final NodeState mockRootState = mock(NodeState.class);
        final NodeState mockLeafState = mock(NodeState.class);
        final PropertyState mockLeafPropertyState = mock(PropertyState.class);
        final Blob mockBlob = mock(Blob.class);
        when(mockNodeStore.getRoot()).thenReturn(mockRootState);
        when(mockRootState.getChildNode(anyString())).thenReturn(mockLeafState);
        when(mockLeafState.getChildNode(anyString())).thenReturn(mockLeafState);
        when(mockLeafState.exists()).thenReturn(true);
        when(mockLeafState.getProperty(anyString())).thenReturn(mockLeafPropertyState);
        doReturn(Lists.newArrayList(mockLeafPropertyState)).when(mockLeafState).getProperties();
        doReturn(Type.BINARY).when(mockLeafPropertyState).getType();
        when(mockLeafPropertyState.getValue(Type.BINARY)).thenReturn(mockBlob);
        when(mockBlob.getContentIdentity()).thenReturn(testNodeId);

        defaultS3ds = mock(SharedS3DataStore.class);
        defaultS3ds.init(folder.newFolder().getAbsolutePath());
        autoSyncMockS3ds = new CustomBackendS3DataStore(new TestMemoryBackend());
        autoSyncMockS3ds.init(folder.newFolder().getAbsolutePath());
        manualSyncMockS3ds = new CustomBackendS3DataStore(new ManuallySyncingInMemoryBackend());
        manualSyncMockS3ds.init(folder.newFolder().getAbsolutePath());
    }

    @After
    public void teardown() throws Exception {
    }

    @Test public void testGetActiveS3FileSyncMetricExists()
        throws Exception {

        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = defaultS3ds;

        assertTrue(0 == mBean.getActiveSyncs());
    }

    @Test
    public void testGetSingleActiveS3FileSyncMetric()
        throws Exception {

        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = manualSyncMockS3ds;

        DataRecord record = null;
        try {
            record = manualSyncMockS3ds.addRecord(new StringInputStream("test"));
            assertTrue(1 == mBean.getActiveSyncs());
        } finally {
            if (null != record) {
                manualSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }

        ((ManuallySyncingInMemoryBackend) manualSyncMockS3ds.getBackend()).clearInProgressWrites();

        assertTrue(0 == mBean.getActiveSyncs());
    }

    @Test
    public void testGetMultilpleActiveS3FileSyncMetric()
        throws Exception {

        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = manualSyncMockS3ds;

        final Set<DataRecord> records = Sets.newHashSet();
        try {
            records.add(manualSyncMockS3ds.addRecord(new StringInputStream("test1")));
            records.add(manualSyncMockS3ds.addRecord(new StringInputStream("test2")));
            records.add(manualSyncMockS3ds.addRecord(new StringInputStream("test3")));

            assertTrue(3 == mBean.getActiveSyncs());
        } finally {
            for (final DataRecord record : records) {
                manualSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }

        ((ManuallySyncingInMemoryBackend) manualSyncMockS3ds.getBackend()).clearInProgressWrites();

        assertTrue(0 == mBean.getActiveSyncs());
    }

    @Test
    public void testIsFileSyncedMetricExists()
        throws Exception {

        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = defaultS3ds;

        assertFalse(mBean.isFileSynced(testNodePathName));
    }

    @Test
    public void testIsFileSyncedNullFileReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = defaultS3ds;

        assertFalse(mBean.isFileSynced(null));
    }

    @Test
    public void testIsFileSyncedEmptyStringReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = defaultS3ds;

        assertFalse(mBean.isFileSynced(""));
    }

    @Test
    public void testIsFileSyncedInvalidFilenameReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = defaultS3ds;

        assertFalse(mBean.isFileSynced("invalid"));
    }

    @Test
    public void testIsFileSyncedFileNotAddedReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = autoSyncMockS3ds;

        assertFalse(mBean.isFileSynced(testNodePathName));
    }

    @Test
    public void testIsFileSyncedSyncIncompleteReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = manualSyncMockS3ds;

        DataRecord record = null;
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(testFile);
            record = manualSyncMockS3ds.addRecord(stream);
            assertFalse(mBean.isFileSynced(testNodePathName));
        } finally {
            IOUtils.closeQuietly(stream);
            if (null != record) {
                manualSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }
    }

    @Test
    public void testIsFileSyncedSyncCompleteReturnsTrue()
        throws Exception {

        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = autoSyncMockS3ds;

        DataRecord record = null;
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(testFile);
            record = autoSyncMockS3ds.addRecord(stream);
            assertTrue(mBean.isFileSynced(testNodePathName));
        } finally {
            IOUtils.closeQuietly(stream);
            if (null != record) {
                autoSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }
    }

    @Test
    public void testIsFileSyncedFileDeletedReturnsFalse()
        throws Exception {
        mBean = new S3DataStoreStats();
        mBean.nodeStore = mockNodeStore;
        mBean.s3ds = autoSyncMockS3ds;

        DataRecord record = null;
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(testFile);
            record = autoSyncMockS3ds.addRecord(stream);
        } finally {
            IOUtils.closeQuietly(stream);
            if (null != record) {
                autoSyncMockS3ds.deleteRecord(record.getIdentifier());
            }
        }

        assertFalse(mBean.isFileSynced(testNodePathName));
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


    // A modified InMemoryBackend that, when writeAsync() is called, does not
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
