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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStoreTest;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreInputStream;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore.BlobId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStoreBlobStoreTest extends AbstractBlobStoreTest {
    @Before
    @Override
    public void setUp() throws Exception {
        store = DataStoreUtils.getBlobStore();
    }

    @Override
    protected void setupCollector(BlobStatsCollector statsCollector) {
        if (store instanceof DataStoreBlobStore){
            ((DataStoreBlobStore) store).setBlobStatsCollector(statsCollector);
        }
    }

    @Override
    protected boolean supportsStatsCollection() {
        return true;
    }

    @Test
    public void testInlineBinary() throws DataStoreException, IOException {
        int maxInlineSize = 300;

        DataStore mockedDS = mock(DataStore.class);
        when(mockedDS.getMinRecordLength()).thenReturn(maxInlineSize);
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        byte[] data = new byte[maxInlineSize];
        new Random().nextBytes(data);

        DataRecord dr = ds.addRecord(new ByteArrayInputStream(data));
        assertTrue(InMemoryDataRecord.isInstance(dr.getIdentifier().toString()));
        assertTrue(IOUtils.contentEquals(new ByteArrayInputStream(data), dr.getStream()));
        assertTrue(IOUtils.contentEquals(new ByteArrayInputStream(data),
                new BlobStoreInputStream(ds, dr.getIdentifier().toString(), 0)));

        assertEquals(dr, ds.getRecordIfStored(dr.getIdentifier()));
        assertEquals(dr, ds.getRecord(dr.getIdentifier()));

        //Check for BlobStore methods
        assertEquals(maxInlineSize, ds.getBlobLength(dr.getIdentifier().toString()));
        assertEquals(dr.getIdentifier().toString(), BlobId.of(ds.writeBlob(new ByteArrayInputStream(data))).blobId);
    }

    @Test
    public void testExternalBinary() throws DataStoreException, IOException {
        int maxInlineSize = 300;
        int actualSize = maxInlineSize + 10;

        byte[] data = new byte[actualSize];
        new Random().nextBytes(data);

        DataIdentifier testDI = new DataIdentifier("test");
        DataRecord testDR = new ByteArrayDataRecord(data, testDI, "testReference");

        DataStore mockedDS = mock(DataStore.class);
        when(mockedDS.getMinRecordLength()).thenReturn(maxInlineSize);
        when(mockedDS.getRecord(testDI)).thenReturn(testDR);
        when(mockedDS.getRecordIfStored(testDI)).thenReturn(testDR);
        when(mockedDS.addRecord(any(InputStream.class))).thenReturn(testDR);
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);


        DataRecord dr = ds.addRecord(new ByteArrayInputStream(data));
        assertFalse(InMemoryDataRecord.isInstance(dr.getIdentifier().toString()));
        assertEquals(testDI, dr.getIdentifier());
        assertTrue(IOUtils.contentEquals(new ByteArrayInputStream(data), dr.getStream()));
        assertTrue(IOUtils.contentEquals(new ByteArrayInputStream(data),
                new BlobStoreInputStream(ds, dr.getIdentifier().toString(), 0)));

        assertEquals(dr, ds.getRecordIfStored(dr.getIdentifier()));
        assertEquals(dr, ds.getRecord(dr.getIdentifier()));

//        assertTrue(ds.getInputStream(dr.getIdentifier().toString()) instanceof BufferedInputStream);
        assertEquals(actualSize, ds.getBlobLength(dr.getIdentifier().toString()));
        assertEquals(testDI.toString(), BlobId.of(ds.writeBlob(new ByteArrayInputStream(data))).blobId);
    }

    @Test
    public void testReference() throws DataStoreException, IOException {
        String reference = "testReference";
        String blobId = "test";
        DataIdentifier testDI = new DataIdentifier(blobId);
        DataRecord testDR = new ByteArrayDataRecord("foo".getBytes(), testDI, reference);

        DataStore mockedDS = mock(DataStore.class);
        when(mockedDS.getRecordFromReference(reference)).thenReturn(testDR);
        when(mockedDS.getRecord(testDI)).thenReturn(testDR);
        when(mockedDS.getRecordIfStored(testDI)).thenReturn(testDR);
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        assertEquals(reference,ds.getReference(blobId));
        assertEquals(blobId, BlobId.of(ds.getBlobId(reference)).blobId);
        assertEquals(BlobId.of(testDR).encodedValue(),ds.getBlobId(reference));

        String inMemBlobId = InMemoryDataRecord.getInstance("foo".getBytes())
                .getIdentifier().toString();

        //For in memory record the reference should be null
        assertNull(ds.getReference(inMemBlobId));
    }

    @Test
    public void testGetAllChunks() throws Exception{
        DataIdentifier d10 = new DataIdentifier("d-10");
        DataIdentifier d20 = new DataIdentifier("d-20");
        DataIdentifier d30 = new DataIdentifier("d-30");
        List<DataIdentifier> dis = ImmutableList.of(d10, d20, d30);
        List<DataRecord> recs = Lists.newArrayList(
            Iterables.transform(dis, new Function<DataIdentifier, DataRecord>() {
                @Nullable
                @Override
                public DataRecord apply(@Nullable DataIdentifier input) {
                    return new TimeDataRecord(input);
                }
        }));
        OakFileDataStore mockedDS = mock(OakFileDataStore.class);
        when(mockedDS.getAllRecords()).thenReturn(recs.iterator());
        when(mockedDS.getRecord(new DataIdentifier("d-10"))).thenReturn(new TimeDataRecord(d10));
        when(mockedDS.getRecord(new DataIdentifier("d-20"))).thenReturn(new TimeDataRecord(d20));
        when(mockedDS.getRecord(new DataIdentifier("d-30"))).thenReturn(new TimeDataRecord(d30));
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        Iterator<String> chunks = ds.getAllChunkIds(25);
        Set<String> expected = Sets.newHashSet("d-10","d-20");
        assertEquals(expected, Sets.newHashSet(chunks));
    }

    @Test
    public void testEncodedBlobId() throws Exception{
        BlobId blobId = new BlobId("abc"+BlobId.SEP+"123");
        assertEquals("abc", blobId.blobId);
        assertEquals(123, blobId.length);

        blobId = new BlobId("abc"+BlobId.SEP+"abc"+BlobId.SEP+"123");
        assertEquals("abc"+BlobId.SEP+"abc", blobId.blobId);
        assertEquals(123, blobId.length);

        blobId = new BlobId("abc",123);
        assertEquals("abc"+BlobId.SEP+"123", blobId.encodedValue());

        assertTrue(BlobId.isEncoded("abc"+BlobId.SEP+"123"));
        assertFalse(BlobId.isEncoded("abc"));
    }

    @Test
    public void testAddOnTrackError() throws Exception {
        int maxInlineSize = 300;
        byte[] data = new byte[maxInlineSize];
        new Random().nextBytes(data);

        DataStore mockedDS = mock(DataStore.class);
        when(mockedDS.getMinRecordLength()).thenReturn(maxInlineSize);
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        BlobIdTracker mockedTracker = mock(BlobIdTracker.class);
        doThrow(new IOException("Mocking tracking error")).when(mockedTracker).add(any(String.class));
        ds.addTracker(mockedTracker);

        String id = ds.writeBlob(new ByteArrayInputStream(data));
        assertTrue(IOUtils.contentEquals(new ByteArrayInputStream(data), ds.getInputStream(id)));
    }

    @Override
    @Test
    public void testCombinedIdentifier() throws Exception {
    }

    @Override
    public void testEmptyIdentifier() throws Exception {
    }

    @Override
    @Test
    public void testGarbageCollection() throws Exception {
    }

    @After
    @Override
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File(DataStoreUtils.getHomeDir()));
        super.tearDown();
    }

    private static class ByteArrayDataRecord implements DataRecord {
        private final byte[] data;
        private final DataIdentifier identifier;
        private final String reference;

        private ByteArrayDataRecord(byte[] data, DataIdentifier di, String reference) {
            this.data = data;
            this.identifier = di;
            this.reference = reference;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getReference() {
            return reference;
        }

        @Override
        public long getLength() throws DataStoreException {
            return data.length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            return new ByteArrayInputStream(data);
        }

        @Override
        public long getLastModified() {
            return 0;
        }
    }

    private static class TimeDataRecord implements DataRecord {
        private final DataIdentifier id;

        private TimeDataRecord(DataIdentifier id) {
            this.id = id;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return id;
        }

        @Override
        public String getReference() {
            return id.toString();
        }

        @Override
        public long getLength() throws DataStoreException {
            throw new DataStoreException(new UnsupportedOperationException());
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLastModified() {
            return Long.valueOf(id.toString().substring(id.toString().indexOf('-')+1));
        }
    }
}
