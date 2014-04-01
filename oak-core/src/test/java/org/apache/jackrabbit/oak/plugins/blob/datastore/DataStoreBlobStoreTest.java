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
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreInputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataStoreBlobStoreTest {

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
        assertEquals(dr.getIdentifier().toString(), ds.writeBlob(new ByteArrayInputStream(data)));
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

        assertEquals(actualSize, ds.getBlobLength(dr.getIdentifier().toString()));
        assertEquals(testDI.toString(), ds.writeBlob(new ByteArrayInputStream(data)));
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
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        assertEquals(reference,ds.getReference(blobId));
        assertEquals(blobId, ds.getBlobId(reference));

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

        DataStore mockedDS = mock(DataStore.class);
        when(mockedDS.getAllIdentifiers()).thenReturn(dis.iterator());
        when(mockedDS.getRecord(new DataIdentifier("d-10"))).thenReturn(new TimeDataRecord(d10));
        when(mockedDS.getRecord(new DataIdentifier("d-20"))).thenReturn(new TimeDataRecord(d20));
        when(mockedDS.getRecord(new DataIdentifier("d-30"))).thenReturn(new TimeDataRecord(d30));
        DataStoreBlobStore ds = new DataStoreBlobStore(mockedDS);

        Iterator<String> chunks = ds.getAllChunkIds(25);
        Set<String> expected = Sets.newHashSet("d-10","d-20");
        assertEquals(expected, Sets.newHashSet(chunks));

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
            throw new UnsupportedOperationException();
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
