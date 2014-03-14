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
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.BlobStoreInputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        DataRecord testDR = new ByteArrayDataRecord(data, testDI);

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

    private static class ByteArrayDataRecord implements DataRecord {
        private final byte[] data;
        private final DataIdentifier identifier;

        private ByteArrayDataRecord(byte[] data, DataIdentifier di) {
            this.data = data;
            this.identifier = di;
        }

        @Override
        public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override
        public String getReference() {
            return null;
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
}
