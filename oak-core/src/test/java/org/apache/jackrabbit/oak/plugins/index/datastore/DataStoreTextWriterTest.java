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

package org.apache.jackrabbit.oak.plugins.index.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText.ExtractionResult;
import org.apache.jackrabbit.oak.plugins.index.fulltext.PreExtractedTextProvider;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DataStoreTextWriterTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void basicOperation() throws Exception {
        File fdsDir = temporaryFolder.newFolder();
        FileDataStore fds = DataStoreUtils.createFDS(fdsDir, 0);
        ByteArrayInputStream is = new ByteArrayInputStream("hello".getBytes());
        DataRecord dr = fds.addRecord(is);

        File writerDir = temporaryFolder.newFolder();
        TextWriter writer = new DataStoreTextWriter(writerDir, false);
        writer.write(dr.getIdentifier().toString(), "hello");

        FileDataStore fds2 = DataStoreUtils.createFDS(writerDir, 0);
        DataRecord dr2 = fds2.getRecordIfStored(dr.getIdentifier());

        is.reset();
        assertTrue(IOUtils.contentEquals(is, dr2.getStream()));

    }

    @Test
    public void noLoadingInReadOnlyMode() throws Exception{
        DataStoreTextWriter w = new DataStoreTextWriter(temporaryFolder.getRoot(), true);
        assertEquals(0, w.getEmptyBlobsHolder().getLoadCount());
        assertEquals(0, w.getErrorBlobsHolder().getLoadCount());

        DataStoreTextWriter w1 = new DataStoreTextWriter(temporaryFolder.getRoot(), false);
        assertEquals(1, w1.getEmptyBlobsHolder().getLoadCount());
        assertEquals(1, w1.getErrorBlobsHolder().getLoadCount());
    }

    @Test
    public void checkEmptyAndErrorBlobs() throws Exception{
        DataStoreTextWriter w = new DataStoreTextWriter(temporaryFolder.getRoot(), false);
        w.markEmpty("a");
        w.markError("b");
        w.close();

        DataStoreTextWriter w2 = new DataStoreTextWriter(temporaryFolder.getRoot(), true);
        assertEquals(ExtractionResult.EMPTY, w2.getText("/a", new IdBlob("foo", "a")).getExtractionResult());
        assertEquals(ExtractionResult.ERROR, w2.getText("/a", new IdBlob("foo", "b")).getExtractionResult());
    }

    @Test
    public void nonExistingEntry() throws Exception{
        File fdsDir = temporaryFolder.newFolder();
        FileDataStore fds = DataStoreUtils.createFDS(fdsDir, 0);
        ByteArrayInputStream is = new ByteArrayInputStream("hello".getBytes());
        DataRecord dr = fds.addRecord(is);

        File writerDir = temporaryFolder.newFolder();
        DataStoreTextWriter w = new DataStoreTextWriter(writerDir, false);
        String id = dr.getIdentifier().toString();
        assertFalse(w.isProcessed(id));
        assertNull(w.getText("/a", new IdBlob("foo", id)));

        w.write(id, "foo");
        assertTrue(w.isProcessed(id));
        ExtractedText et = w.getText("/a", new IdBlob("foo", id));
        assertEquals("foo", et.getExtractedText());
        assertEquals(ExtractionResult.SUCCESS, et.getExtractionResult());

        w.markEmpty("a");
        assertTrue(w.isProcessed("a"));

    }
    
    @Test
    public void inMemoryRecord() throws Exception{
        File fdsDir = temporaryFolder.newFolder();
        FileDataStore fds = DataStoreUtils.createFDS(fdsDir, 10000);
        DataStoreBlobStore dbs = new DataStoreBlobStore(fds);
        ByteArrayInputStream is = new ByteArrayInputStream("".getBytes());
        String blobId = dbs.writeBlob(is);

        File writerDir = temporaryFolder.newFolder();
        PreExtractedTextProvider textProvider = new DataStoreTextWriter(writerDir, true);
        assertNull(textProvider.getText("/content", new BlobStoreBlob(dbs, blobId)));
    }

    private static class IdBlob extends ArrayBasedBlob {
        final String id;

        public IdBlob(String value, String id) {
            super(value.getBytes());
            this.id = id;
        }

        @Override
        public String getContentIdentity() {
            return id;
        }
    }
}