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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory.UNIQUE_KEY_SIZE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ChunkedOakDirectoryTest extends OakDirectoryTestBase {

    @Test
    public void writes_CustomBlobSize() throws Exception{
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, 300);
        Directory dir = createDir(builder, false, "/foo");
        assertWrites(dir, 300);
    }

    @Test
    public void dirNameInException_Flush() throws Exception{
        FailOnDemandBlobStore blobStore = new FailOnDemandBlobStore();
        FileStore store = FileStoreBuilder.fileStoreBuilder(tempFolder.getRoot())
                .withMemoryMapping(false)
                .withBlobStore(blobStore)
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();

        String indexPath = "/foo/bar";

        int minFileSize = SegmentTestConstants.MEDIUM_LIMIT;
        int blobSize = minFileSize + 1000;

        builder = nodeStore.getRoot().builder();
        builder.setProperty(LuceneIndexConstants.BLOB_SIZE, blobSize);
        Directory dir = createDir(builder, false, indexPath);

        IndexOutput o3 = dir.createOutput("test1.txt", IOContext.DEFAULT);
        o3.writeBytes(randomBytes(minFileSize), minFileSize);

        blobStore.startFailing();
        try {
            o3.flush();
            fail();
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString(indexPath));
            assertThat(e.getMessage(), containsString("test1.txt"));
        }

        store.close();
    }

    @Override
    void assertBlobSizeInWrite(PropertyState jcrData, int blobSize, int fileSize) {
        List<Blob> blobs = newArrayList(jcrData.getValue(BINARIES));
        assertEquals(blobSize + UNIQUE_KEY_SIZE, blobs.get(0).length());
    }

    @Override
    OakDirectoryBuilder getOakDirectoryBuilder(NodeBuilder builder, IndexDefinition indexDefinition) {
        return new OakDirectoryBuilder(builder, indexDefinition, false);
    }

    @Override
    MemoryBlobStore getBlackHoleBlobStore() {
        return new BlackHoleBlobStoreForSmallBlobs();
    }

    private static class BlackHoleBlobStoreForSmallBlobs extends MemoryBlobStore {
        private String blobId;
        private byte[] data;
        @Override
        protected synchronized void storeBlock(byte[] digest, int level, byte[] data) {
            //Eat up all the writes
        }

        @Override
        public String writeBlob(InputStream in) throws IOException {
            //Avoid expensive digest calculation as all content is 0 byte. So memorize
            //the id if same content is passed
            if (blobId == null) {
                data = IOUtils.toByteArray(in);
                blobId = super.writeBlob(new ByteArrayInputStream(data));
                return blobId;
            } else {
                byte[] bytes = IOUtils.toByteArray(in);
                if (Arrays.equals(data, bytes)) {
                    return blobId;
                }
                return super.writeBlob(new ByteArrayInputStream(bytes));
            }
        }

        @Override
        protected byte[] readBlockFromBackend(BlockId id) {
            return data;
        }
    }
}