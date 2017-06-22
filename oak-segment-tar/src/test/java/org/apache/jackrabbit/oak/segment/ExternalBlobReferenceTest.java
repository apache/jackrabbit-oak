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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBlobReferenceTest {

    @Rule
    public final TemporaryFolder segmentFolder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private BlobStore blobStore;

    @Before
    public void createFileStore() throws Exception {
        blobStore = mock(BlobStore.class);
        fileStore = fileStoreBuilder(segmentFolder.getRoot()).withBlobStore(blobStore).build();
    }

    @After
    public void destroyFileStore() {
        fileStore.close();
    }

    /**
     * The {@code SegmentWriter} should be able to write blob IDs whose length
     * is between 0 and {@code Segment.BLOB_ID_SMALL_LIMIT - 1} bytes. It should
     * be possible to correctly read the blob ID back and pass it to the {@code
     * BlobStore} to obtain information about the blob.
     * <p/>
     * This code path executes only if the written stream is {@code
     * Segment.MEDIUM_LIMIT} bytes long (or more). If the length of the stream
     * is smaller, the binary value is inlined in the segment and the {@code
     * BlobStore} is never called.
     * <p/>
     * See OAK-3105.
     */
    @Test
    public void testShortBlobId() throws Exception {
        testBlobIdWithLength(SegmentTestConstants.BLOB_ID_SMALL_LIMIT - 1);
    }

    /**
     * If the {@code BlobStore} returns a blob ID whose length is {@code
     * Segment.BLOB_ID_SMALL_LIMIT} bytes long (or more), writing the stream
     * should succeed. In this case, the blob ID is considered a long blob ID
     * and an alternate encoding is used. It should be possible to correctly
     * read the blob ID back and pass it to the {@code BlobStore} to obtain
     * information about the blob.
     * <p/>
     * This code path executes only if the written stream is {@code
     * Segment.MEDIUM_LIMIT} bytes long (or more). If the length of the stream
     * is smaller, the binary value is inlined in the segment and the {@code
     * BlobStore} is never called.
     * <p/>
     * See OAK-3105 and OAK-3107.
     */
    @Test
    public void testLongBlobId() throws Exception {
        testBlobIdWithLength(SegmentTestConstants.BLOB_ID_SMALL_LIMIT);
    }

    private void testBlobIdWithLength(int blobIdLength) throws Exception {
        String blobId = Strings.repeat("x", blobIdLength);
        long blobLength = SegmentTestConstants.MEDIUM_LIMIT;

        doReturn(blobId).when(blobStore).writeBlob(any(InputStream.class));
        doReturn(blobLength).when(blobStore).getBlobLength(blobId);

        SegmentBlob blob = new SegmentBlob(blobStore, fileStore.getWriter().writeStream(newRandomInputStream(blobLength)));

        assertEquals(blobLength, blob.length());
    }

    private static InputStream newRandomInputStream(long size) {
        return new LimitInputStream(new ConstantInputStream(0), size);
    }

    private static class ConstantInputStream extends InputStream {

        private final int value;

        public ConstantInputStream(int value) {
            this.value = value;
        }

        @Override
        public int read() {
            return value;
        }

    }

    private static class LimitInputStream extends InputStream {

        private final InputStream stream;

        private final long limit;

        private long read = 0;

        public LimitInputStream(InputStream stream, long limit) {
            this.stream = stream;
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            if (read >= limit) {
                return -1;
            }

            read = read + 1;

            return stream.read();
        }

    }

}
