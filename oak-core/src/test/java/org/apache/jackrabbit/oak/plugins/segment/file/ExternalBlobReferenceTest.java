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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.InputStream;
import java.util.Random;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ExternalBlobReferenceTest {

    @Rule
    public TemporaryFolder segmentFolder = new TemporaryFolder();

    private FileStore fileStore;

    private BlobStore blobStore;

    @Before
    public void createFileStore() throws Exception {
        blobStore = mock(BlobStore.class);
        fileStore = FileStore.newFileStore(segmentFolder.getRoot()).withBlobStore(blobStore).create();
    }

    @After
    public void destroyFileStore() throws Exception {
        fileStore.close();
    }

    /**
     * The {@code SegmentWriter} should be able to write blob IDs whose length
     * is between 0 and 4095 bytes. It should be possible to correctly read the
     * blob ID back and pass it to the {@code BlobStore} to obtain information
     * about the blob.
     * <p/>
     * This code path executes only if the written stream is {@code
     * Segment.MEDIUM_LIMIT} bytes long (or more). If the length of the stream
     * is smaller, the binary value is inlined in the segment and the {@code
     * BlobStore} is never called.
     * <p/>
     * See OAK-3105.
     */
    @Test
    public void testBlobIdLengthUpperLimit() throws Exception {
        String blobId = Strings.repeat("x", 4095);
        long blobLength = Segment.MEDIUM_LIMIT;

        doReturn(blobId).when(blobStore).writeBlob(any(InputStream.class));
        doReturn(blobLength).when(blobStore).getBlobLength(blobId);

        SegmentBlob blob = fileStore.getTracker().getWriter().writeStream(newRandomInputStream(blobLength));

        assertEquals(blobLength, blob.length());
    }

    /**
     * If the {@code BlobStore} returns a blob ID whose length is 4096 byes long
     * (or more), writing the stream should throw an {@code
     * IllegalArgumentException}.
     * <p/>
     * This code path executes only if the written stream is {@code
     * Segment.MEDIUM_LIMIT} bytes long (or more). If the length of the stream
     * is smaller, the binary value is inlined in the segment and the {@code
     * BlobStore} is never called.
     * <p/>
     * See OAK-3105.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testBlobIdLengthLongerThanUpperLimit() throws Exception {
        String blobId = Strings.repeat("x", 4096);
        long blobLength = Segment.MEDIUM_LIMIT;

        doReturn(blobId).when(blobStore).writeBlob(any(InputStream.class));

        fileStore.getTracker().getWriter().writeStream(newRandomInputStream(blobLength));
    }

    private InputStream newRandomInputStream(final long size) {
        return new InputStream() {

            private Random random = new Random();

            private int read = 0;

            @Override
            public int read() {
                if (read >= size) {
                    return -1;
                }

                read += 1;

                return random.nextInt() & 0xFF;
            }

        };
    }

}
