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
package org.apache.jackrabbit.oak.plugins.document.blob;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.junit.Test;

/**
 * Tests for {@code MongoMicroKernel#read(String, long, byte[], int, int)}
 */
public class DocumentMKReadTest extends AbstractMongoConnectionTest {

    private byte[] blob;
    private String blobId;

    @Test
    public void small() throws Exception {
        read(1024, false);
    }

    @Test
    public void medium() throws Exception {
        read(1024 * 1024, false);
    }

    @Test
    public void large() throws Exception {
        // Skip range tests for large blobs for now as it's complicated with
        // block size.
        read(20 * 1024 * 1024, true);
    }

    private void read(int blobLength, boolean skipRangeTests) throws Exception {
        createAndWriteBlob(blobLength);

        // Complete read.
        byte[] buffer = readFully(mk, blobId);
        Assert.assertEquals(blob.length, buffer.length);
        Assert.assertTrue(Arrays.equals(blob, buffer));

        if (skipRangeTests) {
            return;
        }

        // Range end from end.
        buffer = new byte[blob.length / 2];
        int totalBytes = mk.read(blobId, (blob.length / 2) - 1, buffer, 0, blob.length / 2);
        Assert.assertEquals(blob.length / 2, totalBytes);
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals(blob[((blob.length / 2) - 1) + i], buffer[i]);
        }

        // Range from start.
        buffer = new byte[blob.length / 2];
        totalBytes = mk.read(blobId, 0, buffer, 0, blob.length / 2);
        Assert.assertEquals(blob.length / 2, totalBytes);
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals(blob[i], buffer[i]);
        }
    }

    private void createAndWriteBlob(int blobLength) throws Exception {
        blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) 1;
        }
        blobId = mk.write(new ByteArrayInputStream(blob));
    }
}