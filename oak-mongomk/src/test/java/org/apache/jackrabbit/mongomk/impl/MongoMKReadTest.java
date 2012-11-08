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
package org.apache.jackrabbit.mongomk.impl;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for {@code MongoMicroKernel#read(String, long, byte[], int, int)}
 */
public class MongoMKReadTest extends BaseMongoMicroKernelTest {

    private byte[] blob;
    private String blobId;

    // Override to set the right blob store.
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DB db = mongoConnection.getDB();
        dropCollections(db);

        MongoNodeStore nodeStore = new MongoNodeStore(db);
        BlobStore blobStore = new MongoBlobStore(db);
        mk = new MongoMicroKernel(mongoConnection, nodeStore, blobStore);
    }

    @Test
    public void small() throws Exception {
        read(1024);
    }

    @Test
    public void medium() throws Exception {
        read(1024 * 1024);
    }

    @Test
    @Ignore // FIXME - Add it back when OAK-430 is fixed.
    public void large() throws Exception {
        read(20 * 1024 * 1024);
    }

    private void read(int blobLength) throws Exception {
        createAndWriteBlob(blobLength);

        // Complete read.
        byte[] buffer = new byte[blob.length];
        int totalBytes = mk.read(blobId, 0, buffer, 0, blob.length);
        Assert.assertEquals(blob.length, totalBytes);
        Assert.assertTrue(Arrays.equals(blob, buffer));

        // Range end from end.
        buffer = new byte[blob.length / 2];
        totalBytes = mk.read(blobId, (blob.length / 2) - 1, buffer, 0, blob.length / 2);
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
            blob[i] = (byte)1;
        }
        blobId = mk.write(new ByteArrayInputStream(blob));
    }
}