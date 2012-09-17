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
package org.apache.jackrabbit.mongomk.command;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.command.ReadBlobCommandMongo;
import org.junit.Test;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

public class ReadBlobCommandMongoTest extends BaseMongoTest {

    private byte[] blob;
    private String blobId;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        blob = new byte[100];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) i;
        }
        ByteArrayInputStream is = new ByteArrayInputStream(blob);
        GridFS gridFS = mongoConnection.getGridFS();
        GridFSInputFile gridFSInputFile = gridFS.createFile(is, true);
        gridFSInputFile.save();
        blobId = gridFSInputFile.getMD5();
    }

    @Test
    public void testReadBlobComplete() throws Exception {
        byte[] buffer = new byte[blob.length];
        ReadBlobCommandMongo command = new ReadBlobCommandMongo(mongoConnection, blobId, 0, buffer, 0, blob.length);
        int totalBytes = command.execute();

        Assert.assertEquals(blob.length, totalBytes);
        Assert.assertTrue(Arrays.equals(blob, buffer));
    }

    @Test
    public void testReadBlobRangeFromEnd() throws Exception {
        byte[] buffer = new byte[blob.length / 2];
        ReadBlobCommandMongo command = new ReadBlobCommandMongo(mongoConnection, blobId, (blob.length / 2) - 1,
                buffer, 0, blob.length / 2);
        int totalBytes = command.execute();

        Assert.assertEquals(blob.length / 2, totalBytes);
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals(blob[((blob.length / 2) - 1) + i], buffer[i]);
        }
    }

    @Test
    public void testReadBlobRangeFromStart() throws Exception {
        byte[] buffer = new byte[blob.length / 2];
        ReadBlobCommandMongo command = new ReadBlobCommandMongo(mongoConnection, blobId, 0, buffer, 0,
                blob.length / 2);
        int totalBytes = command.execute();

        Assert.assertEquals(blob.length / 2, totalBytes);
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals(blob[i], buffer[i]);
        }
    }
}
