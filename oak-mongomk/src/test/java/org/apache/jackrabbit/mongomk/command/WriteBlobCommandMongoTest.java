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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.command.WriteBlobCommandMongo;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class WriteBlobCommandMongoTest extends BaseMongoTest {

    @Test
    public void testWriteBlobComplete() throws Exception {
        int blobLength = 100;
        byte[] blob = createBlob(blobLength);

        WriteBlobCommandMongo command = new WriteBlobCommandMongo(mongoConnection,
                new ByteArrayInputStream(blob));
        String blobId = command.execute();
        assertNotNull(blobId);

        byte[] readBlob = new byte[blobLength];
        readBlob(blobId, readBlob);
        assertTrue(Arrays.equals(blob, readBlob));
    }

    private byte[] createBlob(int blobLength) {
        byte[] blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte)i;
        }
        return blob;
    }

    private void readBlob(String blobId, byte[] readBlob) throws IOException {
        GridFS gridFS = mongoConnection.getGridFS();
        GridFSDBFile gridFile = gridFS.findOne(new BasicDBObject("md5", blobId));
        IOUtils.readFully(gridFile.getInputStream(), readBlob, 0, readBlob.length);
    }
}
