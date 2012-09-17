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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;

import org.apache.jackrabbit.mongomk.BaseMongoTest;
import org.apache.jackrabbit.mongomk.command.GetBlobLengthCommandMongo;
import org.junit.Test;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

public class GetBlobLengthCommandMongoTest extends BaseMongoTest {

    @Test
    public void testGetBlobLength() throws Exception {
        int blobLength = 100;
        String blobId = createAndWriteBlob(blobLength);

        GetBlobLengthCommandMongo command = new GetBlobLengthCommandMongo(mongoConnection, blobId);
        long length = command.execute();
        assertEquals(blobLength, length);
    }

    @Test
    public void testNonExistantBlobLength() throws Exception {
        GetBlobLengthCommandMongo command = new GetBlobLengthCommandMongo(mongoConnection,
                "nonExistantBlobId");
        try {
            command.execute();
            fail("Exception expected");
        } catch (Exception expected) {
        }
    }

    private String createAndWriteBlob(int blobLength) {
        byte[] blob = createBlob(blobLength);
        return writeBlob(blob);
    }

    private byte[] createBlob(int blobLength) {
        byte[] blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte)i;
        }
        return blob;
    }

    private String writeBlob(byte[] blob) {
        GridFS gridFS = mongoConnection.getGridFS();
        GridFSInputFile gridFSInputFile = gridFS.createFile(new ByteArrayInputStream(blob), true);
        gridFSInputFile.save();
        return gridFSInputFile.getMD5();
    }
}
