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

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class ReadBlobCommandMongo extends AbstractCommand<Integer> {

    private final MongoConnection mongoConnection;
    private final String blobId;
    private final long blobOffset;
    private final byte[] buffer;
    private final int bufferOffset;
    private final int length;

    public ReadBlobCommandMongo(MongoConnection mongoConnection, String blobId, long blobOffset, byte[] buffer,
            int bufferOffset, int length) {
        this.mongoConnection = mongoConnection;
        this.blobId = blobId;
        this.blobOffset = blobOffset;
        this.buffer = buffer;
        this.bufferOffset = bufferOffset;
        this.length = length;
    }

    @Override
    public Integer execute() throws Exception {
        return fetchBlobFromMongo();
    }

    // FIXME [Mete] This takes a long time, see MicroKernelIT#readBlob. See if
    // it can be improved.
    private int fetchBlobFromMongo() throws Exception {
        GridFS gridFS = mongoConnection.getGridFS();
        GridFSDBFile gridFile = gridFS.findOne(new BasicDBObject("md5", blobId));
        long fileLength = gridFile.getLength();

        long start = blobOffset;
        long end = blobOffset + length;
        if (end > fileLength) {
            end = fileLength;
        }

        int totalBytes = -1;
        if (start < end) {
            InputStream is = gridFile.getInputStream();
            IOUtils.skipFully(is, blobOffset);
            totalBytes = is.read(buffer, bufferOffset, length);
            is.close();
        }
        return totalBytes;
    }
}
