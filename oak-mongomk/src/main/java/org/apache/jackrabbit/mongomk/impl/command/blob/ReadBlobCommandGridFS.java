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
package org.apache.jackrabbit.mongomk.impl.command.blob;

import java.io.InputStream;

import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mongomk.impl.command.BaseCommand;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * {@code Command} for {@code MongoMicroKernel#read(String, long, byte[], int, int)}
 * FIXME - Reading from large blobs with small increments is slow in this implementation.
 * See if this could be improved with some kind of cache mechanism.
 */
public class ReadBlobCommandGridFS extends BaseCommand<Integer> {

    private final GridFS gridFS;
    private final String blobId;
    private final long blobOffset;
    private final byte[] buffer;
    private final int bufferOffset;
    private int length;

    /**
     * Constructs a new {@code ReadBlobCommandMongo}.
     *
     * @param gridFS GridFS instance.
     * @param blobId Blob id.
     * @param blobOffset Blob offset.
     * @param buffer Buffer.
     * @param bufferOffset Buffer offset.
     * @param length Length.
     */
    public ReadBlobCommandGridFS(GridFS gridFS, String blobId, long blobOffset,
            byte[] buffer, int bufferOffset, int length) {
        super();
        this.gridFS = gridFS;
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

    private int fetchBlobFromMongo() throws Exception {
        GridFSDBFile gridFile = gridFS.findOne(new BasicDBObject("md5", blobId));
        long fileLength = gridFile.getLength();
        long start = blobOffset;
        long end = blobOffset + length;
        if (end > fileLength) {
            end = fileLength;
        }
        length = (int)(end - start);

        if (start < end) {
            InputStream is = gridFile.getInputStream();
            if (blobOffset > 0) {
                IOUtils.skipFully(is, blobOffset);
            }
            IOUtils.readFully(is, buffer, bufferOffset, length);
            is.close();
            return length;
        }
        return -1;
    }
}
