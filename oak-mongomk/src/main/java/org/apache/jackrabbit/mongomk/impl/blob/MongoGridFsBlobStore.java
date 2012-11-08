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
package org.apache.jackrabbit.mongomk.impl.blob;

import java.io.InputStream;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.impl.command.DefaultCommandExecutor;
import org.apache.jackrabbit.mongomk.impl.command.blob.GetBlobLengthCommandGridFS;
import org.apache.jackrabbit.mongomk.impl.command.blob.ReadBlobCommandGridFS;
import org.apache.jackrabbit.mongomk.impl.command.blob.WriteBlobCommandGridFS;

import com.mongodb.DB;
import com.mongodb.gridfs.GridFS;

/**
 * Implementation of {@link BlobStore} for the {@code MongoDB} using GridFS.
 */
public class MongoGridFSBlobStore implements BlobStore {

    private final CommandExecutor commandExecutor;
    private final GridFS gridFS;

    /**
     * Constructs a new {@code BlobStoreMongoGridFS}
     *
     * @param db The DB.
     */
    public MongoGridFSBlobStore(DB db) {
        commandExecutor = new DefaultCommandExecutor();
        gridFS = new GridFS(db);
    }

    @Override
    public long getBlobLength(String blobId) throws Exception {
        Command<Long> command = new GetBlobLengthCommandGridFS(gridFS, blobId);
        return commandExecutor.execute(command);
    }

    @Override
    public int readBlob(String blobId, long blobOffset, byte[] buffer, int bufferOffset, int length) throws Exception {
        Command<Integer> command = new ReadBlobCommandGridFS(gridFS, blobId, blobOffset,
                buffer, bufferOffset, length);
        return commandExecutor.execute(command);
    }

    @Override
    public String writeBlob(InputStream is) throws Exception {
        Command<String> command = new WriteBlobCommandGridFS(gridFS, is);
        return commandExecutor.execute(command);
    }
}
