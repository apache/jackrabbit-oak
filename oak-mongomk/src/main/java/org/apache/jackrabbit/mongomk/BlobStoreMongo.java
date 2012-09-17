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
package org.apache.jackrabbit.mongomk;

import java.io.InputStream;

import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.command.Command;
import org.apache.jackrabbit.mongomk.api.command.CommandExecutor;
import org.apache.jackrabbit.mongomk.command.GetBlobLengthCommandMongo;
import org.apache.jackrabbit.mongomk.command.ReadBlobCommandMongo;
import org.apache.jackrabbit.mongomk.command.WriteBlobCommandMongo;
import org.apache.jackrabbit.mongomk.impl.command.CommandExecutorImpl;

public class BlobStoreMongo implements BlobStore {

    private final MongoConnection mongoConnection;
    private final CommandExecutor commandExecutor;

    public BlobStoreMongo(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
        this.commandExecutor = new CommandExecutorImpl();
    }

    @Override
    public long getBlobLength(String blobId) throws Exception {
        Command<Long> command = new GetBlobLengthCommandMongo(mongoConnection, blobId);
        return commandExecutor.execute(command);
    }

    @Override
    public int readBlob(String blobId, long blobOffset, byte[] buffer, int bufferOffset, int length) throws Exception {
        Command<Integer> command = new ReadBlobCommandMongo(mongoConnection, blobId, blobOffset, buffer, bufferOffset, length);
        return commandExecutor.execute(command);
    }

    @Override
    public String writeBlob(InputStream is) throws Exception {
        Command<String> command = new WriteBlobCommandMongo(mongoConnection, is);
        return commandExecutor.execute(command);
    }
}
