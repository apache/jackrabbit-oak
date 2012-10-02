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

import org.apache.jackrabbit.mongomk.api.command.AbstractCommand;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class GetBlobLengthCommandMongo extends AbstractCommand<Long> {

    private final MongoConnection mongoConnection;
    private final String blobId;

    public GetBlobLengthCommandMongo(MongoConnection mongoConnection, String blobId) {
        this.mongoConnection = mongoConnection;
        this.blobId = blobId;
    }

    @Override
    public Long execute() throws Exception {
        GridFS gridFS = mongoConnection.getGridFS();
        GridFSDBFile gridFSDBFile = gridFS.findOne(new BasicDBObject("md5", blobId));
        if (gridFSDBFile == null) {
            throw new Exception("Blob does not exiss");
        }
        return gridFSDBFile.getLength();
    }
}
