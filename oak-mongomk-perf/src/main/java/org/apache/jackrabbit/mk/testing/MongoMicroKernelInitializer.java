/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.testing;

import java.util.ArrayList;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.util.Configuration;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.*;

import com.mongodb.BasicDBObjectBuilder;

/**
 * Creates a {@code MongoMicroKernel}.Initialize the mongo database for the
 * tests.
 */
public class MongoMicroKernelInitializer implements MicroKernelInitializer {

    public ArrayList<MicroKernel> init(Configuration conf, int mksNumber)
            throws Exception {

        ArrayList<MicroKernel> mks = new ArrayList<MicroKernel>();

        MongoConnection mongoConnection;
        mongoConnection = new MongoConnection(conf.getHost(),
                conf.getMongoPort(), conf.getMongoDatabase());

        Thread.sleep(1000);
        MongoClearCollections.clearAllCollections(mongoConnection);
        mongoConnection.close();

        mongoConnection = new MongoConnection(conf.getHost(),
                conf.getMongoPort(), "admin");
        // set the shard key
        mongoConnection.getDB()
                .command(
                        BasicDBObjectBuilder
                                .start("shardCollection", "test.nodes")
                                .push("key").add("path", 1).add("revId", 1)
                                .pop().get());

        for (int i = 0; i < mksNumber; i++) {
            mongoConnection = new MongoConnection(conf.getHost(),
                    conf.getMongoPort(), conf.getMongoDatabase());
            MongoNodeStore nodeStore = new MongoNodeStore(
                    mongoConnection.getDB());
            // MongoAssert.setNodeStore(nodeStore);
            BlobStore blobStore = new MongoGridFSBlobStore(
                    mongoConnection.getDB());
            mks.add(new MongoMicroKernel(mongoConnection, nodeStore, blobStore));
        }

        return mks;
    }

    public String getType() {
        return "Mongo Microkernel implementation";
    }
}
