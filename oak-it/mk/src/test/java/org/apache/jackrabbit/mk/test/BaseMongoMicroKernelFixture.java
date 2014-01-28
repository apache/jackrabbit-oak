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
package org.apache.jackrabbit.mk.test;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;

public abstract class BaseMongoMicroKernelFixture implements MicroKernelFixture {

    protected static final String HOST =
            System.getProperty("mongo.host", "127.0.0.1");

    protected static final int PORT =
            Integer.getInteger("mongo.port", 27017);

    protected static final String DB =
            System.getProperty("mongo.db", "MongoMKDB");

    private MongoConnection mongoConnection = null;

    private MongoConnection getMongoConnection() throws Exception {
        if (mongoConnection == null) {
            mongoConnection = new MongoConnection(HOST, PORT, DB);
        }
        return mongoConnection;
    }

    @Override
    public boolean isAvailable() {
        MongoConnection connection = null;
        try {
            connection = new MongoConnection(HOST, PORT, DB);
            connection.getDB().command(new BasicDBObject("ping", 1));
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public void setUpCluster(MicroKernel[] cluster) throws Exception {
        MongoConnection connection = getMongoConnection();
        DB db = connection.getDB();
        dropCollections(db);

        for (int i = 0; i < cluster.length; i++) {
            cluster[i] = new DocumentMK.Builder().
                    setMongoDB(db).setClusterId(i).open();
        }
    }

    @Override
    public void syncMicroKernelCluster(MicroKernel... nodes) {
        // not needed
    }

    @Override
    public void tearDownCluster(MicroKernel[] cluster) {
        try {
            DB db = getMongoConnection().getDB();
            dropCollections(db);
            mongoConnection.close();
            mongoConnection = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected abstract BlobStore getBlobStore(DB db);

    private static void dropCollections(DB db) {
        db.getCollection(MongoBlobStore.COLLECTION_BLOBS).drop();
        db.getCollection(Collection.NODES.toString()).drop();
    }
}
