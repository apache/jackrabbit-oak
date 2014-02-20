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

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreConfiguration;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;

import com.mongodb.DB;

/**
 * The Class MongoCloudBlobMicroKernelFixture.
 */
public class MongoDataStoreBlobMicroKernelFixture extends BaseMongoMicroKernelFixture {

    /** The blob store. */
    private BlobStore blobStore;

    /**
     * Open connection.
     * 
     * @throws Exception
     */
    protected void openConnection() throws Exception {
        if (blobStore == null) {
            blobStore =
                    DataStoreBlobStoreBuilder
                            .newInstance()
                            .build(
                                    BlobStoreConfiguration.newInstance().loadFromSystemProps()).get();
        }
    }

    @Override
    protected BlobStore getBlobStore(com.mongodb.DB db) {
        return blobStore;
    }

    @Override
    public void setUpCluster(MicroKernel[] cluster) throws Exception {
        MongoConnection connection = getMongoConnection();
        openConnection();
        DB db = connection.getDB();
        dropCollections(db);

        for (int i = 0; i < cluster.length; i++) {
            cluster[i] = new DocumentMK.Builder().
                    setMongoDB(db).setBlobStore(blobStore).setClusterId(i).open();
        }
    }

    @Override
    protected void dropCollections(DB db) {
        db.getCollection(MongoBlobStore.COLLECTION_BLOBS).drop();
        db.getCollection(Collection.NODES.toString()).drop();
        try {
            ((DataStoreBlobStore) blobStore).clearInUse();
            ((DataStoreBlobStore) blobStore).getDataStore()
                    .deleteAllOlderThan(System.currentTimeMillis() + 1000000);
        } catch (DataStoreException e) {
        }
    }
}
