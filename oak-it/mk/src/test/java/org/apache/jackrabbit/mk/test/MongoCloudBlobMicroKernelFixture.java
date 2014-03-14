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

import java.util.Map;

import com.google.common.collect.Maps;
import com.mongodb.DB;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.plugins.blob.cloud.CloudBlobStore;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * The Class MongoCloudBlobMicroKernelFixture.
 */
public class MongoCloudBlobMicroKernelFixture extends BaseMongoMicroKernelFixture {
    /** The blob store. */
    private BlobStore blobStore;

    /**
     * Open connection.
     * 
     * @throws Exception
     */
    protected void openConnection() throws Exception {
        if (blobStore == null) {
            Map<String,?> config = getConfig();
            if(!config.isEmpty()){
                CloudBlobStore cbs  = new CloudBlobStore();
                PropertiesUtil.populate(cbs, config, false);
                cbs.init();
                blobStore = cbs;
            }
            blobStore = null;
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
        ((CloudBlobStore) blobStore).deleteBucket();
    }

    /**
     * See org.apache.jackrabbit.oak.plugins.document.blob.ds.DataStoreUtils#getConfig()
     */
    private static Map<String,?> getConfig(){
        Map<String,Object> result = Maps.newHashMap();
        for(Map.Entry<String,?> e : Maps.fromProperties(System.getProperties()).entrySet()){
            String key = e.getKey();
            if(key.startsWith("bs.")){
                key = key.substring(3); //length of bs.
                result.put(key, e.getValue());
            }
        }
        return result;
    }
}
