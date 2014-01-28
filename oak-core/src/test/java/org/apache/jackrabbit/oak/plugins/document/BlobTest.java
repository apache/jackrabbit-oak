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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

/**
 * Tests the blob store.
 */
public class BlobTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(RandomizedClusterTest.class);

//     private static final boolean MONGO_DB = true;
//     private static final long TOTAL_SIZE = 1 * 1024 * 1024 * 1024;
//     private static final int DOCUMENT_COUNT = 10;

    private static final boolean MONGO_DB = false;
    private static final long TOTAL_SIZE = 1 * 1024 * 1024;
    private static final int DOCUMENT_COUNT = 10;
    
    DB openMongoConnection() {
        return MONGO_DB ? MongoUtils.getConnection().getDB() : null;
    }
    
    void dropCollections() {
        if (MONGO_DB) {
            MongoUtils.dropCollections(MongoUtils.getConnection().getDB());
        }
    }

    @Test
    public void addBlobs() throws Exception {
        DocumentMK mk = new DocumentMK.Builder().
                setMongoDB(openMongoConnection()).open();
        long blobSize = TOTAL_SIZE / DOCUMENT_COUNT;
        ArrayList<String> blobIds = new ArrayList<String>();
        // use a new seed each time, to allow running the test multiple times 
        Random r = new Random();
        for (int i = 0; i < DOCUMENT_COUNT; i++) {
            log("writing " + i + "/" + DOCUMENT_COUNT);
            String id = mk.write(new RandomStream(blobSize, r.nextInt()));
            blobIds.add(id);
        }
        for (String id : blobIds) {
            assertEquals(blobSize, mk.getLength(id));
        }
        mk.dispose();
    }
    
    private static void log(String s) {
        LOG.info(s);
    }
    
}
