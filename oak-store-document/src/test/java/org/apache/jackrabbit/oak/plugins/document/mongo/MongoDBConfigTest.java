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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConfig.CollectionCompressor;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConfig.getCollectionStorageOptions;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConfig.COLLECTION_COMPRESSION_TYPE;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConfig.STORAGE_CONFIG;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDBConfig.STORAGE_ENGINE;


public class MongoDBConfigTest {

    @Test
    public void defaultCollectionStorageOptions() {
        Bson bson = getCollectionStorageOptions(Collections.emptyMap());
        BsonDocument bsonDocument = bson.toBsonDocument(BasicDBObject.class, MongoClient.getDefaultCodecRegistry());
        String  configuredCompressor = bsonDocument.getDocument(STORAGE_ENGINE).getString(STORAGE_CONFIG).getValue();
        assertTrue(configuredCompressor.indexOf(CollectionCompressor.SNAPPY.getName()) > 0);

    }

    @Test (expected = IllegalArgumentException.class)
    public void invalidCollectionStorageOptions() {
        getCollectionStorageOptions(Collections.singletonMap(COLLECTION_COMPRESSION_TYPE, "Invalid"));
        fail("Should fail with IllegalArgumentException due to Invalid Compressor");
    }

    @Test
    public void overrideDefaultCollectionStorageOptions() {
        Bson bson = getCollectionStorageOptions(Collections.singletonMap(COLLECTION_COMPRESSION_TYPE, "zstd"));
        BsonDocument bsonDocument = bson.toBsonDocument(BasicDBObject.class, MongoClient.getDefaultCodecRegistry());
        String  configuredCompressor = bsonDocument.getDocument(STORAGE_ENGINE).getString(STORAGE_CONFIG).getValue();

        assertTrue(configuredCompressor.indexOf(CollectionCompressor.ZSTD.getName()) > 0);
    }

}
