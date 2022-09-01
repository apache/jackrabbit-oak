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
import static org.junit.Assert.assertTrue;


public class MongoDBConfigTest {

    private MongoDBConfig mongoDBConfig;

    @Test
    public void defaultCollectionStorageOptions() {
        Bson bson = mongoDBConfig.getCollectionStorageOptions(Collections.emptyMap());
        BsonDocument bsonDocument = bson.toBsonDocument(BasicDBObject.class, MongoClient.getDefaultCodecRegistry());
        String  configuredCompressor = bsonDocument.getDocument(MongoDBConfig.STORAGE_ENGINE).getString(MongoDBConfig.STORAGE_CONFIG).getValue();
        assertTrue(configuredCompressor.indexOf(CollectionCompressor.SNAPPY.getCompressionType()) > 0);

    }

    @Test (expected = IllegalArgumentException.class)
    public void invalidCollectionStorageOptions() throws Exception {
        mongoDBConfig.getCollectionStorageOptions(Collections.singletonMap(MongoDBConfig.COLLECTION_COMPRESSION_TYPE, "Invalid"));
    }

    @Test
    public void overrideDefaultCollectionStorageOptions() {
        Bson bson = mongoDBConfig.getCollectionStorageOptions(Collections.singletonMap(MongoDBConfig.COLLECTION_COMPRESSION_TYPE, "zstd"));
        BsonDocument bsonDocument = bson.toBsonDocument(BasicDBObject.class, MongoClient.getDefaultCodecRegistry());
        String  configuredCompressor = bsonDocument.getDocument(MongoDBConfig.STORAGE_ENGINE).getString(MongoDBConfig.STORAGE_CONFIG).getValue();

        assertTrue(configuredCompressor.indexOf(CollectionCompressor.ZSTD.getCompressionType()) > 0);
    }



}
