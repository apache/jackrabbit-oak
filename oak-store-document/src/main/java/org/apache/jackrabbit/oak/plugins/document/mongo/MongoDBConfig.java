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

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MongoDBConfig {
    static final String COLLECTION_COMPRESSION_TYPE = "collectionCompressionType";
    static final String STORAGE_ENGINE = "wiredTiger";
    static final String STORAGE_CONFIG = "configString";

    enum CollectionCompressor {
        SNAPPY("snappy"), ZLIB("zlib"), ZSTD("zstd");

        private final String compressionType;
        private static final Map<String, CollectionCompressor> COLLECTION_COMPRESSOR_MAP;

        static {
            Map<String, CollectionCompressor> builder = new HashMap<>();
            for (CollectionCompressor value : CollectionCompressor.values()) {
                builder.put(value.getName().toLowerCase(), value);
            }
            COLLECTION_COMPRESSOR_MAP = Collections.unmodifiableMap(builder);
        }

        CollectionCompressor(String compressionType) {
            this.compressionType = compressionType;
        }

        public String getName() {
            return compressionType;
        }

        public static boolean isSupportedCompressor(String compressionType) {
            return COLLECTION_COMPRESSOR_MAP.containsKey(compressionType.toLowerCase());
        }


    }

    /**
     * reads all storage options from map backed by config, and constructs
     * storage options for collection to be created
     *
     * @param mongoStorageOptions
     * @return
     */
    public static Bson getCollectionStorageOptions(
            Map<String, String> mongoStorageOptions) {

        String compressionType = mongoStorageOptions.getOrDefault(
                COLLECTION_COMPRESSION_TYPE,
                CollectionCompressor.SNAPPY.getName());

        if (CollectionCompressor.isSupportedCompressor(compressionType)) {
            JsonObject root = new JsonObject();
            JsonObject configString = new JsonObject();
            configString.getProperties().put(STORAGE_CONFIG,
                    getCompressionConfig(mongoStorageOptions));
            root.getChildren().put(STORAGE_ENGINE, configString);

            Bson storageOptions = BsonDocument.parse(root.toString());
            return storageOptions;
        } else {
            throw new IllegalArgumentException("Invalid collection compressionType provided: " + compressionType);
        }
    }

    private static String getCompressionConfig(
            Map<String, String> storageOptions) {
        return "\"block_compressor=" + storageOptions.getOrDefault(
                COLLECTION_COMPRESSION_TYPE, "snappy") + "\"";
    }

}
