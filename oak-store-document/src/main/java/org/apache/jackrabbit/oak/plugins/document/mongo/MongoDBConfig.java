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

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.Map;

public class MongoDBConfig {
    public static final String COLLECTION_COMPRESSION_TYPE = "collectionCompressionType";
    public static final String STORAGE_ENGINE = "wiredTiger";
    public static final String STORAGE_CONFIG = "configString";

    enum CollectionCompressor {
        SNAPPY("snappy"), ZLIB("zlib"), ZSTD("zstd");

        private final String compressionType;

        private static final Map<String, CollectionCompressor> supportedCompressionTypes = ImmutableMap.of(
                "snappy", CollectionCompressor.SNAPPY, "zlib",
                CollectionCompressor.ZLIB, "zstd", CollectionCompressor.ZSTD);

        CollectionCompressor(String compressionType) {
            this.compressionType = compressionType;
        }

        public String getCompressionType() {
            return compressionType;
        }

        public static boolean isSupportedCompressor(String compressionType) {
            return supportedCompressionTypes.containsKey(compressionType);
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
                CollectionCompressor.SNAPPY.getCompressionType());

        if (CollectionCompressor.isSupportedCompressor(compressionType)) {
            JsonObject root = new JsonObject();
            JsonObject configString = new JsonObject();
            configString.getProperties().put(STORAGE_CONFIG,
                    getCompressionConfig(mongoStorageOptions));
            root.getChildren().put(STORAGE_ENGINE, configString);

            Bson storageOptions = BsonDocument.parse(root.toString());
            return storageOptions;
        } else {
            throw new IllegalArgumentException("Invalid collection compression type provided " + compressionType);
        }
    }

    private static String getCompressionConfig(
            Map<String, String> storageOptions) {
        return "\"block_compressor=" + storageOptions.getOrDefault(
                COLLECTION_COMPRESSION_TYPE, "snappy") + "\"";
    }

}
