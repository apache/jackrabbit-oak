/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

public interface Constants {
    String META_DIR_NAME = "META";
    String META_KEY_PREFIX = META_DIR_NAME + "/";

    String REF_KEY = "reference.key";
    String LAST_MODIFIED_KEY = "lastModified";

    long BUFFERED_STREAM_THRESHOLD = 8L * 1024L * 1024L; // 8 MiB
    long MIN_MULTIPART_UPLOAD_PART_SIZE = 256L * 1024L; // 256 KiB minimum
    long MAX_MULTIPART_UPLOAD_PART_SIZE = 4000L * 1024L * 1024L; // 4000 MiB (4 GiB) Azure limit
    long MAX_SINGLE_PUT_UPLOAD_SIZE = 256L * 1024L * 1024L; // 256 MiB Azure limit
    long MAX_BINARY_UPLOAD_SIZE = 190L * 1024L * 1024L * 1024L * 1024L; // ~190.7 TiB Azure limit
    int MAX_ALLOWABLE_UPLOAD_URIS = 50000; // Azure limit for blocks per blob
    int MAX_UNIQUE_RECORD_TRIES = 10;
    int DEFAULT_CONCURRENT_REQUEST_COUNT = 5; // Optimal for SDK 12
    int MAX_CONCURRENT_REQUEST_COUNT = 10; // Optimal maximum for SDK 12
    long MAX_BLOCK_SIZE = 100L * 1024L * 1024L; // 100 MiB maximum block size in SDK 12
    int MAX_RETRY_REQUESTS = 4; // Default retry count in SDK 12
    int DEFAULT_TIMEOUT_SECONDS = 60; // Default timeout in seconds
}
