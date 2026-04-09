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

import java.io.InputStream;
import java.time.Instant;

import org.jetbrains.annotations.NotNull;

public interface AzureBlobContainer extends AutoCloseable {
    void createIfNotExists() throws Exception;
    void delete() throws Exception;
    boolean deleteIfExists() throws Exception;
    boolean exists() throws Exception;
    @NotNull
    String getName();
    @NotNull
    String getContainerUri();
    void uploadBlockBlob(@NotNull String name, @NotNull InputStream input, long length) throws Exception;
    @NotNull
    String generateSharedAccessSignature(@NotNull Instant expiry) throws Exception;

    @Override
    default void close() throws Exception {
        // Most implementations do not own extra resources.
    }
}
