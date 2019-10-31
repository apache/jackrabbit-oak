/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.segment.azure.compat;

import com.azure.storage.blob.BlobServiceClient;
import org.jetbrains.annotations.NotNull;

public class CloudBlobClient {
    private final BlobServiceClient blobServiceClient;

    private CloudBlobClient(@NotNull final BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }

    public static CloudBlobClient withBlobServiceClient(@NotNull final BlobServiceClient blobServiceClient) {
        return new CloudBlobClient(blobServiceClient);
    }

    public BlobServiceClient getBlobServiceClient() {
        return blobServiceClient;
    }

    public CloudBlobContainer getContainerReference(@NotNull final String containerName) {
        return CloudBlobContainer.withContainerClient(
                blobServiceClient.getContainerClient(containerName),
                containerName
        );
    }
}
