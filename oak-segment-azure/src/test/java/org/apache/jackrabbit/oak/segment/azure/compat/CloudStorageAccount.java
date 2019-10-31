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

import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.credentials.SharedKeyCredential;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CloudStorageAccount {
    private final String storageUrl;
    private final String accountName;
    private final String accountKey;

    private CloudStorageAccount(@NotNull final String protocol,
                                @NotNull final String accountName,
                                @NotNull final String accountKey) {
        this(protocol, accountName, accountKey, null);
    }
    private CloudStorageAccount(@NotNull final String protocol,
                                @NotNull final String accountName,
                                @NotNull final String accountKey,
                                @Nullable final String blobEndpoint) {
        this.storageUrl = blobEndpoint != null ? blobEndpoint : String.format("%s://%s.blob.core.windows.net", protocol, accountName);
        this.accountName = accountName;
        this.accountKey = accountKey;
    }

    public static CloudStorageAccount parse(@NotNull final String oldStyleConnectionString) {
        String protocol = null;
        String accountName = null;
        String accountKey = null;
        String blobEndpoint = null;

        for (String keypairs : oldStyleConnectionString.split(";")) {
            String[] parts = keypairs.split("=");
            if (2 == parts.length) {
                String key = parts[0];
                String value = parts[1];
                switch (key) {
                    case "DefaultEndpointsProtocol":
                        protocol = value;
                        break;
                    case "AccountName":
                        accountName = value;
                        break;
                    case "AccountKey":
                        accountKey = value;
                        break;
                    case "BlobEndpoint":
                        blobEndpoint = value;
                        break;
                    default:
                        break;
                }
            }
        }

        if (null == protocol) {
            throw new IllegalArgumentException("Invalid connection string - missing element '" + protocol + "'");
        }
        else if (null == accountName) {
            throw new IllegalArgumentException("Invalid connection string - missing element '" + accountName + "'");
        }
        else if (null == accountKey) {
            throw new IllegalArgumentException("Invalid connection string - missing element '" + accountKey + "'");
        }

        return new CloudStorageAccount(protocol, accountName, accountKey, blobEndpoint);
    }

    public CloudBlobClient createCloudBlobClient() {
        SharedKeyCredential credential = new SharedKeyCredential(accountName, accountKey);
        return CloudBlobClient.withBlobServiceClient(
                new BlobServiceClientBuilder()
                        .endpoint(storageUrl)
                        .credential(credential)
                        .buildClient()
        );
    }
}
