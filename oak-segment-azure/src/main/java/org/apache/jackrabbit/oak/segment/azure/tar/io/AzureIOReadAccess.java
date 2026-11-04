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
package org.apache.jackrabbit.oak.segment.azure.tar.io;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.DownloadRetryOptions;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.tar.IOReadAccess;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AzureIOReadAccess implements IOReadAccess {

    private final String archiveName;

    protected final BlobClient readClient;

    public AzureIOReadAccess(String archiveName, BlobClient blobClient) {
        this.archiveName = archiveName;
        this.readClient = blobClient;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    public Buffer read(int position, int length) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(length);
        readClient.downloadStreamWithResponse(
                outputStream,
                new BlobRange(position, (long) length),
                new DownloadRetryOptions().setMaxRetryRequests(5),
                null,
                false,
                null,
                Context.NONE
        );
        return Buffer.wrap(outputStream.toByteArray());
    }

    @Override
    public int length() {
        return Math.toIntExact(readClient.getProperties().getBlobSize());
    }

    @Override
    public void close() throws IOException {
        // nothing to do
    }
}
