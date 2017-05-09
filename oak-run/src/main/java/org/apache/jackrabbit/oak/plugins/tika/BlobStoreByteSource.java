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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nullable;

import com.google.common.io.ByteSource;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * Avoiding use of BlobByteSource to avoid concurrent access to NodeState
 */
class BlobStoreByteSource extends ByteSource {
    private final BlobStore blobStore;
    private final String blobId;
    private final Long size;

    BlobStoreByteSource(BlobStore blobStore, String blobId,@Nullable Long size) {
        this.blobStore = blobStore;
        this.blobId = blobId;
        this.size = size;
    }

    BlobStoreByteSource(BlobStore blobStore, String blobId) {
        this(blobStore, blobId, null);
    }

    @Override
    public InputStream openStream() throws IOException {
        return blobStore.getInputStream(blobId);
    }

    @Override
    public long size() throws IOException {
        if (size != null) {
            return size;
        }
        return blobStore.getBlobLength(blobId);
    }
}
