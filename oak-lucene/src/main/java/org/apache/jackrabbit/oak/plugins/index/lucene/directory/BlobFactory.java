/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.jackrabbit.oak.spi.blob.BlobOptions.UploadType.SYNCHRONOUS;

public interface BlobFactory {
    Boolean ENABLE_ASYNC_DS = Boolean.getBoolean("oak.lucene.ds.async");

    Blob createBlob(InputStream in) throws IOException;

    static BlobFactory getNodeBuilderBlobFactory(final NodeBuilder builder) {
        return builder::createBlob;
    }

    static BlobFactory getBlobStoreBlobFactory(final BlobStore store) {
        return in -> {
            String blobId;
            if (!ENABLE_ASYNC_DS) {
                blobId = store.writeBlob(in, new BlobOptions().setUpload(SYNCHRONOUS));
            } else {
                blobId = store.writeBlob(in);
            }
            return new BlobStoreBlob(store, blobId);
        };
    }
}
