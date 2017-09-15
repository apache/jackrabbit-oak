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

package org.apache.jackrabbit.oak.plugins.blob.serializer;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.BlobDeserializer;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

/**
 * Blob serializer which serializes blobs depending on type
 *
 * In memory blobs (having contentIdentity as null) would be serialized as base64
 * encoded string. For normal blobs there contentIdentity would be the serialized
 * value
 */
public class BlobIdSerializer extends BlobSerializer implements BlobDeserializer {
    private static final String PREFIX = "0y";
    private final Base64BlobSerializer base64 = new Base64BlobSerializer();
    private final BlobStore blobStore;

    public BlobIdSerializer(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public String serialize(Blob blob) {
        String blobId = blob.getContentIdentity();
        if (blobId != null) {
            return blobId;
        }
        return encode(blob);
    }

    @Override
    public Blob deserialize(String value) {
        if (value.startsWith(PREFIX)) {
            return base64.deserialize(value.substring(PREFIX.length()));
        }
        return new BlobStoreBlob(blobStore, value);
    }

    private String encode(Blob blob) {
        return PREFIX + base64.serialize(blob);
    }
}
