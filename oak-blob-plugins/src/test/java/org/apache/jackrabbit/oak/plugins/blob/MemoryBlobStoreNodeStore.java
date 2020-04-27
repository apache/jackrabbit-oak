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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;

/**
 * MemoryNodeStore extension which created blobs in the in-memory blob store
 */
public class MemoryBlobStoreNodeStore extends MemoryNodeStore {
    private final BlobStore blobStore;
    private final boolean fakePath;
    Set<String> referencedBlobs;

    public MemoryBlobStoreNodeStore(BlobStore blobStore) {
        this(blobStore, false);
    }

    public MemoryBlobStoreNodeStore(BlobStore blobStore, boolean fakePath) {
        this.blobStore = blobStore;
        this.fakePath = fakePath;
    }

    public void setReferencedBlobs(Set<String> referencedBlobs) {
        this.referencedBlobs = referencedBlobs;
    }

    public Set<String> getReferencedBlobs() {
        return this.referencedBlobs;
    }

    @Override
    public ArrayBasedBlob createBlob(InputStream in) {
        try {
            String id = blobStore.writeBlob(in);
            return new TestBlob(id, blobStore);
        } catch(Exception e) {
            BlobGCTest.log.error("Error in createBlobs", e);
        }
        return null;
    }

    public BlobReferenceRetriever getBlobReferenceRetriever() {
        return collector -> {
            for (String id : referencedBlobs) {
                collector.addReference(id, (fakePath ? UUID.randomUUID().toString() : null));
            }
        };
    }

    static class TestBlob extends ArrayBasedBlob {
        private String id;
        private BlobStore blobStore;

        public TestBlob(String id, BlobStore blobStore) {
            super(new byte[0]);
            this.id = id;
            this.blobStore = blobStore;
        }

        @Override
        public String getContentIdentity() {
            return id;
        }
        @NotNull
        @Override
        public InputStream getNewStream() {
            try {
                return blobStore.getInputStream(id);
            } catch (IOException e) {
                BlobGCTest.log.error("Error in getNewStream", e);
            }
            return null;
        }

        @Override
        public long length() {
            try {
                return blobStore.getBlobLength(id);
            } catch (IOException e) {
                BlobGCTest.log.error("Error in length", e);
            }
            return 0;
        }
    }
}
