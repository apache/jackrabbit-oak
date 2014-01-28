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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.BlobStoreInputStream;
import org.apache.jackrabbit.oak.api.Blob;

/**
 * A blob implementation.
 */
public class MongoBlob implements Blob {
    
    private final BlobStore blobStore;
    private final String id;
    
    public MongoBlob(BlobStore blobStore, String id) {
        this.blobStore = blobStore;
        this.id = id;
    }

    @Override
    @Nonnull
    public InputStream getNewStream() {
        return new BlobStoreInputStream(blobStore, id, 0);
    }

    @Override
    public long length() {
        try {
            return blobStore.getBlobLength(id);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid blob id: " + id);
        }
    }

    @Override @CheckForNull
    public String getReference() {
        return null;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return id;
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } 
        if (other instanceof MongoBlob) {
            MongoBlob b = (MongoBlob) other;
            // theoretically, the data could be the same  
            // even if the id is different
            return b.id.equals(id);
        }
        return false;
    }

}
