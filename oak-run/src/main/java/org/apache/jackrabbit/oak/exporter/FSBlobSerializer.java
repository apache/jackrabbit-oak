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

package org.apache.jackrabbit.oak.exporter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.json.BlobDeserializer;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;

import static org.apache.commons.io.FileUtils.ONE_KB;

/**
 * Serializer which stores blobs in a FileDataStore format
 */
public class FSBlobSerializer extends BlobSerializer implements BlobDeserializer, Closeable {
    private final File dir;
    private final int maxInlineSize;
    private final DataStoreBlobStore dataStore;

    public FSBlobSerializer(File dir) {
        this(dir, (int) ONE_KB * 4);
    }

    public FSBlobSerializer(File dir, int maxInlineSize) {
        this.dir = dir;
        this.maxInlineSize = maxInlineSize;
        this.dataStore = createDataStore();
    }

    @Override
    public String serialize(Blob blob) {
        try {
            try (InputStream is = blob.getNewStream()) {
                return dataStore.writeBlob(is);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error occurred while serializing Blob " +
                    "with id " + blob.getContentIdentity(), e);
        }
    }

    private DataStoreBlobStore createDataStore() {
        FileDataStore fds = new OakFileDataStore();
        fds.setPath(dir.getAbsolutePath());
        fds.setMinRecordLength(maxInlineSize);
        fds.init(null);
        return new DataStoreBlobStore(fds);
    }

    @Override
    public Blob deserialize(String value) {
        return new BlobStoreBlob(dataStore, value);
    }

    @Override
    public void close() throws IOException {
        if (dataStore != null) {
            try {
                dataStore.close();
            } catch (DataStoreException e) {
                throw new IOException(e);
            }
        }
    }
}
