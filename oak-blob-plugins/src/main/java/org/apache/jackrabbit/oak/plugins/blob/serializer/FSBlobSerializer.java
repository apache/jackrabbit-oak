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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.json.BlobDeserializer;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FileUtils.ONE_KB;

/**
 * Serializer which stores blobs in a FileDataStore format
 */
public class FSBlobSerializer extends BlobSerializer implements BlobDeserializer, Closeable {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final String ERROR_MARKER = "*ERROR*-";
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
        try (InputStream is = blob.getNewStream()) {
            return dataStore.writeBlob(is);
        } catch (Exception e) {
            log.warn("Error occurred while serializing Blob with id {}", blob.getContentIdentity(), e);
            return String.format("%s%s", ERROR_MARKER, blob.getContentIdentity());
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
        if (errorBlob(value)){
            return new ErrorBlob(value);
        }
        return new BlobStoreBlob(dataStore, value);
    }

    private boolean errorBlob(String value) {
        return value.startsWith(ERROR_MARKER);
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

    private static final class ErrorBlob extends AbstractBlob {
        private final String id;

        public ErrorBlob(String id) {
            this.id = id.substring(ERROR_MARKER.length());
        }

        @Override
        public String getContentIdentity() {
            return id;
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            throw createError();
        }

        @Override
        public long length() {
            throw createError();
        }

        private RuntimeException createError() {
            return new RuntimeException("Blob with id ["+id+"] threw error while serializing");
        }
    }
}
