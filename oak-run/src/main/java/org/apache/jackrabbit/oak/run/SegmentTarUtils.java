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

package org.apache.jackrabbit.oak.run;

import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.isValidFileStoreOrFail;
import static org.apache.jackrabbit.oak.plugins.segment.FileStoreHelper.newBasicReadOnlyBlobStore;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.backup.FileStoreBackup;
import org.apache.jackrabbit.oak.backup.FileStoreRestore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

class SegmentTarUtils {

    private static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private static final int TAR_SEGMENT_CACHE_SIZE = Integer.getInteger("cache", 256);

    private SegmentTarUtils() {
        // Prevent instantiation
    }

    static void backup(File source, File target) throws IOException {
        Closer closer = Closer.create();
        try {
            FileStore fs;
            if (FileStoreBackup.USE_FAKE_BLOBSTORE) {
                fs = openReadOnlyFileStore(source, newBasicReadOnlyBlobStore());
            } else {
                fs = openReadOnlyFileStore(source);
            }
            closer.register(asCloseable(fs));
            NodeStore store = SegmentNodeStore.builder(fs).build();
            FileStoreBackup.backup(store, target);
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    static void restore(File source, File target) throws IOException {
        FileStoreRestore.restore(source, target);
    }

    private static FileStore openReadOnlyFileStore(File path, BlobStore blobStore) throws IOException {
        return FileStore.builder(isValidFileStoreOrFail(path))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .withBlobStore(blobStore)
                .buildReadOnly();
    }

    private static FileStore openReadOnlyFileStore(File path) throws IOException {
        return FileStore.builder(isValidFileStoreOrFail(path))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .buildReadOnly();
    }

    private static Closeable asCloseable(final FileStore fs) {
        return new Closeable() {

            @Override
            public void close() throws IOException {
                fs.close();
            }

        };
    }

}
