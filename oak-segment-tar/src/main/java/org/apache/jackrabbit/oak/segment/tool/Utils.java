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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tooling.BasicReadOnlyBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

final class Utils {

    private static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private static final int TAR_SEGMENT_CACHE_SIZE = Integer.getInteger("cache", 256);

    private Utils() {}

    static ReadOnlyFileStore openReadOnlyFileStore(File path, BlobStore blobStore) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(isValidFileStoreOrFail(path))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .withBlobStore(blobStore)
                .buildReadOnly();
    }

    static ReadOnlyFileStore openReadOnlyFileStore(File path) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(isValidFileStoreOrFail(path))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .buildReadOnly();
    }

    static BlobStore newBasicReadOnlyBlobStore() {
        return new BasicReadOnlyBlobStore();
    }

    static List<String> readRevisions(File store) {
        File journal = new File(store, "journal.log");

        if (journal.exists()) {
            try (JournalReader journalReader = new JournalReader(journal)) {
                Iterator<String> revisionIterator = Iterators.transform(journalReader, new Function<JournalEntry, String>() {
                    @Nonnull
                    @Override
                    public String apply(JournalEntry entry) {
                        return entry.getRevision();
                    }
                });
                return newArrayList(revisionIterator);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return newArrayList();
    }

    private static File isValidFileStoreOrFail(File store) {
        checkArgument(isValidFileStore(store), "Invalid FileStore directory " + store);
        return store;
    }

    private static boolean isValidFileStore(File store) {
        if (!store.exists()) {
            return false;
        }

        if (!store.isDirectory()) {
            return false;
        }

        String[] fileNames = store.list();
        if (fileNames == null) {
            return false;
        }

        for (String f : fileNames) {
            if ("journal.log".equals(f)) {
                return true;
            }
        }

        return false;
    }

}
