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
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.sort;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.LATEST_VERSION;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;
import org.apache.jackrabbit.oak.plugins.segment.file.tooling.BasicReadOnlyBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

public final class FileStoreHelper {

    public static final String newline = "\n";

    public static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    public static final int TAR_SEGMENT_CACHE_SIZE = Integer.getInteger("cache", 256);

    private FileStoreHelper() {
    }

    /**
     * Helper method to determine the segment version of the segment
     * containing the current root node state.
     * @param fileStore
     * @return
     */
    public static SegmentVersion getSegmentVersion(FileStore fileStore) {
        return fileStore.getHead().getRecordId().getSegment().getSegmentVersion();
    }

    public static List<String> getTarFiles(FileStore store) {
        List<String> files = newArrayList();
        for (String p : store.getTarReaderIndex().keySet()) {
            files.add(new File(p).getName());
        }
        sort(files, reverseOrder());
        return files;
    }

    public static void getGcRoots(FileStore store, UUID uuidIn,
            Map<UUID, Set<Entry<UUID, String>>> links) throws IOException {
        Deque<UUID> todos = new ArrayDeque<UUID>();
        todos.add(uuidIn);
        Set<UUID> visited = newHashSet();
        while (!todos.isEmpty()) {
            UUID uuid = todos.remove();
            if (!visited.add(uuid)) {
                continue;
            }
            for (String f : getTarFiles(store)) {
                Map<UUID, List<UUID>> graph = store.getTarGraph(f);
                for (Entry<UUID, List<UUID>> g : graph.entrySet()) {
                    if (g.getValue() != null && g.getValue().contains(uuid)) {
                        UUID uuidP = g.getKey();
                        if (!todos.contains(uuidP)) {
                            todos.add(uuidP);
                            Set<Entry<UUID, String>> deps = links.get(uuid);
                            if (deps == null) {
                                deps = newHashSet();
                                links.put(uuid, deps);
                            }
                            deps.add(new SimpleImmutableEntry<UUID, String>(
                                    uuidP, f));
                        }
                    }
                }
            }
        }
    }

    public static void printGcRoots(StringBuilder sb,
            Map<UUID, Set<Entry<UUID, String>>> links, UUID uuid, String space,
            String inc) {
        Set<Entry<UUID, String>> roots = links.remove(uuid);
        if (roots == null || roots.isEmpty()) {
            return;
        }
        // TODO is sorting by file name needed?
        for (Entry<UUID, String> r : roots) {
            sb.append(space + r.getKey() + "[" + r.getValue() + "]");
            sb.append(newline);
            printGcRoots(sb, links, r.getKey(), space + inc, inc);
        }
    }

    public static List<String> readRevisions(File store) {
        File journal = new File(store, "journal.log");
        if (!journal.exists()) {
            return newArrayList();
        }

        List<String> revs = newArrayList();
        JournalReader journalReader = null;
        try {
            journalReader = new JournalReader(journal);
            try {
                revs = newArrayList(journalReader.iterator());
            } finally {
                journalReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (journalReader != null) {
                    journalReader.close();
                }
            } catch (IOException e) {
            }
        }
        return revs;
    }

    public static File isValidFileStoreOrFail(File store) {
        checkArgument(isValidFileStore(store), "Invalid FileStore directory "
                + store);
        return store;
    }

    /**
     * Checks if the provided directory is a valid FileStore
     *
     * @return true if the provided directory is a valid FileStore
     */
    public static boolean isValidFileStore(File store) {
        if (!store.exists()) {
            return false;
        }
        if (!store.isDirectory()) {
            return false;
        }
        // for now the only check is the existence of the journal file
        for (String f : store.list()) {
            if ("journal.log".equals(f)) {
                return true;
            }
        }
        return false;
    }

    public static File checkFileStoreVersionOrFail(String path, boolean force) throws IOException, InvalidFileStoreVersionException {
        File directory = new File(path);
        if (!directory.exists()) {
            return directory;
        }
        FileStore store = openReadOnlyFileStore(directory);
        try {
            SegmentVersion segmentVersion = getSegmentVersion(store);
            if (segmentVersion != LATEST_VERSION) {
                if (force) {
                    System.out
                            .printf("Segment version mismatch. Found %s, expected %s. Forcing execution.\n",
                                    segmentVersion, LATEST_VERSION);
                } else {
                    throw new RuntimeException(
                            String.format(
                                    "Segment version mismatch. Found %s, expected %s. Aborting.",
                                    segmentVersion, LATEST_VERSION));
                }
            }
        } finally {
            store.close();
        }
        return directory;
    }

    public static FileStore openFileStore(String directory) throws IOException, InvalidFileStoreVersionException {
        return openFileStore(directory, false);
    }

    public static FileStore openFileStore(String directory, boolean force)
            throws IOException, InvalidFileStoreVersionException {
        return FileStore.builder(checkFileStoreVersionOrFail(directory, force))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED).build();
    }

    public static FileStore openFileStore(String directory, boolean force,
            BlobStore blobStore
    ) throws IOException, InvalidFileStoreVersionException {
        return FileStore.builder(checkFileStoreVersionOrFail(directory, force))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .withBlobStore(blobStore).build();
    }

    public static ReadOnlyStore openReadOnlyFileStore(File directory)
            throws IOException, InvalidFileStoreVersionException {
        return FileStore.builder(isValidFileStoreOrFail(directory))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .buildReadOnly();
    }

    public static ReadOnlyStore openReadOnlyFileStore(File directory,
            BlobStore blobStore
    ) throws IOException, InvalidFileStoreVersionException {
        return FileStore.builder(isValidFileStoreOrFail(directory))
                .withCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED)
                .withBlobStore(blobStore)
                .buildReadOnly();
    }

    public static BlobStore newBasicReadOnlyBlobStore() {
        return new BasicReadOnlyBlobStore();
    }

}
