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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.reverseOrder;
import static java.util.Collections.sort;

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
import org.apache.jackrabbit.oak.plugins.segment.file.JournalReader;

public final class FileStoreHelper {

    public static final String newline = "\n";

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

}
