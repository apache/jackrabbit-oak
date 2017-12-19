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

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.file.Files.newBufferedWriter;
import static java.nio.file.Files.readAllLines;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists the repository size and the reclaimed size following a cleanup
 * operation in the {@link #GC_JOURNAL gc journal} file with the format:
 * 'repoSize, reclaimedSize, timestamp, gc generation, gc full generation (since Oak 1.8),
 * number of nodes compacted, root id (since Oak 1.8)'.
 */
public class GCJournal {

    private static final Logger LOG = LoggerFactory.getLogger(GCJournal.class);

    public static final String GC_JOURNAL = "gc.log";

    @Nonnull
    private final File directory;

    private GCJournalEntry latest;

    public GCJournal(@Nonnull File directory) {
        this.directory = checkNotNull(directory);
    }

    /**
     * Persists the repository stats (current size, reclaimed size, gc
     * generation, number of compacted nodes) following a cleanup operation for
     * a successful compaction. NOOP if the gcGeneration is the same as the one
     * persisted previously.
     *
     * @param reclaimedSize size reclaimed by cleanup
     * @param repoSize      current repo size
     * @param gcGeneration  gc generation
     * @param nodes         number of compacted nodes
     * @param root          record id of the compacted root node
     */
    public synchronized void persist(long reclaimedSize, long repoSize,
            @Nonnull GCGeneration gcGeneration, long nodes, @Nonnull String root
    ) {
        GCJournalEntry current = read();
        if (current.getGcGeneration().equals(gcGeneration)) {
            // failed compaction, only update the journal if the generation
            // increases
            return;
        }
        latest = new GCJournalEntry(repoSize, reclaimedSize,
                System.currentTimeMillis(), gcGeneration, nodes, checkNotNull(root));
        Path path = new File(directory, GC_JOURNAL).toPath();
        try {
            try (BufferedWriter w = newBufferedWriter(path, UTF_8, WRITE,
                    APPEND, CREATE, DSYNC)) {
                w.write(latest.toString());
                w.newLine();
            }
        } catch (IOException e) {
            LOG.error("Error writing gc journal", e);
        }
    }

    /**
     * Returns the latest entry available
     */
    public synchronized GCJournalEntry read() {
        if (latest == null) {
            List<String> all = readLines();
            if (all.isEmpty()) {
                latest = GCJournalEntry.EMPTY;
            } else {
                String info = all.get(all.size() - 1);
                latest = GCJournalEntry.fromString(info);
            }
        }
        return latest;
    }

    /**
     * Returns all available entries from the journal
     */
    public synchronized Collection<GCJournalEntry> readAll() {
        List<GCJournalEntry> all = new ArrayList<GCJournalEntry>();
        for (String l : readLines()) {
            all.add(GCJournalEntry.fromString(l));
        }
        return all;
    }

    private List<String> readLines() {
        File file = new File(directory, GC_JOURNAL);
        if (file.exists()) {
            try {
                return readAllLines(file.toPath(), UTF_8);
            } catch (IOException e) {
                LOG.error("Error reading gc journal", e);
            }
        }
        return new ArrayList<String>();
    }

    public static class GCJournalEntry {

        static final GCJournalEntry EMPTY = new GCJournalEntry(
                -1, -1, -1, GCGeneration.NULL, -1, RecordId.NULL.toString10());

        private final long repoSize;

        private final long reclaimedSize;

        private final long ts;

        @Nonnull
        private final GCGeneration gcGeneration;

        private final long nodes;

        @Nonnull
        private final String root;

        public GCJournalEntry(long repoSize, long reclaimedSize, long ts,
                @Nonnull GCGeneration gcGeneration, long nodes, @Nonnull String root
        ) {
            this.repoSize = repoSize;
            this.reclaimedSize = reclaimedSize;
            this.ts = ts;
            this.gcGeneration = gcGeneration;
            this.nodes = nodes;
            this.root = root;
        }

        @Override
        public String toString() {
            return Joiner.on(",").join(
                    repoSize,
                    reclaimedSize,
                    ts,
                    gcGeneration.getGeneration(),
                    gcGeneration.getFullGeneration(),
                    nodes,
                    root
            );
        }

        static GCJournalEntry fromString(String in) {
            String[] items = in.split(",");
            int index = 0;

            long repoSize = parseLong(items, index++);
            long reclaimedSize = parseLong(items, index++);
            long ts = parseLong(items, index++);
            int generation = parseInt(items, index++);
            int fullGeneration;
            if (items.length == 7) {
                // gc.log from Oak 1.8 onward
                fullGeneration = parseInt(items, index++);
            } else {
                // gc.log from Oak 1.6
                fullGeneration = generation;
            }
            long nodes = parseLong(items, index++);
            String root = parseString(items, index);
            if (root == null) {
                root = RecordId.NULL.toString10();
            }
            return new GCJournalEntry(repoSize, reclaimedSize, ts,
                    newGCGeneration(generation, fullGeneration, false), nodes, root);
        }

        @CheckForNull
        private static String parseString(String[] items, int index) {
            if (index >= items.length) {
                return null;
            }
            return items[index];
        }

        private static long parseLong(String[] items, int index) {
            String in = parseString(items, index);
            if (in != null) {
                try {
                    return Long.parseLong(in);
                } catch (NumberFormatException ex) {
                    LOG.warn("Unable to parse {} as long value.", in, ex);
                }
            }
            return -1;
        }

        private static int parseInt(String[] items, int index) {
            String in = parseString(items, index);
            if (in != null) {
                try {
                    return Integer.parseInt(in);
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse {} as an integer value.", in, e);
                }
            }
            return -1;
        }

        /**
         * Returns the repository size
         */
        public long getRepoSize() {
            return repoSize;
        }

        /**
         * Returns the reclaimed size
         */
        public long getReclaimedSize() {
            return reclaimedSize;
        }

        /**
         * Returns the timestamp
         */
        public long getTs() {
            return ts;
        }

        /**
         * Returns the gc generation
         */
        @Nonnull
        public GCGeneration getGcGeneration() {
            return gcGeneration;
        }

        /**
         * Returns the number of compacted nodes
         */
        public long getNodes() {
            return nodes;
        }

        /**
         * Returns the record id of the root created by the compactor
         */
        @Nonnull
        public String getRoot() {
            return root;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + gcGeneration.hashCode();
            result = prime * result + root.hashCode();
            result = prime * result + (int) (nodes ^ (nodes >>> 32));
            result = prime * result + (int) (reclaimedSize ^ (reclaimedSize >>> 32));
            result = prime * result + (int) (repoSize ^ (repoSize >>> 32));
            result = prime * result + (int) (ts ^ (ts >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            GCJournalEntry other = (GCJournalEntry) obj;
            if (!gcGeneration.equals(other.gcGeneration)) {
                return false;
            }
            if (nodes != other.nodes) {
                return false;
            }
            if (reclaimedSize != other.reclaimedSize) {
                return false;
            }
            if (repoSize != other.repoSize) {
                return false;
            }
            if (ts != other.ts) {
                return false;
            }
            if (!root.equals(other.root)) {
                return false;
            }
            return true;
        }

    }

}
