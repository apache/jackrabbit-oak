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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists the repository size and the reclaimed size following a cleanup operation in the
 * {@link #GC_JOURNAL gc journal} file with the format: 'repoSize, reclaimedSize, timestamp'.
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

    public synchronized void persist(long reclaimedSize, long repoSize) {
        latest = new GCJournalEntry(repoSize, reclaimedSize, System.currentTimeMillis());
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

    static class GCJournalEntry {

        static GCJournalEntry EMPTY = new GCJournalEntry(-1, -1, -1);

        private final long repoSize;
        private final long reclaimedSize;
        private final long ts;

        public GCJournalEntry(long repoSize, long reclaimedSize, long ts) {
            this.repoSize = repoSize;
            this.reclaimedSize = reclaimedSize;
            this.ts = ts;
        }

        @Override
        public String toString() {
            return  repoSize + "," + reclaimedSize + "," + ts;
        }

        static GCJournalEntry fromString(String in) {
            String[] items = in.split(",");
            if (items.length == 3) {
                long repoSize = safeParse(items[0]);
                long reclaimedSize = safeParse(items[1]);
                long ts = safeParse(items[2]);
                return new GCJournalEntry(repoSize, reclaimedSize, ts);
            }
            return GCJournalEntry.EMPTY;
        }

        private static long safeParse(String in) {
            try {
                return Long.parseLong(in);
            } catch (NumberFormatException ex) {
                LOG.warn("Unable to parse {} as long value.", in, ex);
            }
            return -1;
        }

        public long getRepoSize() {
            return repoSize;
        }

        public long getReclaimedSize() {
            return reclaimedSize;
        }

        public long getTs() {
            return ts;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (reclaimedSize ^ (reclaimedSize >>> 32));
            result = prime * result + (int) (repoSize ^ (repoSize >>> 32));
            result = prime * result + (int) (ts ^ (ts >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            GCJournalEntry other = (GCJournalEntry) obj;
            if (reclaimedSize != other.reclaimedSize)
                return false;
            if (repoSize != other.repoSize)
                return false;
            if (ts != other.ts)
                return false;
            return true;
        }
    }
}
