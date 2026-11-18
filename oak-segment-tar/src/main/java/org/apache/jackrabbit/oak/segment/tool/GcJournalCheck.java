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

package org.apache.jackrabbit.oak.segment.tool;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.tool.Utils.openReadOnlyFileStore;

import java.io.File;
import java.io.PrintWriter;
import java.text.MessageFormat;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.GCJournal;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;

/**
 * Read-only sanity check for {@code gc.log}. Verifies that its latest entry is
 * well-formed, points at a readable root, and matches the current head, so that
 * garbage collection relies on a valid and up-to-date GC journal.
 * <p>
 * Only local segment stores are supported. The head is compared by generation
 * numbers only, so a partially compacted head, which keeps the generation of
 * the last complete compaction, is not reported.
 */
public class GcJournalCheck {

    static final int CONSISTENT = 0;
    static final int INCONSISTENT = 1;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private File path;

        private PrintWriter out = new PrintWriter(System.out, true);

        private PrintWriter err = new PrintWriter(System.err, true);

        private Builder() {
            // Prevent external instantiation.
        }

        public Builder withPath(File path) {
            this.path = requireNonNull(path, "path");
            return this;
        }

        public Builder withOutWriter(PrintWriter out) {
            this.out = requireNonNull(out, "out");
            return this;
        }

        public Builder withErrWriter(PrintWriter err) {
            this.err = requireNonNull(err, "err");
            return this;
        }

        public GcJournalCheck build() {
            requireNonNull(path, "path");
            return new GcJournalCheck(this);
        }

    }

    private final File path;

    private final PrintWriter out;

    private final PrintWriter err;

    private GcJournalCheck(Builder builder) {
        this.path = builder.path;
        this.out = builder.out;
        this.err = builder.err;
    }

    public int run() {
        print("Checking gc.log at {0}", path);

        try (ReadOnlyFileStore store = openReadOnlyFileStore(path)) {
            GCJournal gcJournal = new GCJournal(new TarPersistence(path).getGCJournalFile());
            GCGeneration head = store.getRevisions().getHead().getSegmentId().getGcGeneration();

            if (gcJournal.readAll().isEmpty()) {
                // Any generation above zero was produced by a compaction, which should have been journaled.
                if (head.getGeneration() == 0 && head.getFullGeneration() == 0) {
                    print("No gc.log entries found and the store was never compacted, nothing to check");
                    return CONSISTENT;
                }
                printError("gc.log has no entries but the store head is at generation {0}, full generation {1}",
                    head.getGeneration(), head.getFullGeneration());
                return INCONSISTENT;
            }

            GCJournalEntry latest = gcJournal.read();
            GCGeneration journal = latest.getGcGeneration();
            print("Latest gc.log entry: generation {0}, full generation {1}, root {2}",
                journal.getGeneration(), journal.getFullGeneration(), latest.getRoot());

            // 1. The latest entry must be parseable.
            if (journal.getGeneration() < 0 || journal.getFullGeneration() < 0) {
                printError("Latest gc.log entry is malformed");
                return INCONSISTENT;
            }

            // 2. The recorded root must still be readable.
            String unreadable = rootReadError(store, latest.getRoot());
            if (unreadable != null) {
                printError("gc.log root {0} is not accessible: {1}", latest.getRoot(), unreadable);
                return INCONSISTENT;
            }
            print("gc.log root is accessible");

            // 3. The latest entry must match the current head, else it is stale.
            print("Store head: generation {0}, full generation {1}",
                head.getGeneration(), head.getFullGeneration());

            if (journal.getGeneration() != head.getGeneration()
                || journal.getFullGeneration() != head.getFullGeneration()) {
                printError("gc.log is stale: latest entry does not match the store head");
                return INCONSISTENT;
            }

            print("gc.log is consistent with the store head");
            return CONSISTENT;
        } catch (Exception e) {
            printError("Unable to check gc.log: {0}", e.getMessage());
            e.printStackTrace(err);
            return INCONSISTENT;
        }
    }

    private static String rootReadError(ReadOnlyFileStore store, String root) {
        try {
            RecordId rootId = RecordId.fromString(store.getSegmentIdProvider(), root);
            // Force a read of the root node to surface a missing segment.
            store.getReader().readNode(rootId).getPropertyCount();
            return null;
        } catch (IllegalArgumentException e) {
            return "invalid record id";
        } catch (SegmentNotFoundException e) {
            return "missing segment " + e.getSegmentId();
        }
    }

    private void print(String format, Object... arguments) {
        out.println(MessageFormat.format(format, arguments));
    }

    private void printError(String format, Object... arguments) {
        err.println(MessageFormat.format(format, arguments));
    }

}
