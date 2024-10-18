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
import static org.apache.jackrabbit.oak.segment.tool.Utils.parseSegmentInfoTimestamp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tooling.ConsistencyChecker;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class RecoverJournal {

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private File path;

        private PrintStream out = System.out;

        private PrintStream err = System.err;

        private Builder() {
            // Prevent external instantiation.
        }

        public Builder withPath(File path) {
            this.path = requireNonNull(path, "path");
            return this;
        }

        public Builder withOut(PrintStream out) {
            this.out = requireNonNull(out, "out");
            return this;
        }

        public Builder withErr(PrintStream err) {
            this.err = requireNonNull(err, "err");
            return this;
        }

        public RecoverJournal build() {
            Validate.checkState(path != null, "path not specified");
            return new RecoverJournal(this);
        }

    }

    private final File path;

    private final PrintStream out;

    private final PrintStream err;

    private final Set<String> notFoundSegments = new HashSet<>();

    private RecoverJournal(Builder builder) {
        this.path = builder.path;
        this.out = builder.out;
        this.err = builder.err;
    }

    public int run() {
        List<Entry> entries;

        try (ReadOnlyFileStore store = openReadOnlyFileStore(path)) {
            entries = recoverEntries(store);
        } catch (Exception e) {
            out.println("Unable to recover the journal entries, aborting");
            e.printStackTrace(err);
            return 1;
        }

        if (entries.size() == 0) {
            out.println("No valid journal entries found, aborting");
            return 1;
        }

        File journalBackup = journalBackupName();

        if (journalBackup == null) {
            err.println("Too many journal backups, please cleanup");
            return 1;
        }

        File journal = new File(path, "journal.log");

        try {
            Files.move(journal.toPath(), journalBackup.toPath());
        } catch (IOException e) {
            err.println("Unable to backup old journal, aborting");
            e.printStackTrace(err);
            return 1;
        }

        out.printf("Old journal backed up at %s\n", journalBackup.getName());

        boolean rollback;

        try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(journal)))) {
            for (Entry e : entries) {
                w.printf("%s root %d\n", e.recordId.toString10(), e.timestamp);
            }
            rollback = false;
        } catch (IOException e) {
            err.println("Unable to write the recovered journal, rolling back");
            e.printStackTrace(err);
            rollback = true;
        }

        if (rollback) {
            try {
                Files.deleteIfExists(journal.toPath());
            } catch (IOException e) {
                err.println("Unable to delete the recovered journal, aborting");
                e.printStackTrace(err);
                return 1;
            }

            try {
                Files.move(journalBackup.toPath(), journal.toPath());
            } catch (IOException e) {
                err.println("Unable to roll back the old journal, aborting");
                e.printStackTrace(err);
                return 1;
            }

            out.println("Old journal rolled back");

            return 1;
        }

        out.println("Journal recovered");

        return 0;
    }

    private File journalBackupName() {
        for (int attempt = 0; attempt < 1000; attempt++) {
            File backup = new File(path, String.format("journal.log.bak.%03d", attempt));
            if (backup.exists()) {
                continue;
            }
            return backup;
        }
        return null;
    }

    private static class Entry {

        long timestamp;

        RecordId recordId;

        Entry(long timestamp, RecordId recordId) {
            this.timestamp = timestamp;
            this.recordId = recordId;
        }

    }

    private List<Entry> recoverEntries(ReadOnlyFileStore fileStore) {
        List<Entry> entries = new ArrayList<>();

        for (SegmentId segmentId : fileStore.getSegmentIds()) {
            try {
                recoverEntries(fileStore, segmentId, entries);
            } catch (SegmentNotFoundException e) {
                handle(e);
            }
        }

        entries.sort((left, right) -> {
            // Two entries with different timestamp will be sorted in ascending
            // order of timestamp.

            int timestampComparison = Long.compare(left.timestamp, right.timestamp);
            if (timestampComparison != 0) {
                return timestampComparison;
            }

            // Comparing segment IDs with the same timestamp is totally
            // arbitrary. The relative order of two segments with the same
            // timestamp is unimportant.

            SegmentId leftSegmentId = left.recordId.getSegmentId();
            SegmentId rightSegmentId = right.recordId.getSegmentId();
            int segmentIdComparison = leftSegmentId.compareTo(rightSegmentId);
            if (segmentIdComparison != 0) {
                return segmentIdComparison;
            }

            // Records from the same segments are sorted in increasing order
            // of their record number. This builds on the assumption that a
            // record with a higher record number was added after a record
            // with a lower one, and therefor is more recent.

            int leftRecordNumber = left.recordId.getRecordNumber();
            int rightRecordNumber = right.recordId.getRecordNumber();
            return Integer.compare(leftRecordNumber, rightRecordNumber);
        });

        // Filter out the most recent entries that are not valid for
        // consistency. Make sure that the most recent entry is always
        // consistent.

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        ConsistencyChecker checker = new ConsistencyChecker();
        Set<String> corruptedPaths = new HashSet<>();

        ListIterator<Entry> i = entries.listIterator(entries.size());

        nextRevision:
        while (i.hasPrevious()) {
            Entry entry = i.previous();

            fileStore.setRevision(entry.recordId.toString());

            // If the head has a corrupted path, remove this revision and check
            // the previous one. We don't even bother to check the checkpoints.

            String badHeadPath = checker.checkTreeConsistency(nodeStore.getRoot(), corruptedPaths, true);

            if (badHeadPath != null) {
                out.printf("Skipping revision %s, corrupted path in head: %s\n", entry.recordId, badHeadPath);
                corruptedPaths.add(badHeadPath);
                i.remove();
                continue;
            }

            // If one of the checkpoints is unreachable or has a corrupted path,
            // we remove this revision and check the previous one. We stop
            // checking checkpoints as soon as we find an inconsistent one.

            for (String checkpoint : nodeStore.checkpoints()) {
                NodeState root = nodeStore.retrieve(checkpoint);

                if (root == null) {
                    out.printf("Skipping revision %s, found unreachable checkpoint %s\n", entry.recordId, checkpoint);
                    i.remove();
                    continue nextRevision;
                }

                String badCheckpointPath = checker.checkTreeConsistency(root, corruptedPaths, true);

                if (badCheckpointPath != null) {
                    out.printf("Skipping revision %s, corrupted path in checkpoint %s: %s\n", entry.recordId, checkpoint, badCheckpointPath);
                    corruptedPaths.add(badCheckpointPath);
                    i.remove();
                    continue nextRevision;
                }
            }

            // We didn't find any corruption in the head or in the checkpoints,
            // so we are at the most recent uncorrupted revision. We can skip
            // checking the other revisions because the list of entries now ends
            // with a usable revision.

            break;
        }

        return entries;
    }

    private void recoverEntries(ReadOnlyFileStore fileStore, SegmentId segmentId, List<Entry> entries) {
        if (segmentId.isBulkSegmentId()) {
            return;
        }

        Long timestamp = parseSegmentInfoTimestamp(segmentId);

        if (timestamp == null) {
            err.printf("No timestamp found in segment %s\n", segmentId);
            return;
        }

        segmentId.getSegment().forEachRecord((number, type, offset) -> {
            if (type != RecordType.NODE) {
                return;
            }
            try {
                recoverEntries(fileStore, timestamp, new RecordId(segmentId, number), entries);
            } catch (SegmentNotFoundException e) {
                handle(e);
            }
        });
    }

    private void recoverEntries(ReadOnlyFileStore fileStore, long timestamp, RecordId recordId, List<Entry> entries) {
        SegmentNodeState nodeState = fileStore.getReader().readNode(recordId);

        if (nodeState.hasChildNode("checkpoints") && nodeState.hasChildNode("root")) {
            entries.add(new Entry(timestamp, recordId));
        }
    }

    private void handle(SegmentNotFoundException e) {
        if (notFoundSegments.add(e.getSegmentId())) {
            e.printStackTrace(err);
        }
    }

}
