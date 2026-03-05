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

import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

/**
 * Tests that offline compaction (separate {@code compactFull()} + {@code cleanup()} calls,
 * as performed by oak-run compact command) correctly persists the compacted head to gc.log.
 *
 * <p>All tests in this class are true regression tests: they fail against the unfixed code
 * (because gc.log is never written when compactFull and cleanup are called separately)
 * and pass once the fix that retains the {@code CompactionResult} across the two calls
 * is applied.
 */
public class OfflineCompactionGcLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /**
     * Verifies that calling {@code compactFull()} followed by a separate {@code cleanup()}
     * — which is exactly the sequence used by the oak-run compact command — writes an entry
     * to gc.log with a non-null, non-empty root id and an incremented GC generation.
     *
     * <p>Prior to the fix, the {@code CompactionResult.succeeded(...)} produced by
     * {@code compactFull()} was discarded before {@code cleanup()} ran. The cleanup used a
     * synthetic {@code CompactionResult.skipped(...)} whose {@code requiresGCJournalEntry()}
     * returns {@code false}, so {@code GCJournal.persist()} was never called.
     */
    @Test
    public void testOfflineCompactionPersistsGcLog() throws Exception {
        File storeDir = folder.getRoot();

        // Step 1: populate the store with some content
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", "value");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();
        }

        // Step 2: verify gc.log is empty before offline compaction
        GCJournal gcJournalBefore = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        Assert.assertEquals("gc.log should be empty before offline compaction",
                GCJournalEntry.EMPTY, gcJournalBefore.read());

        // Step 3: perform offline compaction — compactFull() and cleanup() as separate calls
        // (this is the exact sequence used by Compact.run() via oak-run compact)
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            boolean compacted = store.compactFull();
            Assert.assertTrue("compactFull() should succeed", compacted);
            store.cleanup();
        }

        // Step 4: assert gc.log has an entry with the compacted head
        GCJournal gcJournalAfter = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        GCJournalEntry entry = gcJournalAfter.read();

        Assert.assertNotEquals("gc.log must have a non-empty entry after offline compaction",
                GCJournalEntry.EMPTY, entry);
        Assert.assertNotNull("gc.log entry root must not be null", entry.getRoot());
        Assert.assertFalse("gc.log entry root must not be empty", entry.getRoot().isEmpty());

        GCGeneration generation = entry.getGcGeneration();
        Assert.assertTrue("gc.log entry must record a full generation >= 1",
                generation.getFullGeneration() >= 1);
    }

    /**
     * Scenario: cleanup → compact(ok) → cleanup
     *
     * <p>A pre-compaction cleanup (with no preceding {@code compactFull()}) must not write a
     * gc.log entry. Only the cleanup that follows a successful compaction should write one.
     *
     * <p>Before the fix: gc.log is empty after the full sequence — neither cleanup writes
     * because both fall back to the synthetic {@code CompactionResult.skipped(...)} path.
     * After the fix: gc.log has a valid entry written by the second (post-compact) cleanup.
     */
    @Test
    public void testCleanupFirstCompactOkCleanupGcLogWritten() throws Exception {
        File storeDir = folder.getRoot();

        // Step 1: populate the store
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", "value");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();
        }

        // Step 2: run cleanup before any compaction, then compact + cleanup
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            store.cleanup();                        // no preceding compact — must NOT write gc.log
            boolean compacted = store.compactFull();
            Assert.assertTrue("compactFull() should succeed", compacted);
            store.cleanup();                        // succeeded result available — must write gc.log
        }

        // Step 3: assert gc.log has an entry (written by the second cleanup)
        GCJournal gcJournal = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        GCJournalEntry entry = gcJournal.read();

        Assert.assertNotEquals(
                "gc.log must have a non-empty entry after cleanup → compact → cleanup",
                GCJournalEntry.EMPTY, entry);
        Assert.assertTrue("gc.log entry must record a full generation >= 1",
                entry.getGcGeneration().getFullGeneration() >= 1);
    }

    /**
     * Scenario: compact(ok) → cleanup → cleanup
     *
     * <p>The first cleanup after a successful compaction must write a gc.log entry and consume
     * the stored compaction result. The second cleanup must not write a duplicate entry —
     * it has no stored result and falls back to the no-journal path.
     *
     * <p>Before the fix: gc.log has 0 entries — neither cleanup ever writes.
     * After the fix: gc.log has exactly 1 entry — only the first cleanup writes.
     */
    @Test
    public void testCompactOkCleanupCleanupGcLogHasExactlyOneEntry() throws Exception {
        File storeDir = folder.getRoot();

        // Step 1: populate the store
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", "value");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();
        }

        // Step 2: compact then call cleanup twice
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            boolean compacted = store.compactFull();
            Assert.assertTrue("compactFull() should succeed", compacted);
            store.cleanup();    // 2-arg path: writes gc.log, clears stored compaction result
            store.cleanup();    // 1-arg path: no stored result — must NOT write a second entry
        }

        // Step 3: assert gc.log has exactly one entry
        GCJournal gcJournal = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        Assert.assertEquals(
                "gc.log must have exactly one entry after compact → cleanup → cleanup",
                1, gcJournal.readAll().size());
    }

}
