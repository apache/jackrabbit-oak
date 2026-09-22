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

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;

import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that a standalone {@code cleanup()} call - i.e. one with no preceding
 * {@code compactFull()}/{@code compactTail()} on the same {@code FileStore} instance,
 * exactly the sequence used by {@code StandbyClientSync} after a sync cycle, and by
 * {@code FileStoreBackupImpl}/{@code FileStoreBackupRestoreMBean} - still journals gc.log
 * whenever the persisted head is already a compacted generation.
 *
 * <p>Before the fix, {@code CompactionResult.skipped(...)}'s {@code requiresGCJournalEntry()}
 * was hard-coded to {@code false}, so gc.log was never updated for this call path, even
 * though the reclaimer actively removes segments belonging to older generations. This left
 * gc.log pointing at a stale root, which can later cause a {@code SegmentNotFoundException}.
 */
public class StandaloneCleanupGcLogTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /**
     * Simulates a cold standby: one {@code FileStore} instance compacts and persists a new,
     * compacted head (like a primary would), then a second, freshly opened {@code FileStore}
     * instance - which never compacted anything itself - calls the standalone
     * {@code cleanup()} (like {@code StandbyClientSync} does after a sync cycle). gc.log must
     * be updated to reflect the compacted head that this second instance found on disk.
     */
    @Test
    public void testStandaloneCleanupPersistsGcLogForCompactedHead() throws Exception {
        File storeDir = folder.getRoot();

        // Step 1: populate the store and compact it, persisting a compacted head to disk.
        // No cleanup() is called here, mirroring a primary that compacted while this
        // instance never observed the compaction locally.
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", "value");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();

            Assert.assertTrue("compactFull() should succeed", store.compactFull());
        }

        // Step 2: gc.log must still be empty - nothing has called cleanup() yet.
        GCJournal gcJournalBefore = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        Assert.assertEquals("gc.log should be empty before any cleanup() call",
                GCJournalEntry.EMPTY, gcJournalBefore.read());

        // Step 3: open a fresh FileStore instance - it never called compactFull()/compactTail()
        // itself - and call the standalone, no-arg cleanup(), exactly as StandbyClientSync does.
        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            store.cleanup();
        }

        // Step 4: gc.log must now record the compacted head found on disk.
        GCJournal gcJournalAfter = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        GCJournalEntry entry = gcJournalAfter.read();

        Assert.assertNotEquals(
                "gc.log must have a non-empty entry after a standalone cleanup() finds an "
                        + "already-compacted head",
                GCJournalEntry.EMPTY, entry);
        // Note: GCJournalEntry serialization always clears the compacted flag on the persisted
        // generation (see GcJournalTest#testGCGenerationCompactedFlagCleared) - the generation
        // number itself is what proves this entry reflects the compacted head. A single
        // compactFull() on a fresh store always produces generation=1, fullGeneration=1.
        Assert.assertEquals("gc.log entry must record generation 1",
                1, entry.getGcGeneration().getGeneration());
        Assert.assertEquals("gc.log entry must record full generation 1",
                1, entry.getGcGeneration().getFullGeneration());
    }

    /**
     * A standalone {@code cleanup()} on a store whose head was never compacted must not
     * write a gc.log entry - this preserves the original, correct behavior for fresh
     * repositories that have never gone through compaction.
     */
    @Test
    public void testStandaloneCleanupDoesNotPersistGcLogForUncompactedHead() throws Exception {
        File storeDir = folder.getRoot();

        try (FileStore store = fileStoreBuilder(storeDir)
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("key", "value");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            store.flush();

            store.cleanup();
        }

        GCJournal gcJournal = new GCJournal(new TarPersistence(storeDir).getGCJournalFile());
        Assert.assertEquals(
                "gc.log must stay empty when cleanup() runs on a never-compacted head",
                GCJournalEntry.EMPTY, gcJournal.read());
    }

}
