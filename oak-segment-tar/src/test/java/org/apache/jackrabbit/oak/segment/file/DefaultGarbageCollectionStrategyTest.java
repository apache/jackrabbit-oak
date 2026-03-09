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
 *
 */
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.io.IOException;

public class DefaultGarbageCollectionStrategyTest {
    private final GCJournal journal;

    public DefaultGarbageCollectionStrategyTest() {
        journal = Mockito.mock(GCJournal.class);
        Mockito.when(journal.read()).thenReturn(Mockito.mock(GCJournal.GCJournalEntry.class));
    }

    private GarbageCollectionStrategy.Context getMockedGCContext(MemoryStore store) throws IOException {
        GarbageCollectionStrategy.Context mockedContext = Mockito.mock(GarbageCollectionStrategy.Context.class);

        Mockito.when(mockedContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        Mockito.when(mockedContext.getTarFiles()).thenReturn(Mockito.mock(TarFiles.class));
        Mockito.when(mockedContext.getSegmentCache()).thenReturn(Mockito.mock(SegmentCache.class));
        Mockito.when(mockedContext.getFileStoreStats()).thenReturn(Mockito.mock(FileStoreStats.class));

        SegmentTracker tracker = new SegmentTracker((msb, lsb) -> new SegmentId(store, msb, lsb));
        Mockito.when(mockedContext.getSegmentTracker()).thenReturn(tracker);
        Mockito.when(mockedContext.getCompactionMonitor()).thenReturn(GCNodeWriteMonitor.EMPTY);
        Mockito.when(mockedContext.getRevisions()).thenReturn(store.getRevisions());
        Mockito.when(mockedContext.getGCJournal()).thenReturn(journal);

        TarFiles mockedTarFiles = Mockito.mock(TarFiles.class);
        Mockito.when(mockedContext.getTarFiles()).thenReturn(mockedTarFiles);
        Mockito.when(mockedTarFiles.cleanup(Mockito.any(CleanupContext.class)))
                .thenReturn(Mockito.mock(TarFiles.CleanupResult.class));

        return mockedContext;
    }

    private void runCleanup(CompactionResult result) throws IOException {
        MemoryStore store = new MemoryStore();
        DefaultGarbageCollectionStrategy strategy = new DefaultGarbageCollectionStrategy();
        strategy.cleanup(getMockedGCContext(store), result);
    }

    private void verifyGCJournalPersistence(VerificationMode mode) {
        Mockito.verify(journal, mode).persist(
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.any(GCGeneration.class),
                Mockito.anyLong(),
                Mockito.anyString());
    }

    @Test
    public void successfulCompactionPersistsToJournal() throws Exception {
        CompactionResult result = CompactionResult.succeeded(
                SegmentGCOptions.GCType.FULL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                RecordId.NULL,
                0);
        runCleanup(result);
        verifyGCJournalPersistence(Mockito.times(1));
    }

    @Test
    public void partialCompactionDoesNotPersistToJournal() throws Exception {
        CompactionResult result = CompactionResult.partiallySucceeded(GCGeneration.NULL, RecordId.NULL, 0);
        runCleanup(result);
        verifyGCJournalPersistence(Mockito.never());
    }

    @Test
    public void skippedCompactionDoesNotPersistToJournal() throws Exception {
        CompactionResult result = CompactionResult.skipped(
                SegmentGCOptions.GCType.FULL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                RecordId.NULL,
                0);
        runCleanup(result);
        verifyGCJournalPersistence(Mockito.never());
    }

    @Test
    public void nonApplicableCompactionDoesNotPersistToJournal() throws Exception {
        runCleanup(CompactionResult.notApplicable(0));
        verifyGCJournalPersistence(Mockito.never());
    }

    @Test
    public void abortedCompactionDoesNotPersistToJournal() throws Exception {
        runCleanup(CompactionResult.aborted(GCGeneration.NULL, 0));
        verifyGCJournalPersistence(Mockito.never());
    }

    @Test
    public void offlineCompactionAfterSuccessfulTailCompactionPersistsToJournal() throws Exception {
        CompactionResult result = CompactionResult.succeeded(
                SegmentGCOptions.GCType.TAIL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                RecordId.NULL,
                0);
        runCleanup(result);
        verifyGCJournalPersistence(Mockito.times(1));
    }

    @Test
    public void abortedRetryDoesNotOverwritePriorSucceededResultForJournalPersistence() throws Exception {
        // Simulates: compactFull -> aborted, compactFull -> succeeded, cleanup.
        // GarbageCollector stores only the succeeded result (isSuccess() gate),
        // so cleanup is ultimately called with the succeeded result.
        runCleanup(CompactionResult.aborted(GCGeneration.NULL, 0));
        runCleanup(CompactionResult.succeeded(
                SegmentGCOptions.GCType.FULL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                RecordId.NULL,
                0));
        verifyGCJournalPersistence(Mockito.times(1));
    }
}
