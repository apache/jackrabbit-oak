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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.tar.CleanupContext;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

public class DefaultGarbageCollectionStrategyTest {

    private final GCJournal journal;

    public DefaultGarbageCollectionStrategyTest() {
        journal = Mockito.mock(GCJournal.class);
        when(journal.read()).thenReturn(Mockito.mock(GCJournal.GCJournalEntry.class));
    }

    private GarbageCollectionStrategy.Context getMockedGCContext(MemoryStore store)
        throws IOException {
        GarbageCollectionStrategy.Context mockedContext = Mockito.mock(
            GarbageCollectionStrategy.Context.class);

        when(mockedContext.getGCListener()).thenReturn(Mockito.mock(GCListener.class));
        when(mockedContext.getTarFiles()).thenReturn(Mockito.mock(TarFiles.class));
        when(mockedContext.getSegmentCache()).thenReturn(Mockito.mock(SegmentCache.class));
        when(mockedContext.getFileStoreStats()).thenReturn(Mockito.mock(FileStoreStats.class));

        SegmentTracker tracker = new SegmentTracker((msb, lsb) -> new SegmentId(store, msb, lsb));
        when(mockedContext.getSegmentTracker()).thenReturn(tracker);
        when(mockedContext.getCompactionMonitor()).thenReturn(GCNodeWriteMonitor.EMPTY);
        when(mockedContext.getRevisions()).thenReturn(store.getRevisions());
        when(mockedContext.getGCJournal()).thenReturn(journal);

        TarFiles mockedTarFiles = Mockito.mock(TarFiles.class);
        when(mockedContext.getTarFiles()).thenReturn(mockedTarFiles);
        when(mockedTarFiles.cleanup(any(CleanupContext.class)))
            .thenReturn(Mockito.mock(TarFiles.CleanupResult.class));

        return mockedContext;
    }

    private void runCleanup(CompactionResult result) throws IOException {
        MemoryStore store = new MemoryStore();
        DefaultGarbageCollectionStrategy strategy = new DefaultGarbageCollectionStrategy();
        strategy.cleanup(getMockedGCContext(store), result);
    }

    private void verifyGCJournalPersistence(VerificationMode mode) {
        verify(journal, mode).persist(
            anyLong(),
            anyLong(),
            any(GCGeneration.class),
            anyLong(),
            anyString());
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
        verifyGCJournalPersistence(times(1));
    }

    @Test
    public void partialCompactionDoesNotPersistToJournal() throws Exception {
        CompactionResult result = CompactionResult.partiallySucceeded(GCGeneration.NULL,
            RecordId.NULL, 0);
        runCleanup(result);
        verifyGCJournalPersistence(never());
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
        verifyGCJournalPersistence(never());
    }

    @Test
    public void nonApplicableCompactionDoesNotPersistToJournal() throws Exception {
        runCleanup(CompactionResult.notApplicable(0));
        verifyGCJournalPersistence(never());
    }

    @Test
    public void abortedCompactionDoesNotPersistToJournal() throws Exception {
        runCleanup(CompactionResult.aborted(GCGeneration.NULL, 0));
        verifyGCJournalPersistence(never());
    }
}
