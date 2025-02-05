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
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.io.File;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultGarbageCollectionStrategyTest {
    private final GCJournal journal;
    private final GCJournalFile journalFile;

    public DefaultGarbageCollectionStrategyTest() throws IOException {
        journalFile = Mockito.spy(new LocalGCJournalFile(File.createTempFile("gctest", "log")));
        journal = new GCJournal(journalFile);
    }

    private GarbageCollectionStrategy.Context getMockedGCContext(MemoryStore store) throws IOException {
        GarbageCollectionStrategy.Context mockedContext = Mockito.mock(GarbageCollectionStrategy.Context.class);

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

    private void verifyGCJournalPersistence(VerificationMode mode) throws IOException {
        verify(journalFile, mode).writeLine(anyString());
    }

    @Test
    public void successfulCompactionPersistsToJournal() throws Exception {
        CompactionResult result = CompactionResult.succeeded(
                SegmentGCOptions.GCType.FULL,
                GCGeneration.NULL,
                SegmentGCOptions.defaultGCOptions(),
                new RecordId(SegmentId.NULL, 1),
                0
        );
        runCleanup(result);
        verifyGCJournalPersistence(times(1));
    }

    @Test
    public void partialCompactionPersistsToJournal() throws Exception {
        CompactionResult result = CompactionResult.partiallySucceeded(
                GCGeneration.NULL,
                new RecordId(SegmentId.NULL, 1),
                0
        );
        runCleanup(result);
        verifyGCJournalPersistence(times(1));
    }

    @Test
    public void skippedCompactionMayPersistToJournal() throws Exception {
        SegmentGCOptions.GCType gcType = SegmentGCOptions.GCType.FULL;
        GCGeneration gcGeneration = GCGeneration.NULL;
        SegmentGCOptions gcOptions = SegmentGCOptions.defaultGCOptions();
        RecordId rootOne = new RecordId(SegmentId.NULL, 1);
        RecordId rootTwo = new RecordId(SegmentId.NULL, 2);

        // persist when there is no previous entry
        runCleanup(CompactionResult.skipped(gcType, gcGeneration, gcOptions, rootOne, 0));
        verifyGCJournalPersistence(times(1));

        // don't persist when root is identical
        runCleanup(CompactionResult.skipped(gcType, gcGeneration, gcOptions, rootOne, 0));
        verifyGCJournalPersistence(times(1));

        // persist when there is a new root
        runCleanup(CompactionResult.skipped(gcType, gcGeneration, gcOptions, rootTwo, 0));
        verifyGCJournalPersistence(times(2));

        // don't persist when the root is null
        runCleanup(CompactionResult.skipped(gcType, gcGeneration, gcOptions, RecordId.NULL, 0));
        verifyGCJournalPersistence(times(2));
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
