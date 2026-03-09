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

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Verifies the {@code lastCompactionResult} lifecycle inside {@link GarbageCollector}
 * for the offline compaction path, covering all combinations of {@code compactFull()}
 * and {@code cleanup()} calls described in the scenario matrix:
 *
 * <pre>
 * compact(ok) → cleanup                    journal written
 * compact(ok) → compact(ok) → cleanup      gen2 supersedes gen1
 * compact(ok) → compact(abort) → cleanup   abort does not clobber gen1
 * compact(abort) → compact(ok) → cleanup   journal written for gen2
 * compact(abort) → cleanup                 journal NOT written
 * cleanup → compact(ok) → cleanup          journal written only for second cleanup
 * compact(ok) → cleanup → cleanup          second cleanup is a no-op
 * </pre>
 *
 * <p>The strategy is mocked so that specific {@link CompactionResult} instances can be
 * injected. {@link ArgumentCaptor} is then used to assert exactly which result was
 * forwarded to {@code strategy.cleanup()}, proving that the 2-arg
 * {@code cleanup(Context, CompactionResult)} path (journal entry required) vs the 1-arg
 * {@code cleanup(Context)} path (no journal entry) is chosen correctly.
 */
public class GarbageCollectorOfflineCompactionTest {

    private GarbageCollectionStrategy strategy;
    private GarbageCollector collector;

    @Before
    public void setUp() {
        strategy = Mockito.mock(GarbageCollectionStrategy.class);
        collector = new GarbageCollector(
            SegmentGCOptions.defaultGCOptions(),
            Mockito.mock(GCListener.class),
            null,                   // gcJournal   — not needed; strategy is mocked
            new AtomicBoolean(true),
            null,                   // fileReaper
            null,                   // tarFiles
            null,                   // tracker
            null,                   // segmentReader
            () -> null,             // revisionsSupplier
            null,                   // blobStore
            null,                   // segmentCache
            null,                   // segmentWriter
            null,                   // stats
            Canceller.newCanceller(),
            () -> {},               // flusher
            null                    // segmentWriterFactory
        );
    }

    private CompactionResult succeeded(int gen) {
        return CompactionResult.succeeded(
            SegmentGCOptions.GCType.FULL,
            GCGeneration.newGCGeneration(gen, gen, false),
            SegmentGCOptions.defaultGCOptions(),
            RecordId.NULL,
            gen
        );
    }

    private CompactionResult aborted(int gen) {
        return CompactionResult.aborted(GCGeneration.newGCGeneration(gen, gen, false), gen);
    }

    // -----------------------------------------------------------------------
    // Scenario: compactTail(ok) → cleanup
    //   The compactTail path stores lastCompactionResult identically to
    //   compactFull; the succeeded result must reach the 2-arg cleanup.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactTailOkCleanupJournalWritten() throws IOException {
        CompactionResult result = succeeded(1);
        Mockito.when(strategy.compactTail(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(result);

        collector.compactTail(strategy);
        collector.cleanup(strategy);

        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy).cleanup(
            Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame(result, captor.getValue());
        Assert.assertTrue(captor.getValue().requiresGCJournalEntry());
        Mockito.verify(strategy, Mockito.never())
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class));
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(ok) → cleanup
    //   The succeeded result must be passed to the 2-arg cleanup.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactOkCleanupJournalWritten() throws IOException {
        CompactionResult result = succeeded(1);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(result);

        collector.compactFull(strategy);
        collector.cleanup(strategy);

        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy).cleanup(
            Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame(result, captor.getValue());
        Assert.assertTrue(captor.getValue().requiresGCJournalEntry());
        Mockito.verify(strategy, Mockito.never())
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class));
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(ok) → compact(ok) → cleanup
    //   The second succeeded result supersedes the first.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactOkCompactOkCleanupGen2Supersedes() throws IOException {
        CompactionResult gen1 = succeeded(1);
        CompactionResult gen2 = succeeded(2);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(gen1)
               .thenReturn(gen2);

        collector.compactFull(strategy);
        collector.compactFull(strategy);
        collector.cleanup(strategy);

        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy).cleanup(
            Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame("gen2 must supersede gen1", gen2, captor.getValue());
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(ok) → compact(abort) → cleanup
    //   An aborted result must not overwrite the previous succeeded result.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactOkCompactAbortCleanupAbortDoesNotClobber() throws IOException {
        CompactionResult gen1 = succeeded(1);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(gen1)
               .thenReturn(aborted(1));

        collector.compactFull(strategy);
        collector.compactFull(strategy);
        collector.cleanup(strategy);

        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy).cleanup(
            Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame("abort must not clobber the gen1 succeeded result", gen1, captor.getValue());
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(abort) → compact(ok) → cleanup
    //   The initial abort must not prevent a subsequent succeeded result from
    //   being used in cleanup.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactAbortCompactOkCleanupJournalWritten() throws IOException {
        CompactionResult gen2 = succeeded(2);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(aborted(0))
               .thenReturn(gen2);

        collector.compactFull(strategy);
        collector.compactFull(strategy);
        collector.cleanup(strategy);

        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy).cleanup(
            Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame(gen2, captor.getValue());
        Assert.assertTrue(captor.getValue().requiresGCJournalEntry());
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(abort) → cleanup
    //   No succeeded result is available; cleanup must use the 1-arg (skipped)
    //   path — no journal entry.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactAbortCleanupNoJournalEntry() throws IOException {
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(aborted(0));

        collector.compactFull(strategy);
        collector.cleanup(strategy);

        Mockito.verify(strategy)
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class));
        Mockito.verify(strategy, Mockito.never())
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class),
                        Mockito.any(CompactionResult.class));
    }

    // -----------------------------------------------------------------------
    // Scenario: cleanup → compact(ok) → cleanup
    //   The first (pre-compaction) cleanup must not write a journal entry
    //   only the second cleanup (after a succeeded compaction) must.
    // -----------------------------------------------------------------------

    @Test
    public void testCleanupCompactOkCleanupJournalWrittenOnlyForSecond() throws IOException {
        CompactionResult result = succeeded(1);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(result);

        collector.cleanup(strategy);        // no preceding compact — 1-arg path
        collector.compactFull(strategy);
        collector.cleanup(strategy);        // succeeded result available — 2-arg path

        Mockito.verify(strategy, Mockito.times(1))
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class));
        ArgumentCaptor<CompactionResult> captor = ArgumentCaptor.forClass(CompactionResult.class);
        Mockito.verify(strategy, Mockito.times(1))
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class), captor.capture());
        Assert.assertSame(result, captor.getValue());
    }

    // -----------------------------------------------------------------------
    // Scenario: compact(ok) → cleanup → cleanup
    //   The first cleanup consumes lastCompactionResult; the second must fall
    //   back to the 1-arg (skipped) path — no duplicate journal entry.
    // -----------------------------------------------------------------------

    @Test
    public void testCompactOkCleanupCleanupSecondCleanupIsNoop() throws IOException {
        CompactionResult result = succeeded(1);
        Mockito.when(strategy.compactFull(Mockito.any(GarbageCollectionStrategy.Context.class)))
               .thenReturn(result);

        collector.compactFull(strategy);
        collector.cleanup(strategy);    // 2-arg path; clears lastCompactionResult
        collector.cleanup(strategy);    // 1-arg path; lastCompactionResult is null

        Mockito.verify(strategy, Mockito.times(1))
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class),
                        Mockito.any(CompactionResult.class));
        Mockito.verify(strategy, Mockito.times(1))
               .cleanup(Mockito.any(GarbageCollectionStrategy.Context.class));
    }

}
