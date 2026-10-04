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
 */
package org.apache.jackrabbit.oak.segment.consensus.aeron;

import org.junit.Test;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TransactionLifecycleManagerTest {

    @Test
    public void commitIsIdempotentAndReplaySafe() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1000);

        TransactionLifecycleManager.TransitionResult started =
            manager.onStart("tx-1", "corr-1", 5000L, "0xabc", 1_000L, 11L);
        assertTrue(started.isApplied());
        assertEquals(6_000L, started.getRecord().deadlineMs);

        TransactionLifecycleManager.TransitionResult committed =
            manager.onCommit("tx-1", "corr-1", 2_000L);
        assertTrue(committed.isApplied());
        assertEquals(TransactionLifecycleManager.TxStatus.COMMITTED, committed.getRecord().status);
        assertEquals(2_000L, committed.getRecord().completedAtMs);

        TransactionLifecycleManager.TransitionResult duplicateCommit =
            manager.onCommit("tx-1", "corr-1", 9_000L);
        assertTrue(duplicateCommit.isIdempotent());

        TransactionLifecycleManager.TransitionResult replayStart =
            manager.onStart("tx-1", "corr-1", 5000L, "0xabc", 1_000L, 11L);
        assertFalse(replayStart.isApplied());
        assertFalse(replayStart.isIdempotent());

        assertFalse("a timer for a finished transaction is a no-op", manager.onTimer(11L, 6_000L).isTimedOut());
    }

    @Test
    public void abortReplayIsIdempotentAndPreventsCommit() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1000);

        assertTrue(manager.onStart("tx-2", "corr-2", 5000L, "0xdef", 5_000L, 12L).isApplied());
        assertTrue(manager.onAbort("tx-2", "corr-2", "manual", 5_001L).isApplied());

        TransactionLifecycleManager.TransitionResult replayAbort =
            manager.onAbort("tx-2", "corr-2", "manual", 5_002L);
        assertTrue(replayAbort.isIdempotent());

        TransactionLifecycleManager.TransitionResult invalidCommit =
            manager.onCommit("tx-2", "corr-2", 5_003L);
        assertFalse(invalidCommit.isApplied());
        assertFalse(invalidCommit.isIdempotent());
    }

    @Test
    public void timerEventMovesTransactionToTimedOutAndRejectsCommit() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1000);

        assertTrue(manager.onStart("tx-3", "corr-3", 100L, "0xaaa", 10_000L, 13L).isApplied());

        TransactionLifecycleManager.TransitionResult expired = manager.onTimer(13L, 10_100L);
        assertTrue(expired.isTimedOut());
        assertEquals(TransactionLifecycleManager.TxStatus.TIMED_OUT, expired.getRecord().status);
        assertEquals(10_100L, expired.getRecord().completedAtMs);

        TransactionLifecycleManager.TransitionResult invalidCommit =
            manager.onCommit("tx-3", "corr-3", 10_050L);
        assertFalse(invalidCommit.isApplied());
        assertFalse(invalidCommit.isIdempotent());
        assertFalse(invalidCommit.isTimedOut());
    }

    @Test
    public void commitOrAbortStampedAtOrAfterDeadlineExpiresTheTransaction() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1000);
        assertTrue(manager.onStart("tx-c", "corr", 100L, "0xaaa", 1_000L, 1L).isApplied());
        assertTrue(manager.onStart("tx-a", "corr", 100L, "0xaaa", 1_000L, 2L).isApplied());
        assertTrue(manager.onStart("tx-ok", "corr", 100L, "0xaaa", 1_000L, 3L).isApplied());

        assertTrue(manager.onCommit("tx-ok", "corr", 1_099L).isApplied());
        TransactionLifecycleManager.TransitionResult commit = manager.onCommit("tx-c", "corr", 1_100L);
        assertTrue(commit.isTimedOut());
        assertEquals(TransactionLifecycleManager.TxStatus.TIMED_OUT, commit.getRecord().status);
        assertTrue(manager.onAbort("tx-a", "corr", "manual", 1_100L).isTimedOut());
        assertEquals("timeout", manager.get("tx-a").get().abortReason);
        assertFalse(manager.onTimer(1L, 1_200L).isTimedOut());
    }

    @Test
    public void canTransitionsDefaultTimeoutAndSyntheticAbortPathsAreCovered() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1000);

        assertEquals("missing transactionId", manager.onStart(" ", "corr", 100L, "0xabc", 30_000L, 1L).getReason());
        assertEquals("missing transactionId", manager.canStart(null).getReason());
        assertTrue(manager.canStart("tx-open").isApplied());
        assertNull(manager.canStart("tx-open").getRecord());

        TransactionLifecycleManager.TransitionResult started =
            manager.onStart("tx-open", null, 0L, "0xabc", 30_000L, 2L);
        assertTrue(started.isApplied());
        assertEquals(30_000L, started.getRecord().timeoutMs);

        assertTrue(manager.canStart("tx-open").isIdempotent());
        assertTrue(manager.canCommit("tx-open").isApplied());
        assertEquals(TransactionLifecycleManager.TxStatus.STARTED, manager.canAbort("tx-open").getRecord().status);

        TransactionLifecycleManager.TransitionResult aborted =
            manager.onAbort("tx-open", "corr-open", "manual", 30_001L);
        assertTrue(aborted.isApplied());
        assertEquals(TransactionLifecycleManager.TxStatus.ABORTED, aborted.getRecord().status);
        assertEquals("corr-open", aborted.getRecord().correlationId);
        assertEquals("manual", aborted.getRecord().abortReason);

        assertTrue(manager.canAbort("tx-open").isIdempotent());
        assertEquals("cannot commit terminal transaction: ABORTED", manager.canCommit("tx-open").getReason());
        assertEquals("cannot commit terminal transaction: ABORTED",
            manager.onCommit("tx-open", "corr-open", 30_002L).getReason());

        TransactionLifecycleManager.TransitionResult syntheticAbort =
            manager.onAbort("tx-synthetic", "corr-synth", "missing", 30_003L);
        assertTrue(syntheticAbort.isApplied());
        assertEquals(TransactionLifecycleManager.TxStatus.ABORTED, syntheticAbort.getRecord().status);
        assertEquals("corr-synth", syntheticAbort.getRecord().correlationId);
        assertTrue(manager.canAbort("tx-synthetic").isIdempotent());

        Map<String, Object> stats = manager.stats();
        assertEquals(0, stats.get("active"));
        assertEquals(2, stats.get("terminal"));
        assertEquals(0L, stats.get("committed"));
        assertEquals(2L, stats.get("aborted"));
        assertEquals(0L, stats.get("timedOut"));
    }

    @Test
    public void timeoutStatsAndTerminalEvictionAreTracked() {
        TransactionLifecycleManager manager = new TransactionLifecycleManager(1);

        assertTrue(manager.onStart("tx-timeout", "corr-timeout", 50L, "0xaaa", 40_000L, 9L).isApplied());
        assertTrue(manager.onTimer(9L, 40_100L).isTimedOut());
        assertTrue(manager.canAbort("tx-timeout").isIdempotent());
        assertTrue(manager.onAbort("tx-timeout", "corr-timeout", "ignored", 40_101L).isIdempotent());

        for (int i = 0; i <= 100; i++) {
            assertTrue(manager.onAbort("tx-" + i, "corr-" + i, "manual", 40_200L).isApplied());
        }

        Map<String, Object> stats = manager.stats();
        assertEquals(0, stats.get("active"));
        assertEquals(100, stats.get("terminal"));
        assertEquals(100L, stats.get("aborted"));
        assertEquals(0L, stats.get("timedOut"));
        assertFalse(manager.get("tx-timeout").isPresent());
        assertFalse(manager.get("tx-0").isPresent());
        assertTrue(manager.get("tx-100").isPresent());
    }
}
