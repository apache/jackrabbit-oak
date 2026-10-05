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

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.IdleStrategy;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Transaction deadlines come from the START entry's cluster timestamp and expire through an Aeron timer,
 * so COMMIT-versus-expiry is decided by log order alone.
 */
public class ReplicatedTransactionDecisionTest {

    private static final long TIMEOUT_MS = 500L;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final AeronIngressControlPayloadBuilder payloads = new AeronIngressControlPayloadBuilder();

    @Test
    public void commitRacingExpiryIsDecidedIdenticallyRegardlessOfLocalClockAndOnReplay() throws Exception {
        Member fast = new Member("fast");
        List<Step> log = Arrays.asList(
            start(100, 1_000, "tx-commit"),
            start(200, 1_000, "tx-expire"),
            start(300, 1_000, "tx-late"),
            commit(400, 1_400, "tx-commit"),       // before the deadline
            timer(500, 1_500, "tx-expire"),        // the leader's TimerEvent
            timer(550, 1_500, "tx-commit"),        // cancel lost the race: a no-op
            commit(600, 1_600, "tx-expire"),       // after the TimerEvent
            commit(700, 1_700, "tx-late"),         // after the deadline, before its TimerEvent
            timer(800, 1_800, "tx-late"));
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("tx-commit", "COMMITTED");
        expected.put("tx-expire", "TIMED_OUT");
        expected.put("tx-late", "TIMED_OUT");

        assertEquals(expected, fast.apply(log, fast, 0L));
        Member slow = new Member("slow");
        assertEquals("a member whose local clock runs past the deadline decides the same",
            expected, slow.apply(log, fast, TIMEOUT_MS + 100L));
        Member replay = new Member("replay");
        assertEquals(expected, replay.apply(log, fast, 0L));

        for (Member member : Arrays.asList(fast, slow, replay)) {
            assertEquals(fast.timers, member.timers);
            assertEquals(Long.valueOf(1_000 + TIMEOUT_MS), member.timers.get(fast.timerIds.get("tx-late")));
            assertEquals(1_000L + TIMEOUT_MS, member.engine.getTransactionRecord("tx-late").get().get("deadlineMs"));
        }
    }

    private Step start(long position, long timestamp, String txId) {
        return new Step(position, timestamp, txId,
            payloads.buildStartTransaction(txId, "corr-" + txId, TIMEOUT_MS, "0xabc", null));
    }

    private Step commit(long position, long timestamp, String txId) {
        return new Step(position, timestamp, txId, payloads.buildCommitTransaction(txId, "corr-" + txId, null));
    }

    private static Step timer(long position, long timestamp, String txId) {
        return new Step(position, timestamp, txId, null);
    }

    private static final class Step {
        final long position;
        final long timestamp;
        final String txId;
        final AeronEncodedMessage message;

        Step(long position, long timestamp, String txId, AeronEncodedMessage message) {
            this.position = position;
            this.timestamp = timestamp;
            this.txId = txId;
            this.message = message;
        }
    }

    private final class Member {
        final AeronConsensusEngine engine;
        final Map<Long, Long> timers = new LinkedHashMap<>();
        final Map<String, Long> timerIds = new LinkedHashMap<>();
        final long[] time = {0L};
        final long[] position = {0L};

        Member(String name) throws Exception {
            EthereumWallet wallet = mock(EthereumWallet.class);
            when(wallet.getWalletAddress()).thenReturn("0xabc");
            engine = new AeronConsensusEngine(mock(FileStore.class), mock(NodeStore.class), "http://self:8080",
                Arrays.asList("http://peer-1:8080", "http://peer-2:8080"), wallet,
                tempFolder.newFolder(name).getAbsolutePath(), null, new SnapshotService(),
                new AeronBackgroundCoordinator());
            Cluster cluster = mock(Cluster.class);
            when(cluster.time()).thenAnswer(invocation -> time[0]);
            when(cluster.logPosition()).thenAnswer(invocation -> position[0]);
            when(cluster.idleStrategy()).thenReturn(mock(IdleStrategy.class));
            when(cluster.scheduleTimer(anyLong(), anyLong())).thenAnswer(invocation -> {
                timers.put(invocation.getArgument(0), invocation.getArgument(1));
                return true;
            });
            when(cluster.cancelTimer(anyLong())).thenReturn(true);
            setField(engine, "cluster", cluster);
            setField(engine, "idleStrategy", mock(IdleStrategy.class));
        }

        /** Applies the log; timer entries carry the correlation id the leader's member scheduled. */
        Map<String, String> apply(List<Step> log, Member leader, long localPauseBeforeCommitsMs) throws Exception {
            boolean paused = false;
            for (Step step : log) {
                if (!paused && step.message != null
                    && step.message.templateId == SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION) {
                    Thread.sleep(localPauseBeforeCommitsMs);
                    paused = true;
                }
                time[0] = step.timestamp;
                position[0] = step.position;
                if (step.message == null) {
                    Long correlationId = leader.timerIds.get(step.txId);
                    assertNotNull("no timer was scheduled for " + step.txId, correlationId);
                    engine.onTimerEvent(correlationId, step.timestamp);
                    continue;
                }
                int before = timers.size();
                MessageDispatcher dispatcher = (MessageDispatcher) field(engine, "messageDispatcher");
                dispatcher.dispatch(step.timestamp, step.message.buffer, 0, step.message.totalLength);
                if (timers.size() > before) {
                    timerIds.put(step.txId, timers.keySet().stream().reduce((a, b) -> b).get());
                }
            }
            Map<String, String> statuses = new LinkedHashMap<>();
            for (String txId : Arrays.asList("tx-commit", "tx-expire", "tx-late")) {
                statuses.put(txId, String.valueOf(engine.getTransactionRecord(txId).get().get("status")));
            }
            return statuses;
        }
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
