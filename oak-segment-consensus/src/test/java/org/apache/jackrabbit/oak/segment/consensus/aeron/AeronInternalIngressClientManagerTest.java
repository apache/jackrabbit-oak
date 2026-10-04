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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronInternalIngressClientManagerTest {

    @Test
    public void foreignTimeoutDoesNotTriggerReconnect() throws Exception {
        AtomicInteger connectCalls = new AtomicInteger();
        AeronCluster client = mockBoundClient(77L, null, null);

        AeronInternalIngressClientManager manager = newManager(
            connectCalls,
            () -> client,
            25L,
            10L
        );
        try {
            assertTrue(manager.ensureAvailable("initial", 250L));
            assertEquals(1, connectCalls.get());

            manager.handleClusterSessionClose(999L, CloseReason.TIMEOUT);
            Thread.sleep(40L);

            assertEquals(1, connectCalls.get());
            assertEquals("TIMEOUT", manager.diagnostics().get("lastCloseReason"));
        } finally {
            manager.close();
        }
    }

    @Test
    public void ownedTimeoutAndDuplicateRebindRequestsCollapseToSingleReconnect() throws Exception {
        AtomicInteger connectCalls = new AtomicInteger();
        AeronCluster initial = mockBoundClient(77L, null, null);
        AeronCluster rebound = mockBoundClient(88L, null, null);
        AtomicInteger connectIndex = new AtomicInteger();

        AeronInternalIngressClientManager manager = newManager(
            connectCalls,
            () -> connectIndex.getAndIncrement() == 0 ? initial : rebound,
            15L,
            10L
        );
        try {
            assertTrue(manager.ensureAvailable("initial", 250L));
            assertEquals(77L, boundSessionId(manager));

            manager.handleClusterSessionClose(77L, CloseReason.TIMEOUT);
            manager.requestRebind("duplicate-one");
            manager.requestRebind("duplicate-two");

            assertTrue(waitUntil(() -> boundSessionId(manager) == 88L, 1000L));
            assertEquals(2, connectCalls.get());
            assertEquals(88L, ((Number) manager.diagnostics().get("sessionId")).longValue());
        } finally {
            manager.close();
        }
    }

    @Test
    public void sessionLimitFailureEntersCooldownWithoutTightLoop() throws Exception {
        AtomicInteger connectCalls = new AtomicInteger();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AeronInternalIngressClientManager manager = new AeronInternalIngressClientManager(
            () -> new AeronInternalClusterClientConnector(
                (aeronDirectoryName, ingressPlan, idleStrategy) -> {
                    connectCalls.incrementAndGet();
                    throw new RuntimeException("ERROR - concurrent session limit");
                },
                Thread::sleep,
                1,
                attempt -> 0L
            ),
            AeronIngressEndpointPlanner.systemFromUrls("http://self:8080", java.util.List.of("http://peer:8082")),
            () -> "target/aeron-dir",
            () -> mock(IdleStrategy.class),
            executor,
            System::currentTimeMillis,
            25L,
            10L,
            10L,
            80L,
            5L
        );
        try {
            manager.ensureAvailable("limit", 0L);

            assertTrue(waitUntil(() -> "COOLDOWN".equals(manager.diagnostics().get("state")), 500L));
            assertEquals(1, connectCalls.get());
            Thread.sleep(30L);
            assertEquals(1, connectCalls.get());
            assertNotNull(manager.diagnostics().get("cooldownUntil"));
        } finally {
            manager.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void boundClientIsServicedWithPollAndKeepAlive() throws Exception {
        AtomicInteger connectCalls = new AtomicInteger();
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger keepAlives = new AtomicInteger();
        AeronCluster client = mockBoundClient(55L, polls, keepAlives);

        AeronInternalIngressClientManager manager = newManager(
            connectCalls,
            () -> client,
            20L,
            10L
        );
        try {
            assertTrue(manager.ensureAvailable("initial", 250L));
            assertTrue(waitUntil(() -> polls.get() > 0, 500L));
            assertTrue(waitUntil(() -> keepAlives.get() > 0, 500L));

            Map<String, Object> diagnostics = manager.diagnostics();
            assertEquals("BOUND", diagnostics.get("state"));
            assertEquals(55L, ((Number) diagnostics.get("sessionId")).longValue());
            assertEquals(1, connectCalls.get());
        } finally {
            manager.close();
        }
    }

    @Test
    public void rapidSendFailuresCollapseToSingleReconnectAndCloseStaleClientBeforeReuse() throws Exception {
        AtomicInteger connectCalls = new AtomicInteger();
        AtomicBoolean initialClosed = new AtomicBoolean(false);
        AtomicBoolean reboundObservedClosed = new AtomicBoolean(false);
        AtomicInteger connectIndex = new AtomicInteger();

        AeronCluster initial = mockBoundClient(77L, null, null);
        doAnswer(invocation -> {
            initialClosed.set(true);
            return null;
        }).when(initial).close();

        AeronCluster rebound = mockBoundClient(88L, null, null);
        AeronInternalIngressClientManager manager = newManager(
            connectCalls,
            () -> {
                if (connectIndex.getAndIncrement() == 0) {
                    return initial;
                }
                reboundObservedClosed.set(initialClosed.get());
                return rebound;
            },
            25L,
            10L
        );
        try {
            assertTrue(manager.ensureAvailable("initial", 250L));
            assertEquals(77L, boundSessionId(manager));

            manager.notifySendFailure("offer-closed");
            manager.notifySendFailure("offer-not-connected");

            assertTrue(manager.ensureAvailable("rebound", 1000L));
            assertEquals(88L, boundSessionId(manager));
            assertEquals(2, connectCalls.get());
            assertTrue(reboundObservedClosed.get());
            assertEquals(88L, ((Number) manager.diagnostics().get("sessionId")).longValue());
            verify(initial).close();
        } finally {
            manager.close();
        }
    }

    @Test
    public void offerStillQueuedAtDeadlineTimesOutAndIsNeverSent() throws Exception {
        AeronCluster client = mockBoundClient(55L, null, null);
        when(client.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(64L);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AeronInternalIngressClientManager manager =
            newManager(new AtomicInteger(), () -> client, 1000L, 1000L, executor);
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertTrue(manager.ensureAvailable("initial", 1000L));
            executor.execute(() -> awaitQuietly(release));

            assertEquals(AeronInternalIngressClientManager.SendResult.TIMEOUT,
                manager.offer(new UnsafeBuffer(new byte[8]), 8, "busy-owner", 50L));

            release.countDown();
            assertEquals(AeronInternalIngressClientManager.SendResult.SENT,
                manager.offer(new UnsafeBuffer(new byte[8]), 8, "after-release", 1000L));
            verify(client, times(1)).offer(any(DirectBuffer.class), anyInt(), anyInt());
        } finally {
            release.countDown();
            manager.close();
        }
    }

    @Test
    public void offerAsyncDoesNotWaitForTheOwnerAndReportsTheResultFromIt() throws Exception {
        AeronCluster client = mockBoundClient(55L, null, null);
        when(client.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(64L);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        AeronInternalIngressClientManager manager =
            newManager(new AtomicInteger(), () -> client, 1000L, 1000L, executor);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<AeronInternalIngressClientManager.SendResult> result = new AtomicReference<>();
        AtomicReference<Thread> resultThread = new AtomicReference<>();
        AtomicReference<Thread> ownerThread = new AtomicReference<>();
        try {
            assertTrue(manager.ensureAvailable("initial", 1000L));
            executor.execute(() -> {
                ownerThread.set(Thread.currentThread());
                awaitQuietly(release);
            });

            long start = System.nanoTime();
            manager.offerAsync(new UnsafeBuffer(new byte[8]), 8, "service-thread", sendResult -> {
                resultThread.set(Thread.currentThread());
                result.set(sendResult);
            });
            assertTrue("offerAsync returned without waiting",
                System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(200));
            assertNull(result.get());

            release.countDown();
            assertTrue(waitUntil(() -> result.get() != null, 1000L));
            assertEquals(AeronInternalIngressClientManager.SendResult.SENT, result.get());
            assertSame(ownerThread.get(), resultThread.get());
        } finally {
            release.countDown();
            manager.close();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static AeronInternalIngressClientManager newManager(AtomicInteger connectCalls,
                                                                java.util.function.Supplier<AeronCluster> connectResultSupplier,
                                                                long keepAliveIntervalMs,
                                                                long pollIntervalMs) {
        return newManager(connectCalls, connectResultSupplier, keepAliveIntervalMs, pollIntervalMs,
            Executors.newSingleThreadScheduledExecutor());
    }

    private static AeronInternalIngressClientManager newManager(AtomicInteger connectCalls,
                                                                java.util.function.Supplier<AeronCluster> connectResultSupplier,
                                                                long keepAliveIntervalMs,
                                                                long pollIntervalMs,
                                                                ScheduledExecutorService executor) {
        return new AeronInternalIngressClientManager(
            () -> new AeronInternalClusterClientConnector(
                (aeronDirectoryName, ingressPlan, idleStrategy) -> {
                    connectCalls.incrementAndGet();
                    return connectResultSupplier.get();
                },
                Thread::sleep,
                1,
                attempt -> 0L
            ),
            AeronIngressEndpointPlanner.systemFromUrls("http://self:8080", java.util.List.of("http://peer:8082")),
            () -> "target/aeron-dir",
            () -> mock(IdleStrategy.class),
            executor,
            System::currentTimeMillis,
            keepAliveIntervalMs,
            pollIntervalMs,
            10L,
            80L,
            5L
        );
    }

    private static long boundSessionId(AeronInternalIngressClientManager manager) {
        Object sessionId = manager.diagnostics().get("sessionId");
        return sessionId == null ? -1L : ((Number) sessionId).longValue();
    }

    private static AeronCluster mockBoundClient(long sessionId,
                                                AtomicInteger polls,
                                                AtomicInteger keepAlives) {
        AeronCluster client = mock(AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        when(client.clusterSessionId()).thenReturn(sessionId);
        if (polls != null) {
            doAnswer(invocation -> {
                polls.incrementAndGet();
                return 0;
            }).when(client).pollEgress();
        }
        if (keepAlives != null) {
            when(client.sendKeepAlive()).thenAnswer(invocation -> {
                keepAlives.incrementAndGet();
                return true;
            });
        } else {
            when(client.sendKeepAlive()).thenReturn(true);
        }
        return client;
    }

    private static boolean waitUntil(java.util.concurrent.Callable<Boolean> condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(10L);
        }
        return Boolean.TRUE.equals(condition.call());
    }
}
