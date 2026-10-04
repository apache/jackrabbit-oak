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
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Aeron's {@link AeronCluster} is not threadsafe and its ingress publication is an ExclusivePublication
 * ("NOT threadsafe for offer and tryClaim"). Every call must therefore come from one owner thread.
 */
public class AeronIngressSingleOwnerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void concurrentSendsPollKeepAliveAndReconnectUseOneThreadAndNeverOverlapOffers() throws Exception {
        CallRecorder recorder = new CallRecorder();
        AeronCluster first = recordingClient(recorder, 11L);
        AeronCluster second = recordingClient(recorder, 22L);
        AtomicInteger connects = new AtomicInteger();
        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any())).thenAnswer(invocation -> {
            recorder.record("connect");
            return AeronInternalClusterClientConnector.ConnectAttemptResult.success(
                connects.getAndIncrement() == 0 ? first : second);
        });

        AeronConsensusEngine engine = newEngine(connector);
        int senders = 6;
        ExecutorService pool = Executors.newFixedThreadPool(senders);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger sent = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < senders; t++) {
                int sender = t;
                futures.add(pool.submit(() -> {
                    go.await();
                    for (int i = 0; !stop.get(); i++) {
                        if (engine.sendWriteThroughIngressWithId("0xabc", "/content/" + sender + "/" + i,
                                "page", "m", "sig", null, "p-" + sender + "-" + i)) {
                            sent.incrementAndGet();
                        }
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    return null;
                }));
            }
            go.countDown();

            assertTrue("first client receives offers", waitUntil(() -> recorder.offersTo(first) > 0, 5000L));
            ClientSession ownedSession = mock(ClientSession.class);
            when(ownedSession.id()).thenReturn(11L);
            engine.onSessionClose(ownedSession, 0L, CloseReason.TIMEOUT);
            assertTrue("rebound client receives offers", waitUntil(() -> recorder.offersTo(second) > 0, 5000L));
            assertTrue("poll loop ran", waitUntil(() -> recorder.count("pollEgress") > 2, 5000L));

            stop.set(true);
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            stop.set(true);
            pool.shutdownNow();
            engine.onTerminate(mock(Cluster.class));
        }

        assertTrue("sends succeeded", sent.get() > 0);
        assertTrue("stale client was closed", recorder.count("close") > 0);
        assertEquals("AeronCluster calls came from threads " + recorder.threads, 1, recorder.threads.size());
        assertFalse("offers overlapped", recorder.overlap.get());
    }

    private AeronConsensusEngine newEngine(AeronInternalClusterClientConnector connector) throws Exception {
        AeronConsensusEngine engine = new AeronConsensusEngine(
            mock(FileStore.class),
            mock(NodeStore.class),
            "http://self:8080",
            List.of(),
            mock(EthereumWallet.class),
            tempFolder.newFolder("store").getAbsolutePath(),
            null,
            null,
            new AeronBackgroundCoordinator()
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        setField(engine, "cluster", cluster);
        // Stands in for cluster.idleStrategy() as installed by onStart.
        setField(engine, "idleStrategy", new NoOpIdleStrategy());
        setField(engine, "internalClusterClientConnector", connector);
        engine.setAeronDirectoryName(tempFolder.newFolder("aeron").getAbsolutePath());
        return engine;
    }

    private static AeronCluster recordingClient(CallRecorder recorder, long sessionId) {
        AeronCluster client = mock(AeronCluster.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(client.clusterSessionId()).thenAnswer(invocation -> {
            recorder.record("clusterSessionId");
            return sessionId;
        });
        when(client.isClosed()).thenAnswer(invocation -> {
            recorder.record("isClosed");
            return closed.get();
        });
        when(client.pollEgress()).thenAnswer(invocation -> {
            recorder.record("pollEgress");
            return 0;
        });
        when(client.sendKeepAlive()).thenAnswer(invocation -> {
            recorder.record("sendKeepAlive");
            return true;
        });
        doAnswer(invocation -> {
            recorder.record("close");
            closed.set(true);
            return null;
        }).when(client).close();
        when(client.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenAnswer(invocation -> {
            recorder.record("offer");
            recorder.offers.computeIfAbsent(client, ignored -> new AtomicInteger()).incrementAndGet();
            if (recorder.offersInFlight.incrementAndGet() > 1) {
                recorder.overlap.set(true);
            }
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(200));
            recorder.offersInFlight.decrementAndGet();
            return closed.get() ? io.aeron.Publication.CLOSED : 64L;
        });
        return client;
    }

    private static final class CallRecorder {
        private final Set<String> threads = ConcurrentHashMap.newKeySet();
        private final ConcurrentHashMap<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<AeronCluster, AtomicInteger> offers = new ConcurrentHashMap<>();
        private final AtomicInteger offersInFlight = new AtomicInteger();
        private final AtomicBoolean overlap = new AtomicBoolean();

        void record(String call) {
            threads.add(Thread.currentThread().getName());
            calls.computeIfAbsent(call, ignored -> new AtomicInteger()).incrementAndGet();
        }

        int count(String call) {
            AtomicInteger count = calls.get(call);
            return count == null ? 0 : count.get();
        }

        int offersTo(AeronCluster client) {
            AtomicInteger count = offers.get(client);
            return count == null ? 0 : count.get();
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
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
