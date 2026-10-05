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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.aeron.cluster.service.Cluster;
import org.junit.Test;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeaderDiscoveryServiceTest {

    @Test
    public void testNotifyBecameLeaderUpdatesKnownAndCache() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setSelfUrl("http://localhost:8090");

        service.notifyBecameLeader(3);

        assertEquals("http://localhost:8090", service.getKnownLeaderUrl());
        assertEquals(3, service.getKnownLeaderMemberId());
        assertEquals("http://localhost:8090", service.getCachedLeaderUrl());
        assertTrue(service.isLeaderKnown());
    }

    @Test
    public void testInvalidateCacheClearsCachedLeader() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setKnownLeader("http://localhost:8090", 1);

        service.invalidateCache();

        assertNull(service.getCachedLeaderUrl());
        assertEquals("http://localhost:8090", service.getKnownLeaderUrl());
    }

    @Test
    public void testIsSameUrlHandlesLocalhostAndIp() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());

        assertTrue(service.isSameUrl("http://localhost:8090", "http://127.0.0.1:8090"));
        assertFalse(service.isSameUrl("http://localhost:8090", "http://127.0.0.1:8091"));
    }

    @Test
    public void testBestKnownLeaderPrefersKnown() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setKnownLeader("http://leader-1:8090", 2);

        assertEquals("http://leader-1:8090", service.getBestKnownLeaderUrl());
    }

    @Test
    public void testNotifyLostLeadershipClearsTrackedLeaderState() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setSelfUrl("http://self:8090");
        service.notifyBecameLeader(2);

        service.notifyLostLeadership();

        assertNull(service.getCachedLeaderUrl());
        assertNull(service.getKnownLeaderUrl());
        assertEquals(-1, service.getKnownLeaderMemberId());
        assertFalse(service.isLeaderKnown());
        assertNull(service.getKnownLeaderHint());
    }

    @Test
    public void testDiscoverLeaderReturnsCachedLeaderBeforeInspectingCluster() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setKnownLeader("http://leader-1:8090", 2);

        Cluster cluster = mock(Cluster.class);
        assertEquals("http://leader-1:8090", service.discoverLeader(cluster.role()));
    }

    @Test
    public void testDiscoverLeaderUsesClusterRoleAndTrackedLeader() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setSelfUrl("http://self:8090");

        Cluster leaderCluster = mock(Cluster.class);
        when(leaderCluster.role()).thenReturn(Cluster.Role.LEADER);
        assertEquals("http://self:8090", service.discoverLeader(leaderCluster.role()));

        service.invalidateCache();
        service.setKnownLeader("http://leader-2:8090", 2);
        service.invalidateCache();
        Cluster followerCluster = mock(Cluster.class);
        when(followerCluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        assertEquals("http://leader-2:8090", service.discoverLeader(followerCluster.role()));
    }

    @Test
    public void testLostLeadershipKeepsNewerLeaderAlreadyKnownFromLog() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        service.setSelfUrl("http://self:8090");
        service.notifyBecameLeader(0);
        service.setKnownLeader("http://leader-2:8090", 2);

        service.notifyLostLeadership();

        assertEquals("http://leader-2:8090", service.getKnownLeaderUrl());
        assertEquals(2, service.getKnownLeaderMemberId());
    }

    @Test
    public void testDiscoverLeaderFallsBackToPeerPollingAndDeactivateInvalidatesCache() throws Exception {
        HttpServer server = startServer(
            "/ngrok/v1/aeron/cluster-state",
            200,
            "{\"role\":\"FOLLOWER\",\"leaderUrl\":\"http://leader-polled:8090\"}"
        );
        try {
            String peerUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/ngrok";
            LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), List.of(peerUrl));

            assertEquals("http://leader-polled:8090", service.discoverLeader(null));
            assertEquals("http://leader-polled:8090", service.getCachedLeaderUrl());

            service.deactivate();

            assertNull(service.getCachedLeaderUrl());
            assertNull(service.getBestKnownLeaderUrl());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testActivateSettersAndPeerLeaderPollingPaths() throws Exception {
        HttpServer server = startServer(
            "/v1/aeron/cluster-state",
            200,
            "{\"role\":\"LEADER\"}"
        );
        try {
            LeaderDiscoveryService service = new LeaderDiscoveryService();
            service.setNodeIdMapping(Map.of(1, "http://leader-1:8090"));
            service.setPeerUrls(List.of("http://127.0.0.1:" + server.getAddress().getPort()));
            service.setSelfUrl("http://self:8090");
            service.activate();

            assertEquals("http://127.0.0.1:" + server.getAddress().getPort(), service.discoverLeader(null));
            assertEquals("http://127.0.0.1:" + server.getAddress().getPort(), service.getKnownLeaderHint());
            assertEquals("http://127.0.0.1:" + server.getAddress().getPort(), service.getBestKnownLeaderUrl());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void testDiscoverLeaderHandlesUnreachablePeers() {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), List.of("http://127.0.0.1:1"));
        Cluster follower = mock(Cluster.class);
        when(follower.role()).thenReturn(Cluster.Role.FOLLOWER);

        assertNull(service.discoverLeader(follower.role()));
        assertNull(service.getCachedLeaderUrl());
        assertNull(service.getKnownLeaderHint());
    }

    @Test
    public void testPrivateJsonExtractionAndUrlComparisonBranches() throws Exception {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());

        assertEquals("LEADER", extractJsonField(service, "{\"role\":\"LEADER\"}", "role"));
        assertEquals("null", extractJsonField(service, "{\"leaderUrl\":null}", "leaderUrl"));
        assertEquals("true", extractJsonField(service, "{\"leader\":true}", "leader"));
        assertEquals("42", extractJsonField(service, "{\"leaderId\":42}", "leaderId"));
        assertNull(extractJsonField(service, "{\"other\":1}", "role"));
        assertTrue(service.isSameUrl("http://localhost:8090", "http://localhost:8090"));
        assertFalse(service.isSameUrl(null, "http://localhost:8090"));
        assertFalse(service.isSameUrl("http://localhost:8090", "http://localhost:8091"));
        assertFalse(service.isSameUrl("http://[invalid", "http://localhost:8090"));
    }

    @Test
    public void testKnownLeaderHintCanFallBackToCacheOnly() throws Exception {
        LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), Collections.emptyList());
        setField(service, "cachedLeaderUrl", "http://cached-only:8090");
        setField(service, "cachedLeaderTimestamp", System.currentTimeMillis());
        setField(service, "knownLeaderUrl", null);

        assertEquals("http://cached-only:8090", service.getKnownLeaderHint());
    }

    /**
     * Two followers are each other's first peer; the leader is the second peer. Each follower's
     * stub answers the leader endpoint the way {@code ConsensusStatusHandler} does: a plain query
     * runs that node's own discovery, a {@code localOnly=true} query answers from local knowledge.
     */
    @Test
    public void testFollowersThatAreEachOthersFirstPeerDoNotPollRecursively() throws Exception {
        Cluster follower = mock(Cluster.class);
        when(follower.role()).thenReturn(Cluster.Role.FOLLOWER);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        AtomicInteger followerQueries = new AtomicInteger();
        LeaderDiscoveryService[] nodes = new LeaderDiscoveryService[2];
        HttpServer[] stubs = new HttpServer[2];
        HttpServer leader = startServer("/v1/consensus/leader", 200, "{\"isLeader\":true}");
        try {
            for (int i = 0; i < 2; i++) {
                LeaderDiscoveryService self = nodes[i] = new LeaderDiscoveryService(new HashMap<>(), new ArrayList<>());
                stubs[i] = startLeaderEndpoint(exchange -> {
                    followerQueries.incrementAndGet();
                    int depth = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(depth, Math::max);
                    try {
                        if (depth > 6) {
                            return null; // stop a runaway loop; depth is what the test asserts on
                        }
                        String query = exchange.getRequestURI().getQuery();
                        return query != null && query.contains("localOnly=true")
                            ? self.getKnownLeaderHint()
                            : self.discoverLeader(follower.role());
                    } finally {
                        inFlight.decrementAndGet();
                    }
                });
            }
            String leaderUrl = url(leader);
            nodes[0].setPeerUrls(List.of(url(stubs[1]), leaderUrl));
            nodes[1].setPeerUrls(List.of(url(stubs[0]), leaderUrl));

            long start = System.nanoTime();
            assertEquals(leaderUrl, nodes[0].discoverLeader(follower.role()));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertEquals("one bounded, non-nested query to the other follower", 1, followerQueries.get());
            assertEquals(1, maxInFlight.get());
            assertTrue("took " + elapsedMs + " ms", elapsedMs < 1000);
        } finally {
            leader.stop(0);
            for (HttpServer stub : stubs) {
                if (stub != null) {
                    stub.stop(0);
                }
            }
        }
    }

    @Test
    public void testConcurrentCallersWithExpiredCacheShareOnePeerPoll() throws Exception {
        int callers = 8;
        AtomicInteger peerPolls = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        HttpServer leader = startLeaderEndpoint(exchange -> {
            peerPolls.incrementAndGet();
            release.await(5, TimeUnit.SECONDS);
            return "self";
        });
        ExecutorService pool = Executors.newFixedThreadPool(callers);
        try {
            String leaderUrl = url(leader);
            LeaderDiscoveryService service = new LeaderDiscoveryService(new HashMap<>(), List.of(leaderUrl));
            Cluster follower = mock(Cluster.class);
            when(follower.role()).thenReturn(Cluster.Role.FOLLOWER);

            CountDownLatch started = new CountDownLatch(callers);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < callers; i++) {
                results.add(pool.submit(() -> {
                    started.countDown();
                    return service.discoverLeader(follower.role());
                }));
            }
            started.await(5, TimeUnit.SECONDS);
            Thread.sleep(300);
            release.countDown();

            int resolved = 0;
            for (Future<String> result : results) {
                String leaderSeen = result.get(10, TimeUnit.SECONDS);
                if (leaderSeen != null) {
                    assertEquals(leaderUrl, leaderSeen);
                    resolved++;
                }
            }
            assertEquals(1, peerPolls.get());
            assertTrue(resolved >= 1);
            assertEquals(leaderUrl, service.discoverLeader(follower.role()));
            assertEquals(1, peerPolls.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
            leader.stop(0);
        }
    }

    private interface LeaderAnswer {
        /** Returns the leader to report: null for unknown, "self" for this stub. */
        String answer(HttpExchange exchange) throws Exception;
    }

    private static HttpServer startLeaderEndpoint(LeaderAnswer answer) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/v1/consensus/leader", exchange -> {
            String body;
            try {
                String leader = answer.answer(exchange);
                body = "self".equals(leader)
                    ? "{\"isLeader\":true}"
                    : "{\"isLeader\":false,\"currentLeader\":" + (leader == null ? "null" : "\"" + leader + "\"") + "}";
            } catch (Exception e) {
                body = "{\"isLeader\":false,\"currentLeader\":null}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(bytes);
            }
        });
        server.start();
        return server;
    }

    private static String url(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static String extractJsonField(LeaderDiscoveryService service, String json, String field) throws Exception {
        Method method = LeaderDiscoveryService.class.getDeclaredMethod("extractJsonField", String.class, String.class);
        method.setAccessible(true);
        return (String) method.invoke(service, json, field);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static HttpServer startServer(String path, int status, String body) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(path, exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(bytes);
            }
        });
        server.start();
        return server;
    }
}
