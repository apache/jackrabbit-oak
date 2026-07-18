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

import io.aeron.Image;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Focused unit tests for core AeronConsensusEngine behavior that can be verified
 * without a running Aeron cluster.
 */
public class AeronConsensusEngineTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private FileStore mockFileStore;

    @Mock
    private NodeStore mockNodeStore;

    @Mock
    private EthereumWallet mockWallet;

    private File storeDirectory;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        storeDirectory = tempFolder.newFolder("segmentstore");
    }

    @After
    public void tearDown() {
        System.clearProperty("oak.health.peerProbeMode");
        System.clearProperty("oak.cluster.reachability.cacheMs");
        System.clearProperty("oak.cluster.reachability.connectTimeoutMs");
        System.clearProperty("oak.cluster.reachability.readTimeoutMs");
    }

    @Test
    public void roleChangeToLeaderUpdatesRoleTermAndLeaderUrl() {
        AeronConsensusEngine engine = createEngine();

        assertEquals(0, engine.getCurrentTerm());

        engine.onRoleChange(Cluster.Role.LEADER);

        assertEquals(1, engine.getCurrentTerm());
        assertTrue(engine.isLeader());
        assertEquals("http://self:8080", engine.getCurrentLeader());
    }

    @Test
    public void reachableValidatorCountWithHttpProbesReportsQuorumLossWhenPeersAreDown() throws Exception {
        configureReachability("http", 0L, 100, 100);
        AeronConsensusEngine engine = createEngine(List.of(
            "http://127.0.0.1:" + unusedPort(),
            "http://127.0.0.1:" + unusedPort()
        ));

        assertEquals(1, engine.getReachableValidatorCount());
        assertFalse(engine.hasQuorum());
    }

    @Test
    public void reachableValidatorCountWithHttpProbesCountsLivePeers() throws Exception {
        configureReachability("http", 0L, 100, 100);
        HttpServer peer = startHealthServer();
        try {
            AeronConsensusEngine engine = createEngine(List.of(
                "http://127.0.0.1:" + peer.getAddress().getPort(),
                "http://127.0.0.1:" + unusedPort()
            ));

            assertEquals(2, engine.getReachableValidatorCount());
            assertTrue(engine.hasQuorum());
        } finally {
            peer.stop(0);
        }
    }

    @Test
    public void reachableValidatorCountIgnoresPeersReturningUnhealthyStatus() throws Exception {
        configureReachability("http", 0L, 100, 100);
        HttpServer peer = startHealthServer(503);
        try {
            AeronConsensusEngine engine = createEngine(List.of(
                "http://127.0.0.1:" + peer.getAddress().getPort(),
                "http://127.0.0.1:" + unusedPort()
            ));

            assertEquals(1, engine.getReachableValidatorCount());
            assertFalse(engine.hasQuorum());
        } finally {
            peer.stop(0);
        }
    }

    @Test
    public void reachableValidatorCountCanBeExplicitlyDisabled() throws Exception {
        configureReachability("none", 0L, 100, 100);
        AeronConsensusEngine engine = createEngine(List.of(
            "http://127.0.0.1:" + unusedPort(),
            "http://127.0.0.1:" + unusedPort()
        ));

        assertEquals(3, engine.getReachableValidatorCount());
        assertTrue(engine.hasQuorum());
    }

    @Test
    public void roleChangeToLeaderClosesStaleIngressClientAndSchedulesRebind() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(scheduler, 2000L, 3000L, 5000L),
            mockNodeStore
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(7);
        when(cluster.time()).thenReturn(12345L);
        setField(engine, "cluster", cluster);

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        setField(engine, "internalClusterClient", client);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.failure(
                AeronInternalClusterClientConnector.FailureKind.FAILED,
                "connect failed"));
        setField(engine, "internalClusterClientConnector", connector);

        engine.onRoleChange(Cluster.Role.LEADER);

        assertTrue(waitUntil(() -> {
            try {
                verify(client).close();
                return true;
            } catch (AssertionError assertionError) {
                return false;
            }
        }, 1500L));
        assertNull(getField(engine, "internalClusterClient"));
        assertEquals(0, scheduler.tasks.size());
    }

    @Test
    public void currentRoleUsesClusterAsSourceOfTruthWhenAvailable() throws Exception {
        AeronConsensusEngine engine = createEngine();
        engine.onRoleChange(Cluster.Role.LEADER);

        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        setField(engine, "cluster", cluster);

        assertFalse(engine.isLeader());
    }

    @Test
    public void leadershipHistoryCapturesMemberMetadata() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(7);
        when(cluster.time()).thenReturn(12345L);
        setField(engine, "cluster", cluster);

        engine.onRoleChange(Cluster.Role.LEADER);

        List<LeadershipChange> history = engine.getLeadershipHistory(0);
        assertEquals(1, history.size());
        assertEquals(Cluster.Role.LEADER, history.get(0).newRole);
        assertEquals(7, history.get(0).memberId);
        assertEquals(1, history.get(0).term);
        assertTrue(history.get(0).timestamp > 0L);
        assertEquals(12345L, history.get(0).clusterTime);
    }

    @Test
    public void timerEventExpiresTransactionAndInvokesAbortCallback() throws Exception {
        AeronConsensusEngine engine = createEngine();
        AeronConsensusEngine.TransactionLifecycleCallback callback =
            mock(AeronConsensusEngine.TransactionLifecycleCallback.class);
        engine.setTransactionLifecycleCallback(callback);

        TransactionLifecycleManager manager =
            (TransactionLifecycleManager) getField(engine, "transactionLifecycleManager");
        manager.onStart("tx-1", "corr-1", 1L, "0xabc");
        Thread.sleep(5L);

        engine.onTimerEvent(100L, System.currentTimeMillis());

        verify(callback).onAbortTransaction("tx-1", "corr-1", "timeout");
        Optional<Map<String, Object>> tx = engine.getTransactionRecord("tx-1");
        assertTrue(tx.isPresent());
        assertEquals("TIMED_OUT", tx.get().get("status"));
    }

    @Test
    public void reconnectShortCircuitsWhenClientAlreadyHealthy() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        setField(engine, "internalClusterClient", healthyClient);
        setField(engine, "reconnectInProgress", true);

        AtomicInteger ensureCalls = new AtomicInteger(0);
        engine.attemptReconnectForTest(
            "test",
            3,
            attempt -> 0L,
            ensureCalls::incrementAndGet,
            backoff -> {
                fail("sleep should not be called when client is healthy");
                return false;
            }
        );

        assertEquals(0, ensureCalls.get());
        assertFalse((Boolean) getField(engine, "reconnectInProgress"));
    }

    @Test
    public void reconnectRetriesUntilClientBecomesHealthy() throws Exception {
        AeronConsensusEngine engine = createEngine();
        setField(engine, "reconnectInProgress", true);

        AtomicInteger ensureCalls = new AtomicInteger(0);
        List<Long> backoffs = new ArrayList<>();
        engine.attemptReconnectForTest(
            "test",
            4,
            attempt -> (long) attempt * 10L,
            () -> {
                int call = ensureCalls.incrementAndGet();
                if (call == 2) {
                    try {
                        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
                        when(healthyClient.isClosed()).thenReturn(false);
                        setField(engine, "internalClusterClient", healthyClient);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            },
            backoff -> {
                backoffs.add(backoff);
                return true;
            }
        );

        assertEquals(2, ensureCalls.get());
        assertEquals(1, backoffs.size());
        assertEquals(Long.valueOf(10L), backoffs.get(0));
        assertFalse((Boolean) getField(engine, "reconnectInProgress"));
    }

    @Test
    public void reconnectExhaustionDoesNotSleepAfterFinalAttempt() throws Exception {
        AeronConsensusEngine engine = createEngine();
        setField(engine, "reconnectInProgress", true);

        AtomicInteger ensureCalls = new AtomicInteger(0);
        List<Long> backoffs = new ArrayList<>();
        engine.attemptReconnectForTest(
            "test",
            3,
            attempt -> (long) attempt,
            ensureCalls::incrementAndGet,
            backoff -> {
                backoffs.add(backoff);
                return true;
            }
        );

        assertEquals(3, ensureCalls.get());
        assertEquals(2, backoffs.size());
        assertEquals(Long.valueOf(1L), backoffs.get(0));
        assertEquals(Long.valueOf(2L), backoffs.get(1));
        assertFalse((Boolean) getField(engine, "reconnectInProgress"));
    }

    @Test
    public void stopStopsBeaconClientPollingWhenPresent() throws Exception {
        AeronConsensusEngine engine = createEngine();
        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        setField(engine, "beaconClient", beaconClient);

        engine.stop();

        verify(beaconClient).stopBackgroundPolling();
    }

    @Test
    public void onStartRestoresSnapshotAndUpdatesEpochWhenHeadMatches() {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("head-1");
        SnapshotService snapshotService = mock(SnapshotService.class);
        SnapshotService.SnapshotState snapshotState =
            new SnapshotService.SnapshotState("head-1", 42, 1234L, 1);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.idleStrategy()).thenReturn(idleStrategy);
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(snapshotState);

        AeronConsensusEngine engine = createEngine(fileStore, snapshotService);

        engine.onStart(cluster, snapshotImage);

        verify(snapshotService).restoreSnapshot(snapshotImage, idleStrategy);
        assertEquals(42, engine.getCurrentEpoch());
        assertTrue(engine.isLeader());
    }

    @Test
    public void onStartWithSnapshotAndNoStateStartsFresh() {
        SnapshotService snapshotService = mock(SnapshotService.class);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.idleStrategy()).thenReturn(idleStrategy);
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(null);

        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService);

        engine.onStart(cluster, snapshotImage);

        assertEquals(0, engine.getCurrentEpoch());
    }

    @Test
    public void onStartFailsWhenSnapshotHeadDoesNotMatchFileStore() {
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString()).thenReturn("file-head");
        SnapshotService snapshotService = mock(SnapshotService.class);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.idleStrategy()).thenReturn(idleStrategy);
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(
            new SnapshotService.SnapshotState("snapshot-head", 7, 999L, 1)
        );

        AeronConsensusEngine engine = createEngine(fileStore, snapshotService);

        try {
            engine.onStart(cluster, snapshotImage);
            fail("Expected snapshot mismatch to fail startup");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Snapshot load failed"));
        }
    }

    @Test
    public void onStartWithFreshLeaderSchedulesGenesisBootstrapWhenMissing() {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronBackgroundCoordinator backgroundCoordinator =
            new AeronBackgroundCoordinator(scheduler, 7L, 11L, 13L);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.idleStrategy()).thenReturn(mock(IdleStrategy.class));

        AeronConsensusEngine engine = createEngine(
            mock(FileStore.class, RETURNS_DEEP_STUBS),
            new SnapshotService(),
            backgroundCoordinator,
            new MemoryNodeStore()
        );

        engine.onStart(cluster, null);

        assertEquals(1, scheduler.tasks.size());
        assertEquals("genesis-creator", scheduler.tasks.get(0).name);
    }

    @Test
    public void onSessionMessageDelegatesToIngressHandler() throws Exception {
        AeronConsensusEngine engine = createEngine();
        AeronIngressHandler ingressHandler = mock(AeronIngressHandler.class);
        ClientSession session = mock(ClientSession.class);
        Header header = mock(Header.class);
        DirectBuffer buffer = mock(DirectBuffer.class);
        Cluster cluster = mock(Cluster.class);
        setField(engine, "ingressHandler", ingressHandler);
        setField(engine, "cluster", cluster);

        engine.onSessionMessage(session, 123L, buffer, 4, 5, header);

        verify(ingressHandler).handleMessage(session, 123L, buffer, 4, 5, header, cluster);
    }

    @Test
    public void onTakeSnapshotDelegatesToSnapshotService() throws Exception {
        SnapshotService snapshotService = mock(SnapshotService.class);
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService);
        io.aeron.ExclusivePublication publication = mock(io.aeron.ExclusivePublication.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        setField(engine, "idleStrategy", idleStrategy);
        setField(engine, "currentEthereumEpoch", 17);

        engine.onTakeSnapshot(publication);

        verify(snapshotService).createSnapshot(publication, idleStrategy, 17);
    }

    @Test
    public void getCurrentLeaderHintUsesKnownLeaderHintForFollower() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        setField(engine, "cluster", cluster);
        LeaderDiscoveryService leaderDiscoveryService =
            (LeaderDiscoveryService) getField(engine, "leaderDiscoveryService");
        leaderDiscoveryService.setKnownLeader("http://leader:8080", 2);

        assertEquals("http://leader:8080", engine.getCurrentLeaderHint());
    }

    @Test
    public void refreshLeaderTermIfNeededSyncsTermFromLeaderEndpoint() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        setField(engine, "cluster", cluster);
        setField(engine, "currentTerm", 2);

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/aeron/cluster-state", exchange -> {
            byte[] payload = "{\"term\":7,\"logPosition\":300}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        try {
            LeaderDiscoveryService leaderDiscoveryService =
                (LeaderDiscoveryService) getField(engine, "leaderDiscoveryService");
            leaderDiscoveryService.setKnownLeader("http://127.0.0.1:" + server.getAddress().getPort(), 3);

            Method method = AeronConsensusEngine.class.getDeclaredMethod("refreshLeaderTermIfNeeded", boolean.class);
            method.setAccessible(true);
            method.invoke(engine, true);

            assertEquals(7, engine.getCurrentTerm());
            assertEquals(300L, engine.getReplicationLagStatus().get("leaderLogPosition"));
            assertEquals(300L, engine.getReplicationLagStatus().get("replicationLag"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void replicationLagTreatsObservedZeroPositionAsValid() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.logPosition()).thenReturn(0L);
        setField(engine, "cluster", cluster);
        setField(engine, "lastLeaderTermFetchMs", System.currentTimeMillis());

        engine.updateLeaderLogPosition(0L);

        Map<String, Object> status = engine.getReplicationLagStatus();
        assertEquals(Boolean.TRUE, status.get("measurementAvailable"));
        assertEquals("HEALTHY", status.get("healthStatus"));
        assertEquals(0L, status.get("leaderLogPosition"));
        assertEquals(0L, status.get("replicationLag"));
        assertTrue(((Number) status.get("measurementAgeMs")).longValue() >= 0L);
    }

    @Test
    public void createGenesisViaConsensusOffersGenesisProposal() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        Method method = AeronConsensusEngine.class.getDeclaredMethod("createGenesisViaConsensus");
        method.setAccessible(true);
        method.invoke(engine);

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL, offer.templateId);
        assertTrue(offer.json.contains("\"genesisValidator\":\"http://self:8080\""));
        assertTrue(offer.json.contains("\"timestamp\":"));
    }

    @Test
    public void onRoleChangeToLeaderSkipsGenesisBootstrapWhenGenesisExists() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronBackgroundCoordinator backgroundCoordinator =
            new AeronBackgroundCoordinator(scheduler, 7L, 11L, 13L);
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedGenesis(nodeStore);
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.memberId()).thenReturn(7);
        when(cluster.time()).thenReturn(12345L);

        AeronConsensusEngine engine = createEngine(
            mock(FileStore.class, RETURNS_DEEP_STUBS),
            new SnapshotService(),
            backgroundCoordinator,
            nodeStore
        );
        setField(engine, "cluster", cluster);

        engine.onRoleChange(Cluster.Role.LEADER);

        assertEquals(0, scheduler.tasks.size());
    }

    @Test
    public void durabilityAckFailureInvokesFailureCallbackAndClearsPendingState() throws Exception {
        AeronConsensusEngine engine = createEngine();
        AeronConsensusEngine.DurabilityStatusCallback callback =
            mock(AeronConsensusEngine.DurabilityStatusCallback.class);
        engine.setDurabilityStatusCallback(callback);

        DurabilityAckTracker tracker = (DurabilityAckTracker) getField(engine, "durabilityAckTracker");
        tracker.track("p-fail", 3, 2);
        assertEquals(1, pendingDurabilityCount(tracker));

        MessageDispatcher dispatcher = (MessageDispatcher) getField(engine, "messageDispatcher");
        MessageDispatcher.DurabilityCallback durabilityCallback =
            (MessageDispatcher.DurabilityCallback) getField(dispatcher, "durabilityCallback");
        durabilityCallback.onAckSegmentPersisted("p-fail", false, null, "disk full", 3, 2);

        verify(callback).onFailure("p-fail", "disk full");
        verify(callback, never()).onDurable("p-fail", null);
        assertEquals(0, pendingDurabilityCount(tracker));
    }

    @Test
    public void durabilityAckFailureWithoutErrorUsesDefaultMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        AeronConsensusEngine.DurabilityStatusCallback callback =
            mock(AeronConsensusEngine.DurabilityStatusCallback.class);
        engine.setDurabilityStatusCallback(callback);

        DurabilityAckTracker tracker = (DurabilityAckTracker) getField(engine, "durabilityAckTracker");
        tracker.track("p-default-error", 3, 2);
        assertEquals(1, pendingDurabilityCount(tracker));

        MessageDispatcher dispatcher = (MessageDispatcher) getField(engine, "messageDispatcher");
        MessageDispatcher.DurabilityCallback durabilityCallback =
            (MessageDispatcher.DurabilityCallback) getField(dispatcher, "durabilityCallback");
        durabilityCallback.onAckSegmentPersisted("p-default-error", false, null, null, 3, 2);

        verify(callback).onFailure("p-default-error", "durability failed");
        assertEquals(0, pendingDurabilityCount(tracker));
    }

    @Test
    public void stepDownAsLeaderClosesInternalClientAndClearsLeader() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(3);
        setField(engine, "cluster", cluster);
        setField(engine, "currentLeader", "http://self:8080");

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        setField(engine, "internalClusterClient", client);

        assertTrue(engine.stepDownAsLeader());
        verify(client).close();
        assertNull(getField(engine, "internalClusterClient"));
        assertNull(getField(engine, "currentLeader"));
        assertEquals(ValidatorRole.FOLLOWER, getField(engine, "currentRole"));
    }

    @Test
    public void roleChangeFromLeaderClosesInternalClientWithoutSchedulingRebind() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(scheduler, 2000L, 3000L, 5000L),
            mockNodeStore
        );
        setField(engine, "currentRole", ValidatorRole.LEADER);
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(3);
        when(cluster.time()).thenReturn(456L);
        setField(engine, "cluster", cluster);

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        setField(engine, "internalClusterClient", client);

        engine.onRoleChange(Cluster.Role.FOLLOWER);

        assertTrue(waitUntil(() -> {
            try {
                verify(client).close();
                return true;
            } catch (AssertionError assertionError) {
                return false;
            }
        }, 1500L));
        assertNull(getField(engine, "internalClusterClient"));
        assertEquals(1, scheduler.tasks.size());
        assertEquals("aeron-leader-discovery", scheduler.tasks.get(0).name);
    }

    @Test
    public void stepDownAsLeaderReturnsFalseWhenInternalClientUnavailable() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        setField(engine, "cluster", cluster);

        assertFalse(engine.stepDownAsLeader());
    }

    @Test
    public void stepDownAsLeaderReturnsFalseWhenNodeIsNotLeader() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        setField(engine, "cluster", cluster);

        assertFalse(engine.stepDownAsLeader());
    }

    @Test
    public void sendQueueSegmentOffersDurabilityMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendQueueSegment("proposal-1", 3, 2));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_QUEUE_SEGMENT, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-1\""));
        assertTrue(offer.json.contains("\"totalMembers\":3"));
        assertTrue(offer.json.contains("\"requiredAcks\":2"));
    }

    @Test
    public void sendSegmentPersistedDefersReconnectWhenIngressClientIsClosed() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(scheduler, 2000L, 3000L, 5000L),
            mockNodeStore
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.memberId()).thenReturn(1);
        setField(engine, "cluster", cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.CLOSED);
        setField(engine, "internalClusterClient", staleClient);

        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        when(healthyClient.clusterSessionId()).thenReturn(91L);
        when(healthyClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(healthyClient));
        setField(engine, "internalClusterClientConnector", connector);

        assertFalse(engine.sendSegmentPersisted("proposal-7", "head-1", true, null));

        assertEquals(1, scheduler.tasks.size());
        assertEquals("aeron-durability-retry-segment-persisted-1", scheduler.tasks.get(0).name);
        assertTrue(waitUntil(() -> getFieldUnchecked(engine, "internalClusterClient") == healthyClient, 1500L));
        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());

        scheduler.tasks.get(0).runnable.run();

        verify(healthyClient).offer(any(MutableDirectBuffer.class), eq(0), anyInt());

        CapturedOffer offer = captureOffer(healthyClient);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-7\""));
        assertTrue(offer.json.contains("\"durableHead\":\"head-1\""));
    }

    @Test
    public void sendSegmentPersistedDefersReconnectWhenIngressIsNotConnected() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(scheduler, 2000L, 3000L, 5000L),
            mockNodeStore
        );
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.memberId()).thenReturn(1);
        setField(engine, "cluster", cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.NOT_CONNECTED);
        setField(engine, "internalClusterClient", staleClient);

        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        when(healthyClient.clusterSessionId()).thenReturn(93L);
        when(healthyClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(healthyClient));
        setField(engine, "internalClusterClientConnector", connector);

        assertFalse(engine.sendSegmentPersisted("proposal-8", "head-2", true, null));

        assertEquals(1, scheduler.tasks.size());
        assertTrue(waitUntil(() -> getFieldUnchecked(engine, "internalClusterClient") == healthyClient, 1500L));
        scheduler.tasks.get(0).runnable.run();

        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());
        verify(healthyClient).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void sendStartTransactionOffersTransactionMessageWithTerm() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        setField(engine, "currentTerm", 7);

        assertTrue(engine.sendStartTransactionThroughIngress("tx-1", "corr-1", 5000L, "0xabc"));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION, offer.templateId);
        assertTrue(offer.json.contains("\"transactionId\":\"tx-1\""));
        assertTrue(offer.json.contains("\"correlationId\":\"corr-1\""));
        assertTrue(offer.json.contains("\"initiatorWallet\":\"0xabc\""));
        assertTrue(offer.json.contains("\"timeoutMs\":5000"));
        assertTrue(offer.json.contains("\"term\":7"));
    }

    @Test
    public void sendDeleteThroughIngressOffersDeleteProposal() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        setField(engine, "currentTerm", 4);

        assertTrue(engine.sendDeleteThroughIngress("0xabc", "/content/site", "sig-1", "proposal-2"));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL, offer.templateId);
        assertTrue(offer.json.contains("\"walletAddress\":\"0xabc\""));
        assertTrue(offer.json.contains("\"path\":\"/content/site\""));
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-2\""));
        assertTrue(offer.json.contains("\"term\":4"));
    }

    @Test
    public void sendWriteThroughIngressWithIdOffersWriteProposal() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        setField(engine, "currentTerm", 9);

        assertTrue(engine.sendWriteThroughIngressWithId(
            "0xabc",
            "/content/write",
            "page",
            "{\"title\":\"Oak\"}",
            "sig-2",
            "cid-1",
            "proposal-3"
        ));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, offer.templateId);
        assertTrue(offer.json.contains("\"walletAddress\":\"0xabc\""));
        assertTrue(offer.json.contains("\"path\":\"/content/write\""));
        assertTrue(offer.json.contains("\"ipfsCid\":\"cid-1\""));
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-3\""));
        assertTrue(offer.json.contains("\"term\":9"));
    }

    @Test
    public void sendWriteThroughIngressWithBinaryOffersBlobMetadata() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        setField(engine, "currentTerm", 11);

        assertTrue(engine.sendWriteThroughIngress(
            "0xabc",
            "/content/binary",
            "asset",
            "{\"title\":\"Oak\"}",
            "sig-3",
            "blob-99",
            "image/png",
            "cid-2",
            "proposal-4"
        ));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL, offer.templateId);
        assertTrue(offer.json.contains("\"blobId\":\"blob-99\""));
        assertTrue(offer.json.contains("\"mimeType\":\"image/png\""));
        assertTrue(offer.json.contains("\"ipfsCid\":\"cid-2\""));
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-4\""));
        assertTrue(offer.json.contains("\"term\":11"));
    }

    @Test
    public void sendWriteThroughIngressRetriesWithFreshClientAfterStaleSessionFailure() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(3);
        setField(engine, "cluster", cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.NOT_CONNECTED);
        setField(engine, "internalClusterClient", staleClient);

        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        when(healthyClient.clusterSessionId()).thenReturn(88L);
        when(healthyClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(healthyClient));
        setField(engine, "internalClusterClientConnector", connector);

        assertTrue(engine.sendWriteThroughIngressWithId(
            "0xabc",
            "/content/write",
            "page",
            "{\"title\":\"Oak\"}",
            "sig-2",
            "cid-1",
            "proposal-3"
        ));

        assertTrue(waitUntil(() -> getFieldUnchecked(engine, "internalClusterClient") == healthyClient, 1500L));
        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());
        verify(healthyClient).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void sendWriteBatchThroughIngressOffersWriteBatchMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        setField(engine, "currentTerm", 13);

        QueuedProposal first = proposal("proposal-5");
        first.setWalletAddress("0xaaa");
        first.setPath("/content/a");
        first.setContentType("page");
        first.setMessage("hello");
        first.setSignature("sig-a");
        first.setIntentToken("intent-5");

        QueuedProposal second = proposal("proposal-6");
        second.setWalletAddress("0xbbb");
        second.setPath("/content/b");
        second.setContentType("asset");
        second.setMessage("world");
        second.setSignature("sig-b");
        second.setBlobId("blob-6");
        second.setMimeType("image/jpeg");
        second.setIpfsCid("cid-6");

        assertEquals(2, engine.sendWriteBatchThroughIngress(List.of(first, second)));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-5\""));
        assertTrue(offer.json.contains("\"intentToken\":\"intent-5\""));
        assertTrue(offer.json.contains("\"proposalId\":\"proposal-6\""));
        assertTrue(offer.json.contains("\"blobId\":\"blob-6\""));
        assertTrue(offer.json.contains("\"mimeType\":\"image/jpeg\""));
        assertTrue(offer.json.contains("\"ipfsCid\":\"cid-6\""));
        assertTrue(offer.json.contains("\"term\":13"));
    }

    @Test
    public void sendGCProposalThroughIngressOffersGcProposalMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendGCProposalThroughIngress("gc-2", "0xwallet", null, 512L, null));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"gc-2\""));
        assertTrue(offer.json.contains("\"proposerWallet\":\"0xwallet\""));
        assertTrue(offer.json.contains("\"targetRevision\":\"HEAD\""));
        assertTrue(offer.json.contains("\"estimatedReclaimableSizeMB\":512"));
        assertTrue(offer.json.contains("\"estimatedCostUSDC\":\"0\""));
    }

    @Test
    public void sendGCVoteThroughIngressOffersGcVoteMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendGCVoteThroughIngress("gc-3", 4, false, "too expensive"));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_VOTE, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"gc-3\""));
        assertTrue(offer.json.contains("\"validatorId\":4"));
        assertTrue(offer.json.contains("\"approve\":false"));
        assertTrue(offer.json.contains("\"reason\":\"too expensive\""));
    }

    @Test
    public void sendGCExecuteThroughIngressOffersGcExecuteMessageForLeader() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendGCExecuteThroughIngress("gc-1", 5));

        CapturedOffer offer = captureOffer(client);
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE, offer.templateId);
        assertTrue(offer.json.contains("\"proposalId\":\"gc-1\""));
        assertTrue(offer.json.contains("\"executorId\":5"));
    }

    @Test
    public void sendGCExecuteThroughIngressRejectsFollower() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        setField(engine, "cluster", cluster);

        assertFalse(engine.sendGCExecuteThroughIngress("gc-1", 5));
    }

    private AeronConsensusEngine createEngine() {
        return createEngine(mockFileStore, null, new AeronBackgroundCoordinator(), mockNodeStore);
    }

    private AeronConsensusEngine createEngine(List<String> peerUrls) {
        return createEngine(mockFileStore, null, new AeronBackgroundCoordinator(), mockNodeStore, peerUrls);
    }

    private AeronConsensusEngine createEngine(FileStore fileStore, SnapshotService snapshotService) {
        return createEngine(fileStore, snapshotService, new AeronBackgroundCoordinator(), mockNodeStore);
    }

    private AeronConsensusEngine createEngine(FileStore fileStore,
                                              SnapshotService snapshotService,
                                              AeronBackgroundCoordinator backgroundCoordinator,
                                              NodeStore nodeStore) {
        return createEngine(fileStore, snapshotService, backgroundCoordinator, nodeStore, List.of());
    }

    private AeronConsensusEngine createEngine(FileStore fileStore,
                                              SnapshotService snapshotService,
                                              AeronBackgroundCoordinator backgroundCoordinator,
                                              NodeStore nodeStore,
                                              List<String> peerUrls) {
        return new AeronConsensusEngine(
            fileStore,
            nodeStore,
            "http://self:8080",
            peerUrls,
            mockWallet,
            storeDirectory.getAbsolutePath(),
            null,
            snapshotService,
            backgroundCoordinator
        );
    }

    private static void configureReachability(String mode, long cacheMs, int connectTimeoutMs, int readTimeoutMs) {
        System.setProperty("oak.health.peerProbeMode", mode);
        System.setProperty("oak.cluster.reachability.cacheMs", Long.toString(cacheMs));
        System.setProperty("oak.cluster.reachability.connectTimeoutMs", Integer.toString(connectTimeoutMs));
        System.setProperty("oak.cluster.reachability.readTimeoutMs", Integer.toString(readTimeoutMs));
    }

    private static HttpServer startHealthServer() throws Exception {
        return startHealthServer(200);
    }

    private static HttpServer startHealthServer(int statusCode) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/health/local", exchange -> {
            byte[] payload = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        return server;
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void seedGenesis(MemoryNodeStore nodeStore) throws Exception {
        NodeBuilder root = nodeStore.getRoot().builder();
        root.child("oak-chain")
            .child("00")
            .child("00")
            .child("00")
            .child("0x0000000000000000000000000000000000000000")
            .child("content")
            .child("genesis");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static QueuedProposal proposal(String proposalId) {
        return new QueuedProposal(proposalId, "0xtx", null, 1L, 2L, ProposalState.PENDING);
    }

    private static final class RecordingTaskScheduler implements AeronBackgroundCoordinator.TaskScheduler {
        private final List<ScheduledTask> tasks = new ArrayList<>();

        @Override
        public void schedule(String name, long delayMs, Runnable task) {
            tasks.add(new ScheduledTask(name, delayMs, task));
        }

        @Override
        public void close() {
            tasks.clear();
        }
    }

    private static final class ScheduledTask {
        private final String name;
        private final long delayMs;
        private final Runnable runnable;

        private ScheduledTask(String name, long delayMs, Runnable runnable) {
            this.name = name;
            this.delayMs = delayMs;
            this.runnable = runnable;
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Object getFieldUnchecked(Object target, String name) {
        try {
            return getField(target, name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean waitUntil(java.util.concurrent.Callable<Boolean> condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(condition.call())) {
                return true;
            }
            Thread.sleep(25L);
        }
        return Boolean.TRUE.equals(condition.call());
    }

    private io.aeron.cluster.client.AeronCluster installHealthyClient(AeronConsensusEngine engine,
                                                                       Cluster.Role role) throws Exception {
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(role);
        when(cluster.memberId()).thenReturn(3);
        setField(engine, "cluster", cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        when(client.clusterSessionId()).thenReturn(99L);
        when(client.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);
        setField(engine, "internalClusterClient", client);
        AeronInternalIngressClientManager manager =
            (AeronInternalIngressClientManager) getField(engine, "internalIngressClientManager");
        assertTrue(manager.ensureAvailable("test install", 250L));
        return client;
    }

    private static CapturedOffer captureOffer(io.aeron.cluster.client.AeronCluster client) {
        ArgumentCaptor<MutableDirectBuffer> bufferCaptor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(client).offer(bufferCaptor.capture(), eq(0), lengthCaptor.capture());
        MutableDirectBuffer buffer = bufferCaptor.getValue();
        int totalLength = lengthCaptor.getValue();
        SimpleMessageHeader.HeaderInfo header = SimpleMessageHeader.decode(buffer, 0);
        byte[] jsonBytes = new byte[totalLength - SimpleMessageHeader.ENCODED_LENGTH];
        buffer.getBytes(SimpleMessageHeader.ENCODED_LENGTH, jsonBytes);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        return new CapturedOffer(header.templateId, json);
    }

    private static int pendingDurabilityCount(DurabilityAckTracker tracker) throws Exception {
        Field pendingField = DurabilityAckTracker.class.getDeclaredField("pending");
        pendingField.setAccessible(true);
        Map<?, ?> pending = (Map<?, ?>) pendingField.get(tracker);
        return pending.size();
    }

    private static final class CapturedOffer {
        private final int templateId;
        private final String json;

        private CapturedOffer(int templateId, String json) {
            this.templateId = templateId;
            this.json = json;
        }
    }
}
