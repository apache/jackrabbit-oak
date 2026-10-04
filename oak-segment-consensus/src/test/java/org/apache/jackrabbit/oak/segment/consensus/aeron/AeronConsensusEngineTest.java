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
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.IdleStrategy;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
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
import static org.mockito.Mockito.timeout;
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
    public void roleChangeToLeaderUpdatesRoleAndLeaderUrlButNotTerm() {
        AeronConsensusEngine engine = createEngine();

        assertEquals(0, engine.getCurrentTerm());

        engine.onRoleChange(Cluster.Role.LEADER);

        assertEquals("term comes only from the log", 0, engine.getCurrentTerm());
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
        installCluster(engine, cluster);

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        bindClient(engine, client);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));

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
        assertTrue(waitUntil(() -> !ingressManager(engine).isHealthy(), 1500L));
        assertEquals(0, scheduler.tasks.size());
    }

    @Test
    public void currentRoleUsesClusterAsSourceOfTruthWhenAvailable() throws Exception {
        AeronConsensusEngine engine = createEngine();
        engine.onRoleChange(Cluster.Role.LEADER);

        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        installCluster(engine, cluster);

        assertFalse(engine.isLeader());
    }

    @Test
    public void leadershipHistoryCapturesMemberMetadata() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(7);
        when(cluster.time()).thenReturn(12345L);
        installCluster(engine, cluster);
        applyTermEvent(engine, 4);

        engine.onRoleChange(Cluster.Role.LEADER);

        List<LeadershipChange> history = engine.getLeadershipHistory(0);
        assertEquals(1, history.size());
        assertEquals(Cluster.Role.LEADER, history.get(0).newRole);
        assertEquals(7, history.get(0).memberId);
        assertEquals(4, history.get(0).term);
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
        manager.onStart("tx-1", "corr-1", 1L, "0xabc", 1_000L, 100L);
        installCluster(engine, mock(Cluster.class));

        engine.onTimerEvent(100L, 1_001L);

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
        bindClient(engine, healthyClient);
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
                        bindClient(engine, healthyClient);
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
    public void onStartRestoresSnapshotMetadataWhenTheStoreHasItsWatermark() throws Exception {
        SnapshotService snapshotService = mock(SnapshotService.class);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        Cluster cluster = snapshotCluster(Cluster.Role.LEADER, idleStrategy);
        when(cluster.context().clusterDir()).thenReturn(clusterDirWithTerms(0L, 1L, 2L, 3L));
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(
            new SnapshotService.SnapshotState(new AppliedLogPosition(200L, 0, 3L), 3L, 42, "head-1"));
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService,
            new AeronBackgroundCoordinator(), storeWithWatermark(new AppliedLogPosition(200L, 0, 3L)));

        engine.onStart(cluster, snapshotImage);

        verify(snapshotService).restoreSnapshot(snapshotImage, idleStrategy);
        assertEquals(42, engine.getCurrentEpoch());
        assertEquals(3, engine.getCurrentTerm());
        assertTrue(engine.isLeader());
    }

    @Test
    public void onStartRefusesSnapshotWhenTheStoreIsBehindIt() {
        SnapshotService snapshotService = mock(SnapshotService.class);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        Cluster cluster = snapshotCluster(Cluster.Role.FOLLOWER, idleStrategy);
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(
            new SnapshotService.SnapshotState(new AppliedLogPosition(200L, 1, 3L), 3L, 7, "snapshot-head"));
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService,
            new AeronBackgroundCoordinator(), storeWithWatermark(new AppliedLogPosition(200L, 0, 3L)));

        try {
            engine.onStart(cluster, snapshotImage);
            fail("Expected a store behind the snapshot to fail startup");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("behind the Aeron snapshot"));
            assertTrue(e.getMessage(), e.getMessage().contains("--fresh"));
        }
    }

    @Test
    public void onStartFailsWhenTheSnapshotHoldsNoMetadata() {
        SnapshotService snapshotService = mock(SnapshotService.class);
        Image snapshotImage = mock(Image.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        when(snapshotService.restoreSnapshot(snapshotImage, idleStrategy))
            .thenThrow(new io.aeron.cluster.client.ClusterException("no metadata"));
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService);

        try {
            engine.onStart(snapshotCluster(Cluster.Role.LEADER, idleStrategy), snapshotImage);
            fail("Expected an unreadable snapshot to fail startup");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Snapshot load failed"));
        }
    }

    private File clusterDirWithTerms(long... terms) throws Exception {
        File dir = tempFolder.newFolder();
        try (io.aeron.cluster.RecordingLog recordingLog = new io.aeron.cluster.RecordingLog(dir, true)) {
            for (long term : terms) {
                recordingLog.appendTerm(1L, term, term * 100L, 0L);
            }
        }
        return dir;
    }

    private static Cluster snapshotCluster(Cluster.Role role, IdleStrategy idleStrategy) {
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(role);
        when(cluster.idleStrategy()).thenReturn(idleStrategy);
        return cluster;
    }

    private static MemoryNodeStore storeWithWatermark(AppliedLogPosition watermark) {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder root = store.getRoot().builder();
        watermark.writeTo(root);
        try {
            store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return store;
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
    public void onStartSkipsReplayedEntriesAtOrBelowTheStoreWatermark() throws Exception {
        MemoryNodeStore store = new MemoryNodeStore();
        NodeBuilder root = store.getRoot().builder();
        new AppliedLogPosition(512L, 0, 0L).writeTo(root);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), new SnapshotService(),
            new AeronBackgroundCoordinator(new RecordingTaskScheduler(), 2000L, 3000L, 5000L), store);
        List<String> applied = new ArrayList<>();
        engine.setWriteApplicationCallback(new AeronConsensusEngine.WriteApplicationCallback() {
            @Override
            public void applyReplicatedWrite(String walletAddress, String path, String contentType, String message,
                                             String signature, String intentToken, String blobId, String mimeType,
                                             String ipfsCid, org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata auditMetadata) {
                applied.add(path + "@" + auditMetadata.getAppliedLogPosition());
            }
        });
        Cluster cluster = mock(Cluster.class, RETURNS_DEEP_STUBS);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.idleStrategy()).thenReturn(mock(IdleStrategy.class));
        when(cluster.context().clusterDir()).thenReturn(clusterDirWithTerms(0L));
        engine.onStart(cluster, null);
        AeronEncodedMessage replayed = new AeronIngressWritePayloadBuilder()
            .buildWriteProposal("0xabc", "/oak-chain/a/b/c/old", "page", "m", "sig", null, null, "p-old");
        AeronEncodedMessage fresh = new AeronIngressWritePayloadBuilder()
            .buildWriteProposal("0xabc", "/oak-chain/a/b/c/new", "page", "m", "sig", null, null, "p-new");

        engine.onSessionMessage(mock(ClientSession.class), 1L, replayed.buffer, 0, replayed.totalLength, headerAt(512L));
        engine.onSessionMessage(mock(ClientSession.class), 2L, fresh.buffer, 0, fresh.totalLength, headerAt(640L));

        assertEquals(List.of("/oak-chain/a/b/c/new@position=640 item=0 term=0"), applied);
    }

    @Test
    public void nodeLocalApplyFailureStopsThisMemberThroughTheFatalPath() throws Exception {
        AeronConsensusEngine engine = createEngine();
        engine.setWriteApplicationCallback(new AeronConsensusEngine.WriteApplicationCallback() {
            @Override
            public void applyReplicatedWrite(String walletAddress, String path, String contentType, String message,
                                             String signature, String intentToken, String blobId, String mimeType,
                                             String ipfsCid, org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata auditMetadata) {
                throw new RuntimeException("Failed to apply replicated write",
                    new java.io.IOException("No space left on device"));
            }
        });
        setField(engine, "cluster", mock(Cluster.class));
        AeronEncodedMessage write = new AeronIngressWritePayloadBuilder()
            .buildWriteProposal("0xabc", "/oak-chain/a/b/c/doc", "page", "m", "sig", null, null, "p-1");

        try {
            engine.onSessionMessage(mock(ClientSession.class), 1L, write.buffer, 0, write.totalLength, headerAt(640L));
            fail("a node-local apply failure was swallowed");
        } catch (AgentTerminationException e) {
            assertTrue(e.getCause() instanceof io.aeron.exceptions.AeronException);
            assertEquals(io.aeron.exceptions.AeronException.Category.FATAL,
                ((io.aeron.exceptions.AeronException) e.getCause()).category());
            assertTrue(e.getMessage(), e.getMessage().contains("640"));
            assertTrue(e.getCause().getCause().getCause() instanceof java.io.IOException);
        }
    }

    private static Header headerAt(long position) {
        Header header = mock(Header.class);
        when(header.position()).thenReturn(position);
        return header;
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
        installCluster(engine, cluster);

        engine.onSessionMessage(session, 123L, buffer, 4, 5, header);

        verify(ingressHandler).handleMessage(session, 123L, buffer, 4, 5, header, cluster);
    }

    @Test
    public void onTakeSnapshotFlushesOakThenWritesWatermarkTermEpochAndHead() throws Exception {
        SnapshotService snapshotService = mock(SnapshotService.class);
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        when(fileStore.getHead().getRecordId().toString10()).thenReturn("head-9");
        AeronConsensusEngine engine = createEngine(fileStore, snapshotService, new AeronBackgroundCoordinator(),
            storeWithWatermark(new AppliedLogPosition(300L, 1, 5L)));
        io.aeron.ExclusivePublication publication = mock(io.aeron.ExclusivePublication.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        setField(engine, "idleStrategy", idleStrategy);
        setField(engine, "currentEthereumEpoch", 17);
        applyTermEvent(engine, 5);

        engine.onTakeSnapshot(publication);

        ArgumentCaptor<SnapshotService.SnapshotState> state = ArgumentCaptor.forClass(SnapshotService.SnapshotState.class);
        org.mockito.InOrder order = org.mockito.Mockito.inOrder(fileStore, snapshotService);
        order.verify(fileStore).flush();
        order.verify(snapshotService).createSnapshot(eq(publication), eq(idleStrategy), state.capture());
        assertEquals(new AppliedLogPosition(300L, 1, 5L), state.getValue().applied);
        assertEquals(5L, state.getValue().leadershipTermId);
        assertEquals(17, state.getValue().epoch);
        assertEquals("head-9", state.getValue().head);
    }

    @Test
    public void onTakeSnapshotPropagatesOakFlushFailure() throws Exception {
        SnapshotService snapshotService = mock(SnapshotService.class);
        FileStore fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        org.mockito.Mockito.doThrow(new java.io.IOException("disk full")).when(fileStore).flush();
        AeronConsensusEngine engine = createEngine(fileStore, snapshotService, new AeronBackgroundCoordinator(),
            new MemoryNodeStore());

        try {
            engine.onTakeSnapshot(mock(io.aeron.ExclusivePublication.class));
            fail("Expected the snapshot to fail");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("flush"));
        }
        verify(snapshotService, never()).createSnapshot(any(), any(), any());
    }

    @Test
    public void onTakeSnapshotPropagatesClosedSnapshotPublication() {
        AeronConsensusEngine engine = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), new SnapshotService(),
            new AeronBackgroundCoordinator(), new MemoryNodeStore());
        io.aeron.ExclusivePublication publication = mock(io.aeron.ExclusivePublication.class);
        when(publication.offer(any(DirectBuffer.class), anyInt(), anyInt())).thenReturn(io.aeron.Publication.CLOSED);

        try {
            engine.onTakeSnapshot(publication);
            fail("Expected the snapshot to fail");
        } catch (io.aeron.cluster.client.ClusterException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("closed"));
        }
    }

    @Test
    public void snapshotTriggerFiresOnlyOnTheLeader() throws Exception {
        AtomicInteger toggles = new AtomicInteger();
        AeronConsensusEngine engine = createEngine();
        setField(engine, "ingressHandler", mock(AeronIngressHandler.class));
        setField(engine, "snapshotTrigger", new SnapshotTrigger(0L, 1L, () -> toggles.incrementAndGet() > 0, 0L));
        Cluster cluster = mock(Cluster.class);
        setField(engine, "cluster", cluster);

        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        engine.onSessionMessage(mock(ClientSession.class), 1L, mock(DirectBuffer.class), 0, 8, mock(Header.class));
        assertEquals(0, toggles.get());

        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        engine.onSessionMessage(mock(ClientSession.class), 2L, mock(DirectBuffer.class), 0, 8, mock(Header.class));
        assertEquals(1, toggles.get());
    }

    @Test
    public void snapshotCarriesLogDerivedTermAndRestoreReappliesIt() throws Exception {
        SnapshotService snapshotService = mock(SnapshotService.class);
        AeronConsensusEngine source = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), snapshotService,
            new AeronBackgroundCoordinator(), new MemoryNodeStore());
        io.aeron.ExclusivePublication publication = mock(io.aeron.ExclusivePublication.class);
        IdleStrategy idleStrategy = mock(IdleStrategy.class);
        setField(source, "idleStrategy", idleStrategy);
        applyTermEvent(source, 5);

        source.onTakeSnapshot(publication);

        ArgumentCaptor<SnapshotService.SnapshotState> state = ArgumentCaptor.forClass(SnapshotService.SnapshotState.class);
        verify(snapshotService).createSnapshot(eq(publication), eq(idleStrategy), state.capture());
        assertEquals(5L, state.getValue().leadershipTermId);

        Image snapshotImage = mock(Image.class);
        SnapshotService restoreService = mock(SnapshotService.class);
        when(restoreService.restoreSnapshot(snapshotImage, idleStrategy)).thenReturn(state.getValue());
        AeronConsensusEngine restored = createEngine(mock(FileStore.class, RETURNS_DEEP_STUBS), restoreService,
            new AeronBackgroundCoordinator(new RecordingTaskScheduler(), 2000L, 3000L, 5000L), new MemoryNodeStore());

        restored.onStart(snapshotCluster(Cluster.Role.FOLLOWER, idleStrategy), snapshotImage);

        assertEquals(5, restored.getCurrentTerm());
    }

    @Test
    public void ingressOmitsTermUntilTheFirstTermEventThenStampsTheLogTerm() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendDeleteThroughIngress("0xabc", "/content/a", "sig-1", "p-0"));
        assertFalse(captureOffer(client).json.contains("\"term\""));

        applyTermEvent(engine, 3);
        client = installHealthyClient(engine, Cluster.Role.LEADER);

        assertTrue(engine.sendDeleteThroughIngress("0xabc", "/content/b", "sig-2", "p-1"));
        assertTrue(captureOffer(client).json.contains("\"term\":3"));
    }

    @Test
    public void getCurrentLeaderHintUsesKnownLeaderHintForFollower() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        installCluster(engine, cluster);
        LeaderDiscoveryService leaderDiscoveryService =
            (LeaderDiscoveryService) getField(engine, "leaderDiscoveryService");
        leaderDiscoveryService.setKnownLeader("http://leader:8080", 2);

        assertEquals("http://leader:8080", engine.getCurrentLeaderHint());
    }

    @Test
    public void refreshLeaderLogPositionReadsLogPositionButNeverTheLeadersTerm() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        installCluster(engine, cluster);
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

            Method method = AeronConsensusEngine.class.getDeclaredMethod("refreshLeaderLogPositionIfNeeded", boolean.class);
            method.setAccessible(true);
            method.invoke(engine, true);

            assertEquals(2, engine.getCurrentTerm());
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
        installCluster(engine, cluster);
        setField(engine, "lastLeaderLogPositionFetchMs", System.currentTimeMillis());

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
        installCluster(engine, cluster);

        engine.onRoleChange(Cluster.Role.LEADER);

        assertEquals(0, scheduler.tasks.size());
    }

    @Test
    public void durabilityFailureWithoutErrorUsesDefaultMessage() throws Exception {
        AeronConsensusEngine engine = createEngine(List.of("http://peer-1:8080", "http://peer-2:8080"));
        AeronConsensusEngine.DurabilityStatusCallback callback =
            mock(AeronConsensusEngine.DurabilityStatusCallback.class);
        engine.setDurabilityStatusCallback(callback);

        MessageDispatcher dispatcher = (MessageDispatcher) getField(engine, "messageDispatcher");
        MessageDispatcher.DurabilityCallback durabilityCallback =
            (MessageDispatcher.DurabilityCallback) getField(dispatcher, "durabilityCallback");
        durabilityCallback.onSegmentPersisted("p-default-error", 0, null, false, null, 1L);
        verify(callback, never()).onFailure(any(), any());
        durabilityCallback.onSegmentPersisted("p-default-error", 1, null, false, null, 1L);

        verify(callback).onFailure("p-default-error", "durability failed");
    }

    @Test
    public void stepDownAsLeaderClosesInternalClientAndClearsLeader() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(3);
        installCluster(engine, cluster);
        setField(engine, "currentLeader", "http://self:8080");

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        bindClient(engine, client);

        assertTrue(engine.stepDownAsLeader());
        verify(client).close();
        assertFalse(ingressManager(engine).isHealthy());
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
        installCluster(engine, cluster);

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        bindClient(engine, client);

        engine.onRoleChange(Cluster.Role.FOLLOWER);

        assertTrue(waitUntil(() -> {
            try {
                verify(client).close();
                return true;
            } catch (AssertionError assertionError) {
                return false;
            }
        }, 1500L));
        assertTrue(waitUntil(() -> !ingressManager(engine).isHealthy(), 1500L));
        assertEquals(1, scheduler.tasks.size());
        assertEquals("aeron-leader-discovery", scheduler.tasks.get(0).name);
    }

    @Test
    public void stepDownAsLeaderReturnsFalseWhenInternalClientUnavailable() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        installCluster(engine, cluster);

        assertFalse(engine.stepDownAsLeader());
    }

    @Test
    public void stepDownAsLeaderReturnsFalseWhenNodeIsNotLeader() throws Exception {
        AeronConsensusEngine engine = createEngine();
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        installCluster(engine, cluster);

        assertFalse(engine.stepDownAsLeader());
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
        installCluster(engine, cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.CLOSED);
        bindClient(engine, staleClient);

        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        when(healthyClient.clusterSessionId()).thenReturn(91L);
        when(healthyClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(healthyClient));
        setField(engine, "internalClusterClientConnector", connector);

        // Durability sends are fire-and-forget (they may run on the service thread): true means queued.
        assertTrue(engine.sendSegmentPersisted("proposal-7", "head-1", true, null));

        assertTrue(waitUntil(() -> scheduler.tasks.size() == 1, 1500L));
        assertEquals("aeron-durability-retry-segment-persisted-1", scheduler.tasks.get(0).name);
        assertTrue(waitUntil(() -> boundSessionId(engine) == 91L, 1500L));
        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());

        scheduler.tasks.get(0).runnable.run();

        verify(healthyClient, timeout(1500L)).offer(any(MutableDirectBuffer.class), eq(0), anyInt());

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
        installCluster(engine, cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.NOT_CONNECTED);
        bindClient(engine, staleClient);

        io.aeron.cluster.client.AeronCluster healthyClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(healthyClient.isClosed()).thenReturn(false);
        when(healthyClient.clusterSessionId()).thenReturn(93L);
        when(healthyClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);

        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(healthyClient));
        setField(engine, "internalClusterClientConnector", connector);

        assertTrue(engine.sendSegmentPersisted("proposal-8", "head-2", true, null));

        assertTrue(waitUntil(() -> scheduler.tasks.size() == 1, 1500L));
        assertTrue(waitUntil(() -> boundSessionId(engine) == 93L, 1500L));
        scheduler.tasks.get(0).runnable.run();

        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());
        verify(healthyClient, timeout(1500L)).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void sendStartTransactionOffersTransactionMessageWithTerm() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        applyTermEvent(engine, 7);

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
        applyTermEvent(engine, 4);

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
        applyTermEvent(engine, 9);

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
        applyTermEvent(engine, 11);

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
        installCluster(engine, cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");

        io.aeron.cluster.client.AeronCluster staleClient = mock(io.aeron.cluster.client.AeronCluster.class);
        when(staleClient.isClosed()).thenReturn(false);
        when(staleClient.offer(any(MutableDirectBuffer.class), eq(0), anyInt()))
            .thenReturn(io.aeron.Publication.NOT_CONNECTED);
        bindClient(engine, staleClient);

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

        assertTrue(waitUntil(() -> boundSessionId(engine) == 88L, 1500L));
        verify(staleClient).close();
        verify(connector, atLeastOnce()).connectOnce(any(), any(), any(), any());
        verify(healthyClient).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void backPressuredWriteFailsWithoutClosingTheIngressSession() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        when(client.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(io.aeron.Publication.BACK_PRESSURED);
        AeronInternalClusterClientConnector connector =
            (AeronInternalClusterClientConnector) getField(engine, "internalClusterClientConnector");

        assertFalse(engine.sendWriteThroughIngressWithId(
            "0xabc", "/content/write", "page", "{}", "sig-2", null, "proposal-bp"));

        Thread.sleep(50L);
        verify(client, never()).close();
        verify(connector, times(1)).connectOnce(any(), any(), any(), any());
    }

    @Test
    public void backPressuredDurabilityMessageIsRetriedWithoutClosingTheIngressSession() throws Exception {
        RecordingTaskScheduler scheduler = new RecordingTaskScheduler();
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(scheduler, 2000L, 3000L, 5000L),
            mockNodeStore
        );
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.FOLLOWER);
        when(client.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(io.aeron.Publication.BACK_PRESSURED);

        assertTrue(engine.sendSegmentPersisted("proposal-bp", "head-1", true, null));

        assertTrue(waitUntil(() -> scheduler.tasks.size() == 1, 1500L));
        assertEquals("aeron-durability-retry-segment-persisted-1", scheduler.tasks.get(0).name);
        verify(client, never()).close();
    }

    @Test
    public void sendWriteBatchThroughIngressOffersWriteBatchMessage() throws Exception {
        AeronConsensusEngine engine = createEngine();
        io.aeron.cluster.client.AeronCluster client = installHealthyClient(engine, Cluster.Role.LEADER);
        applyTermEvent(engine, 13);

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
        installCluster(engine, cluster);

        assertFalse(engine.sendGCExecuteThroughIngress("gc-1", 5));
    }

    @Test
    public void followerResolvesLeaderFromLeadershipTermEventWithoutHttp() throws Exception {
        AtomicInteger peerCalls = new AtomicInteger();
        HttpServer peer = startFailingPeer(peerCalls);
        try {
            String peerUrl = "http://127.0.0.1:" + peer.getAddress().getPort();
            AeronConsensusEngine engine = createFollowerEngine(peerUrl);

            engine.onNewLeadershipTermEvent(3L, 100L, 0L, 0L, 2, 1, java.util.concurrent.TimeUnit.MILLISECONDS, 1);
            ((LeaderDiscoveryService) getField(engine, "leaderDiscoveryService")).invalidateCache();

            assertEquals("http://leader-2:8080", engine.getCurrentLeader());
            assertEquals(0, peerCalls.get());
        } finally {
            peer.stop(0);
        }
    }

    @Test
    public void lateStepDownDoesNotEraseLeaderFromNewerTermEvent() throws Exception {
        AtomicInteger peerCalls = new AtomicInteger();
        HttpServer peer = startFailingPeer(peerCalls);
        try {
            String peerUrl = "http://127.0.0.1:" + peer.getAddress().getPort();
            AeronConsensusEngine engine = createFollowerEngine(peerUrl);
            engine.onRoleChange(Cluster.Role.LEADER);

            engine.onNewLeadershipTermEvent(4L, 200L, 0L, 200L, 2, 1, java.util.concurrent.TimeUnit.MILLISECONDS, 1);
            engine.onRoleChange(Cluster.Role.FOLLOWER);

            assertEquals("http://leader-2:8080", engine.getCurrentLeaderHint());
            assertEquals("http://leader-2:8080", engine.getCurrentLeader());
            assertEquals(0, peerCalls.get());
        } finally {
            peer.stop(0);
        }
    }

    @Test
    public void stepDownBeforeTermEventStillLearnsNewLeaderFromLog() throws Exception {
        AtomicInteger peerCalls = new AtomicInteger();
        HttpServer peer = startFailingPeer(peerCalls);
        try {
            String peerUrl = "http://127.0.0.1:" + peer.getAddress().getPort();
            AeronConsensusEngine engine = createFollowerEngine(peerUrl);
            engine.onRoleChange(Cluster.Role.LEADER);

            engine.onRoleChange(Cluster.Role.FOLLOWER);
            engine.onNewLeadershipTermEvent(4L, 200L, 0L, 200L, 2, 1, java.util.concurrent.TimeUnit.MILLISECONDS, 1);

            assertEquals("http://leader-2:8080", engine.getCurrentLeader());
            assertEquals(0, peerCalls.get());
        } finally {
            peer.stop(0);
        }
    }

    /**
     * The stale-term decision runs in the replicated apply path, so it must be a pure function of
     * the log: members with different role histories, and a fresh member replaying from the start,
     * must apply and skip exactly the same proposals.
     */
    @Test
    public void staleTermDecisionsAreIdenticalAcrossRoleHistoriesAndReplay() {
        List<String> expected = List.of("p0a", "p0b", "p1a", "p1b");

        // Led term 0, stepped down when term 1 started.
        List<String> formerLeader = applyTermLog(createQuietEngine(), 0, Cluster.Role.LEADER, 3, Cluster.Role.FOLLOWER);
        // Followed in term 0, won the election for term 1.
        List<String> newLeader = applyTermLog(createQuietEngine(), 3, Cluster.Role.LEADER, -1, null);
        // Fresh member replaying the log without any role change.
        List<String> replay = applyTermLog(createQuietEngine(), -1, null, -1, null);

        assertEquals("[formerLeader, newLeader, replay]",
            List.of(expected, expected, expected), List.of(formerLeader, newLeader, replay));
    }

    /**
     * Feeds: term event 0, proposals stamped 0, term event 1, a late proposal stamped 0,
     * proposals stamped 1. Role changes are injected before the given log step.
     */
    private List<String> applyTermLog(AeronConsensusEngine engine,
                                      int firstRoleChangeStep, Cluster.Role firstRole,
                                      int secondRoleChangeStep, Cluster.Role secondRole) {
        List<String> applied = new ArrayList<>();
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message,
                                   String signature, String intentToken, String blobId, String mimeType,
                                   String ipfsCid, String proposalId) {
                applied.add(proposalId);
            }
        });
        dispatcher.setTermProvider(engine::getCurrentTerm);
        AeronIngressWritePayloadBuilder builder = new AeronIngressWritePayloadBuilder();
        Object[][] log = {
            {"term", 0}, {"p0a", 0}, {"p0b", 0}, {"term", 1}, {"late", 0}, {"p1a", 1}, {"p1b", 1}
        };
        for (int step = 0; step < log.length; step++) {
            if (step == firstRoleChangeStep) {
                engine.onRoleChange(firstRole);
            }
            if (step == secondRoleChangeStep) {
                engine.onRoleChange(secondRole);
            }
            String entry = (String) log[step][0];
            int term = (Integer) log[step][1];
            if ("term".equals(entry)) {
                engine.onNewLeadershipTermEvent(term, step * 100L, 0L, step * 100L, term,
                    1, java.util.concurrent.TimeUnit.MILLISECONDS, 1);
            } else {
                AeronEncodedMessage encoded = builder.buildWriteProposal(
                    "0xabc", "/oak-chain/" + entry, "page", entry, "sig", term, null, entry);
                dispatcher.dispatch(step * 100L, encoded.buffer, 0, encoded.totalLength);
            }
        }
        return applied;
    }

    private static void applyTermEvent(AeronConsensusEngine engine, long leadershipTermId) {
        engine.onNewLeadershipTermEvent(leadershipTermId, 0L, 0L, 0L, 0, 1,
            java.util.concurrent.TimeUnit.MILLISECONDS, 1);
    }

    private AeronConsensusEngine createQuietEngine() {
        return createEngine(mockFileStore, null,
            new AeronBackgroundCoordinator(new RecordingTaskScheduler(), 2000L, 3000L, 5000L), mockNodeStore);
    }

    private AeronConsensusEngine createFollowerEngine(String peerUrl) throws Exception {
        AeronConsensusEngine engine = createEngine(
            mockFileStore,
            null,
            new AeronBackgroundCoordinator(new RecordingTaskScheduler(), 2000L, 3000L, 5000L),
            mockNodeStore,
            List.of(peerUrl)
        );
        engine.setNodeIdMapping(Map.of(0, "http://self:8080", 1, peerUrl, 2, "http://leader-2:8080"));
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.FOLLOWER);
        when(cluster.memberId()).thenReturn(0);
        installCluster(engine, cluster);
        return engine;
    }

    /** A peer that records every request; tests assert it is never called. */
    private static HttpServer startFailingPeer(AtomicInteger calls) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        server.start();
        return server;
    }

    /** Installs {@code cluster} and publishes its state, as onStart does on the service thread. */
    private static void installCluster(AeronConsensusEngine engine, Cluster cluster) throws Exception {
        setField(engine, "cluster", cluster);
        setField(engine, "publishedRole", cluster.role());
        setField(engine, "publishedLogPosition", cluster.logPosition());
        setField(engine, "publishedClusterTime", cluster.time());
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
        private final List<ScheduledTask> tasks = new java.util.concurrent.CopyOnWriteArrayList<>();

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
        installCluster(engine, cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));

        io.aeron.cluster.client.AeronCluster client = mock(io.aeron.cluster.client.AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        when(client.clusterSessionId()).thenReturn(99L);
        when(client.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);
        bindClient(engine, client);
        return client;
    }

    /** Binds {@code client} through the ingress owner, as a successful connect would. */
    private void bindClient(AeronConsensusEngine engine, io.aeron.cluster.client.AeronCluster client) throws Exception {
        when(client.sendKeepAlive()).thenReturn(true);
        engine.setAeronDirectoryName(storeDirectory.getAbsolutePath() + "/aeron-test");
        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(client));
        setField(engine, "internalClusterClientConnector", connector);
        if (ingressManager(engine).isHealthy()) {
            ingressManager(engine).notifySendFailure("test rebind");
        }
        assertTrue(ingressManager(engine).ensureAvailable("test bind", 1500L));
    }

    private static AeronInternalIngressClientManager ingressManager(AeronConsensusEngine engine) {
        return (AeronInternalIngressClientManager) getFieldUnchecked(engine, "internalIngressClientManager");
    }

    private static long boundSessionId(AeronConsensusEngine engine) {
        Object sessionId = ingressManager(engine).diagnostics().get("sessionId");
        return sessionId == null ? -1L : ((Number) sessionId).longValue();
    }

    private static CapturedOffer captureOffer(io.aeron.cluster.client.AeronCluster client) {
        ArgumentCaptor<MutableDirectBuffer> bufferCaptor = ArgumentCaptor.forClass(MutableDirectBuffer.class);
        ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(client, timeout(1500L)).offer(bufferCaptor.capture(), eq(0), lengthCaptor.capture());
        MutableDirectBuffer buffer = bufferCaptor.getValue();
        int totalLength = lengthCaptor.getValue();
        SimpleMessageHeader.HeaderInfo header = SimpleMessageHeader.decode(buffer, 0);
        byte[] jsonBytes = new byte[totalLength - SimpleMessageHeader.ENCODED_LENGTH];
        buffer.getBytes(SimpleMessageHeader.ENCODED_LENGTH, jsonBytes);
        String json = new String(jsonBytes, StandardCharsets.UTF_8);
        return new CapturedOffer(header.templateId, json);
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
