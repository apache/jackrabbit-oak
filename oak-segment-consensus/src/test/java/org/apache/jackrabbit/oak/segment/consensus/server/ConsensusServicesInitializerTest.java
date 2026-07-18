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
package org.apache.jackrabbit.oak.segment.consensus.server;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker;
import org.apache.jackrabbit.oak.segment.consensus.eth.BeaconChainClient;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.queue.BackpressureManager;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.queue.RaftAppendCallback;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsensusServicesInitializerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testInitializeWiresMockModeCollaboratorsAndContext() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(true);
        when(blockchainConfig.getNetwork()).thenReturn("mock");
        when(blockchainConfig.getContractAddress()).thenReturn("0xabc");

        EvmBridge evmBridge = mock(EvmBridge.class);
        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        ProposalQueueManagerOptimized proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        ValidatorEarningsTracker earningsTracker = mock(ValidatorEarningsTracker.class);
        BackpressureManager backpressureManager = new BackpressureManager();
        when(testContext.aeronEngine.getBackpressureManager()).thenReturn(backpressureManager);
        when(testContext.aeronEngine.getBeaconClient()).thenReturn(beaconClient);

        RecordingProposalQueueManagerFactory proposalFactory =
            new RecordingProposalQueueManagerFactory(proposalQueueManager);
        RecordingValidatorEarningsTrackerFactory earningsFactory =
            new RecordingValidatorEarningsTrackerFactory(earningsTracker);

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> evmBridge,
            ignored -> beaconClient,
            proposalFactory,
            earningsFactory,
            (property, env, defaultValue) -> defaultValue
        );

        initializer.initialize(
            testContext.aeronEngine,
            testContext.httpServer,
            testContext.wallet,
            testContext.storeDir.toString(),
            "https://beacon.example",
            "0xcluster",
            Arrays.asList("node-1", "node-2", "node-3")
        );

        verify(evmBridge).start();
        verify(beaconClient, never()).startBackgroundPolling();
        verify(proposalQueueManager).start();
        verify(testContext.httpServer).registerSelfValidator(testContext.walletAddress);

        assertSame(proposalQueueManager, testContext.serverContext.proposalQueueManager);
        assertSame(evmBridge, testContext.serverContext.evmBridge);
        assertSame(earningsTracker, testContext.serverContext.validatorEarningsTracker);
        assertEquals(testContext.walletAddress, testContext.serverContext.validatorWalletAddress);
        assertEquals("0xcluster", testContext.serverContext.clusterWalletAddress);
        assertSame(backpressureManager, proposalFactory.backpressureManager);
        assertSame(beaconClient, proposalFactory.beaconClient);
        assertEquals(new File(testContext.storeDir.toFile(), "proposal-queue").getAbsolutePath(),
            proposalFactory.proposalPersistenceDir);
        assertEquals(Arrays.asList(
            testContext.walletAddress,
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000002"
        ), earningsFactory.validatorWallets);
        assertNotNull(proposalFactory.raftAppendCallback);
    }

    @Test
    public void testInitializeUsesFallbackBackpressureAndNullAeronCallbackIsSafe() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(false);
        when(blockchainConfig.getNetwork()).thenReturn("sepolia");
        when(blockchainConfig.getContractAddress()).thenReturn("0xdef");
        when(blockchainConfig.getRpcUrl()).thenReturn("https://rpc.example.invalid");

        EvmBridge evmBridge = mock(EvmBridge.class);
        BeaconChainClient beaconClient = mock(BeaconChainClient.class);
        ProposalQueueManagerOptimized proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        ValidatorEarningsTracker earningsTracker = mock(ValidatorEarningsTracker.class);

        RecordingProposalQueueManagerFactory proposalFactory =
            new RecordingProposalQueueManagerFactory(proposalQueueManager);
        RecordingValidatorEarningsTrackerFactory earningsFactory =
            new RecordingValidatorEarningsTrackerFactory(earningsTracker);

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> evmBridge,
            ignored -> beaconClient,
            proposalFactory,
            earningsFactory,
            (property, env, defaultValue) -> "/var/tmp/custom-proposals"
        );

        initializer.initialize(
            null,
            testContext.httpServer,
            testContext.wallet,
            testContext.storeDir.toString(),
            "https://beacon.example",
            "0xcluster",
            null
        );

        assertNotNull(proposalFactory.backpressureManager);
        assertEquals("/var/tmp/custom-proposals", proposalFactory.proposalPersistenceDir);
        assertEquals(Collections.singletonList(testContext.walletAddress), earningsFactory.validatorWallets);

        proposalFactory.raftAppendCallback.appendProposal("0xwallet", "/a", "text/plain", "body", "sig");
        proposalFactory.raftAppendCallback.appendDeleteProposal("0xwallet", "/a", "sig");
        assertFalse(proposalFactory.raftAppendCallback.tryAppendProposalWithId(
            "proposal-1", "0xwallet", "/a", "text/plain", "body", "sig"));
        assertFalse(proposalFactory.raftAppendCallback.tryAppendDeleteProposalWithId(
            "proposal-2", "0xwallet", "/a", "sig"));
        assertEquals(0, proposalFactory.raftAppendCallback.appendProposalBatch(Collections.<QueuedProposal>emptyList()));
    }

    @Test
    public void testInitializeRejectsMainnetModeForV1() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(false);
        when(blockchainConfig.getNetwork()).thenReturn("mainnet");
        when(blockchainConfig.getRpcUrl()).thenReturn("https://rpc.example.invalid");
        when(blockchainConfig.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> mock(EvmBridge.class),
            ignored -> mock(BeaconChainClient.class),
            (evmBridge, raftAppendCallback, backpressureManager, beaconClient, proposalPersistenceDir) ->
                mock(ProposalQueueManagerOptimized.class),
            wallets -> mock(ValidatorEarningsTracker.class),
            (property, env, defaultValue) -> defaultValue
        );

        try {
            initializer.initialize(
                testContext.aeronEngine,
                testContext.httpServer,
                testContext.wallet,
                testContext.storeDir.toString(),
                "https://beacon.example",
                "0xcluster",
                Collections.singletonList("node-1")
            );
            fail("Expected mainnet mode to be rejected for v1");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("MAINNET mode is disabled"));
        }
    }

    @Test
    public void testInitializeRejectsSepoliaWithoutRpcUrl() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(false);
        when(blockchainConfig.getNetwork()).thenReturn("sepolia");
        when(blockchainConfig.getRpcUrl()).thenReturn(null);
        when(blockchainConfig.getContractAddress()).thenReturn("0x1111111111111111111111111111111111111111");

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> mock(EvmBridge.class),
            ignored -> mock(BeaconChainClient.class),
            (evmBridge, raftAppendCallback, backpressureManager, beaconClient, proposalPersistenceDir) ->
                mock(ProposalQueueManagerOptimized.class),
            wallets -> mock(ValidatorEarningsTracker.class),
            (property, env, defaultValue) -> defaultValue
        );

        try {
            initializer.initialize(
                testContext.aeronEngine,
                testContext.httpServer,
                testContext.wallet,
                testContext.storeDir.toString(),
                "https://beacon.example",
                "0xcluster",
                Collections.singletonList("node-1")
            );
            fail("Expected sepolia mode without RPC URL to be rejected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Chain-backed modes require OAK_BLOCKCHAIN_RPC_URL"));
        }
    }

    @Test
    public void testInitializeRejectsSepoliaPlaceholderContract() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(false);
        when(blockchainConfig.getNetwork()).thenReturn("sepolia");
        when(blockchainConfig.getRpcUrl()).thenReturn("https://rpc.example.invalid");
        when(blockchainConfig.getContractAddress()).thenReturn("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> mock(EvmBridge.class),
            ignored -> mock(BeaconChainClient.class),
            (evmBridge, raftAppendCallback, backpressureManager, beaconClient, proposalPersistenceDir) ->
                mock(ProposalQueueManagerOptimized.class),
            wallets -> mock(ValidatorEarningsTracker.class),
            (property, env, defaultValue) -> defaultValue
        );

        try {
            initializer.initialize(
                testContext.aeronEngine,
                testContext.httpServer,
                testContext.wallet,
                testContext.storeDir.toString(),
                "https://beacon.example",
                "0xcluster",
                Collections.singletonList("node-1")
            );
            fail("Expected placeholder Sepolia contract to be rejected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("example and zero-address defaults are not valid"));
        }
    }

    @Test
    public void testRaftAppendCallbackForwardsIngressOperationsToAeronEngine() throws Exception {
        TestContext testContext = newTestContext();
        BlockchainConfig blockchainConfig = mock(BlockchainConfig.class);
        when(blockchainConfig.isMockMode()).thenReturn(true);
        when(blockchainConfig.getNetwork()).thenReturn("mock");
        when(blockchainConfig.getContractAddress()).thenReturn("0xabc");
        when(testContext.aeronEngine.sendWriteThroughIngress("0xwallet", "/content", "text/plain", "body", "sig"))
            .thenReturn(true);
        when(testContext.aeronEngine.sendWriteThroughIngressWithId(
            "0xwallet", "/content", "text/plain", "body", "sig", null, "proposal-1"))
            .thenReturn(true);
        when(testContext.aeronEngine.sendWriteThroughIngress(
            "0xwallet", "/content", "text/plain", "body", "sig", "blob-1", "image/png"))
            .thenReturn(true);
        when(testContext.aeronEngine.sendWriteThroughIngress(
            "0xwallet", "/content", "text/plain", "body", "sig", "blob-2", "image/png", "cid-1", "proposal-2"))
            .thenReturn(true);
        when(testContext.aeronEngine.sendDeleteThroughIngress("0xwallet", "/content", "sig"))
            .thenReturn(true);
        when(testContext.aeronEngine.sendDeleteThroughIngress("0xwallet", "/content", "sig", "proposal-3"))
            .thenReturn(true);

        ProposalQueueManagerOptimized proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        RecordingProposalQueueManagerFactory proposalFactory =
            new RecordingProposalQueueManagerFactory(proposalQueueManager);
        when(testContext.aeronEngine.sendWriteBatchThroughIngress(proposalFactory.batchProposals)).thenReturn(2);

        ConsensusServicesInitializer initializer = new ConsensusServicesInitializer(
            () -> blockchainConfig,
            ignored -> mock(EvmBridge.class),
            ignored -> mock(BeaconChainClient.class),
            proposalFactory,
            wallets -> mock(ValidatorEarningsTracker.class),
            (property, env, defaultValue) -> defaultValue
        );

        initializer.initialize(
            testContext.aeronEngine,
            testContext.httpServer,
            testContext.wallet,
            testContext.storeDir.toString(),
            "https://beacon.example",
            "0xcluster",
            Collections.singletonList("node-1")
        );

        RaftAppendCallback callback = proposalFactory.raftAppendCallback;
        callback.appendProposal("0xwallet", "/content", "text/plain", "body", "sig");
        callback.appendProposal("0xwallet", "/content", "text/plain", "body", "sig", "blob-1", "image/png");
        callback.appendDeleteProposal("0xwallet", "/content", "sig");
        assertTrue(callback.tryAppendProposalWithId(
            "proposal-1", "0xwallet", "/content", "text/plain", "body", "sig"));
        assertTrue(callback.tryAppendProposalWithId(
            "proposal-2", "0xwallet", "/content", "text/plain", "body", "sig", "blob-2", "image/png", "cid-1"));
        assertTrue(callback.tryAppendDeleteProposalWithId("proposal-3", "0xwallet", "/content", "sig"));
        assertEquals(2, callback.appendProposalBatch(proposalFactory.batchProposals));

        verify(testContext.aeronEngine).sendWriteThroughIngress("0xwallet", "/content", "text/plain", "body", "sig");
        verify(testContext.aeronEngine).sendWriteThroughIngressWithId(
            "0xwallet", "/content", "text/plain", "body", "sig", null, "proposal-1");
        verify(testContext.aeronEngine).sendWriteThroughIngress(
            "0xwallet", "/content", "text/plain", "body", "sig", "blob-1", "image/png");
        verify(testContext.aeronEngine).sendWriteThroughIngress(
            "0xwallet", "/content", "text/plain", "body", "sig", "blob-2", "image/png", "cid-1", "proposal-2");
        verify(testContext.aeronEngine).sendDeleteThroughIngress("0xwallet", "/content", "sig");
        verify(testContext.aeronEngine).sendDeleteThroughIngress("0xwallet", "/content", "sig", "proposal-3");
        verify(testContext.aeronEngine).sendWriteBatchThroughIngress(proposalFactory.batchProposals);
    }

    @Test
    public void testBuildValidatorWalletsAddsSyntheticPeersForClusterSize() {
        assertEquals(Arrays.asList(
            "0xself",
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000002"
        ), ConsensusServicesInitializer.buildValidatorWallets(
            "0xself",
            Arrays.asList("node-1", "node-2", "node-3")
        ));
    }

    private TestContext newTestContext() throws Exception {
        Path storeDir = tempFolder.newFolder("store").toPath();
        ServerContext serverContext = new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            storeDir,
            "http://self"
        );
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        when(httpServer.getContext()).thenReturn(serverContext);

        EthereumWallet wallet = mock(EthereumWallet.class);
        String walletAddress = "0x1234567890abcdef1234567890abcdef12345678";
        when(wallet.getWalletAddress()).thenReturn(walletAddress);

        AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);
        return new TestContext(storeDir, serverContext, httpServer, wallet, walletAddress, aeronEngine);
    }

    private static final class TestContext {
        private final Path storeDir;
        private final ServerContext serverContext;
        private final SegmentHttpServer httpServer;
        private final EthereumWallet wallet;
        private final String walletAddress;
        private final AeronConsensusEngine aeronEngine;

        private TestContext(Path storeDir,
                            ServerContext serverContext,
                            SegmentHttpServer httpServer,
                            EthereumWallet wallet,
                            String walletAddress,
                            AeronConsensusEngine aeronEngine) {
            this.storeDir = storeDir;
            this.serverContext = serverContext;
            this.httpServer = httpServer;
            this.wallet = wallet;
            this.walletAddress = walletAddress;
            this.aeronEngine = aeronEngine;
        }
    }

    private static final class RecordingProposalQueueManagerFactory
            implements ConsensusServicesInitializer.ProposalQueueManagerFactory {
        private final ProposalQueueManagerOptimized proposalQueueManager;
        private final List<QueuedProposal> batchProposals = Arrays.asList(
            mock(QueuedProposal.class),
            mock(QueuedProposal.class)
        );
        private RaftAppendCallback raftAppendCallback;
        private BackpressureManager backpressureManager;
        private BeaconChainClient beaconClient;
        private String proposalPersistenceDir;

        private RecordingProposalQueueManagerFactory(ProposalQueueManagerOptimized proposalQueueManager) {
            this.proposalQueueManager = proposalQueueManager;
        }

        @Override
        public ProposalQueueManagerOptimized create(EvmBridge evmBridge,
                                                    RaftAppendCallback raftAppendCallback,
                                                    BackpressureManager backpressureManager,
                                                    BeaconChainClient beaconClient,
                                                    String proposalPersistenceDir) {
            this.raftAppendCallback = raftAppendCallback;
            this.backpressureManager = backpressureManager;
            this.beaconClient = beaconClient;
            this.proposalPersistenceDir = proposalPersistenceDir;
            return proposalQueueManager;
        }
    }

    private static final class RecordingValidatorEarningsTrackerFactory
            implements ConsensusServicesInitializer.ValidatorEarningsTrackerFactory {
        private final ValidatorEarningsTracker earningsTracker;
        private final List<String> validatorWallets = new ArrayList<>();

        private RecordingValidatorEarningsTrackerFactory(ValidatorEarningsTracker earningsTracker) {
            this.earningsTracker = earningsTracker;
        }

        @Override
        public ValidatorEarningsTracker create(List<String> validatorWallets) {
            this.validatorWallets.clear();
            this.validatorWallets.addAll(validatorWallets);
            return earningsTracker;
        }
    }
}
