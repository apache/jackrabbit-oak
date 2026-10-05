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

import java.util.Arrays;
import java.util.Collections;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterConfig;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterService;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class ConsensusStartupCoordinatorTest {

    private static final String PROP_CONSENSUS_ENABLED = "consensus.enabled";
    private static final String PROP_CONSENSUS_PEERS = "consensus.peers";

    private final ConsensusStartupCoordinator coordinator = new ConsensusStartupCoordinator();

    @After
    public void tearDown() {
        System.clearProperty(PROP_CONSENSUS_ENABLED);
        System.clearProperty(PROP_CONSENSUS_PEERS);
    }

    @Test
    public void testInitializeStartsClusterAndConsensusServicesWhenEnabled() throws Exception {
        System.setProperty(PROP_CONSENSUS_ENABLED, "true");

        TestContext testContext = newTestContext();
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.enabled()).thenReturn(true);
        when(config.selfUrl()).thenReturn("http://validator-0:8090");
        when(config.peerUrls()).thenReturn(new String[] {"http://validator-1:8090", "http://validator-2:8090"});
        when(config.observeElections()).thenReturn(true);
        when(config.logClusterStateDetails()).thenReturn(true);
        when(config.beaconApiUrl()).thenReturn("https://beacon.example");

        AeronClusterService clusterService = mock(AeronClusterService.class);
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        AeronConsensusEngine aeronEngine = mock(AeronConsensusEngine.class);
        when(aeronEngine.getCurrentRole()).thenReturn(ValidatorRole.LEADER);
        when(aeronEngine.getCurrentLeader()).thenReturn("http://validator-0:8090");
        when(aeronEngine.getCurrentEthereumEpoch()).thenReturn(42);

        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            aeronEngine,
            launcher,
            Arrays.asList("validator-0", "validator-1", "validator-2"),
            7
        );
        when(clusterService.startCluster(
            testContext.fileStore,
            testContext.nodeStore,
            testContext.httpServer,
            testContext.wallet,
            "/tmp/test-store",
            testContext.blobStore,
            "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"),
            true,
            true
        )).thenReturn(startupResult);

        ConsensusServicesInitializer initializer = mock(ConsensusServicesInitializer.class);
        when(testContext.componentFactory.createConsensusServicesInitializer()).thenReturn(initializer);

        ConsensusStartupCoordinator.StartupOutcome outcome = coordinator.initialize(
            new ConsensusStartupCoordinator.StartupContext(
                8090,
                true,
                false,
                testContext.fileStore,
                testContext.nodeStore,
                testContext.httpServer,
                testContext.wallet,
                "/tmp/test-store",
                testContext.blobStore,
                clusterService,
                testContext.componentFactory,
                "0xcluster",
                config
            )
        );

        assertEquals(ConsensusStartupCoordinator.StartupDisposition.INITIALIZED, outcome.getDisposition());
        assertEquals("http://validator-0:8090", outcome.getSelfUrl());
        assertEquals(Arrays.asList("http://validator-1:8090", "http://validator-2:8090"), outcome.getPeerUrls());
        assertSame(clusterService, outcome.getAeronClusterService());
        assertSame(launcher, outcome.getLauncher());
        verify(initializer).initialize(
            aeronEngine,
            testContext.httpServer,
            testContext.wallet,
            "/tmp/test-store",
            "https://beacon.example",
            "0xcluster",
            Arrays.asList("validator-0", "validator-1", "validator-2")
        );
    }

    @Test
    public void testInitializeBuildsStandaloneServiceWhenCurrentServiceMissing() throws Exception {
        System.setProperty(PROP_CONSENSUS_ENABLED, "true");
        System.setProperty(PROP_CONSENSUS_PEERS, "http://validator-1:8090,http://validator-2:8090");

        TestContext testContext = newTestContext();
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.enabled()).thenReturn(true);
        when(config.selfUrl()).thenReturn("http://validator-0:8090");
        when(config.peerUrls()).thenReturn(new String[0]);
        when(config.observeElections()).thenReturn(false);
        when(config.logClusterStateDetails()).thenReturn(false);
        when(config.beaconApiUrl()).thenReturn("https://beacon.example");

        AeronClusterService clusterService = mock(AeronClusterService.class);
        when(testContext.componentFactory.createAeronClusterService()).thenReturn(clusterService);

        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            mock(AeronConsensusEngine.class),
            mock(AeronClusterLauncher.class),
            Collections.singletonList("validator-0"),
            1
        );
        when(clusterService.startCluster(
            testContext.fileStore,
            testContext.nodeStore,
            testContext.httpServer,
            testContext.wallet,
            "/tmp/test-store",
            testContext.blobStore,
            "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"),
            false,
            false
        )).thenReturn(startupResult);
        when(testContext.componentFactory.createConsensusServicesInitializer())
            .thenReturn(mock(ConsensusServicesInitializer.class));

        ConsensusStartupCoordinator.StartupOutcome outcome = coordinator.initialize(
            new ConsensusStartupCoordinator.StartupContext(
                8090,
                true,
                false,
                testContext.fileStore,
                testContext.nodeStore,
                testContext.httpServer,
                testContext.wallet,
                "/tmp/test-store",
                testContext.blobStore,
                null,
                testContext.componentFactory,
                "0xcluster",
                config
            )
        );

        assertSame(clusterService, outcome.getAeronClusterService());
        assertEquals(Arrays.asList("http://validator-1:8090", "http://validator-2:8090"), outcome.getPeerUrls());
        verify(testContext.componentFactory).createAeronClusterService();
    }

    @Test
    public void testInitializeReturnsDeferredWhenStandbyMode() throws Exception {
        System.setProperty(PROP_CONSENSUS_ENABLED, "true");

        TestContext testContext = newTestContext();
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.selfUrl()).thenReturn("http://validator-0:8090");
        when(config.peerUrls()).thenReturn(new String[] {"http://validator-1:8090"});

        ConsensusStartupCoordinator.StartupOutcome outcome = coordinator.initialize(
            new ConsensusStartupCoordinator.StartupContext(
                8090,
                true,
                true,
                testContext.fileStore,
                testContext.nodeStore,
                testContext.httpServer,
                testContext.wallet,
                "/tmp/test-store",
                testContext.blobStore,
                null,
                testContext.componentFactory,
                "0xcluster",
                config
            )
        );

        assertEquals(ConsensusStartupCoordinator.StartupDisposition.DEFERRED, outcome.getDisposition());
        assertEquals("http://validator-0:8090", outcome.getSelfUrl());
        assertEquals(Collections.singletonList("http://validator-1:8090"), outcome.getPeerUrls());
        verifyNoInteractions(testContext.componentFactory);
    }

    @Test
    public void testInitializeReturnsDisabledWhenAeronConfigDisabled() throws Exception {
        System.setProperty(PROP_CONSENSUS_ENABLED, "true");
        System.setProperty(PROP_CONSENSUS_PEERS, "http://validator-1:8090");

        TestContext testContext = newTestContext();
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.enabled()).thenReturn(false);
        when(config.selfUrl()).thenReturn("http://validator-0:8090");
        when(config.peerUrls()).thenReturn(new String[0]);

        ConsensusStartupCoordinator.StartupOutcome outcome = coordinator.initialize(
            new ConsensusStartupCoordinator.StartupContext(
                8090,
                true,
                false,
                testContext.fileStore,
                testContext.nodeStore,
                testContext.httpServer,
                testContext.wallet,
                "/tmp/test-store",
                testContext.blobStore,
                null,
                testContext.componentFactory,
                "0xcluster",
                config
            )
        );

        assertEquals(ConsensusStartupCoordinator.StartupDisposition.DISABLED, outcome.getDisposition());
        assertEquals("http://validator-0:8090", outcome.getSelfUrl());
        assertEquals(Collections.singletonList("http://validator-1:8090"), outcome.getPeerUrls());
        assertTrue(outcome.getPeerUrls().contains("http://validator-1:8090"));
        verifyNoInteractions(testContext.componentFactory);
    }

    private static TestContext newTestContext() {
        return new TestContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            mock(SegmentHttpServer.class),
            mock(EthereumWallet.class),
            mock(BlobStore.class),
            mock(GlobalStoreServerComponentFactory.class)
        );
    }

    private static final class TestContext {
        private final FileStore fileStore;
        private final NodeStore nodeStore;
        private final SegmentHttpServer httpServer;
        private final EthereumWallet wallet;
        private final BlobStore blobStore;
        private final GlobalStoreServerComponentFactory componentFactory;

        private TestContext(FileStore fileStore,
                            NodeStore nodeStore,
                            SegmentHttpServer httpServer,
                            EthereumWallet wallet,
                            BlobStore blobStore,
                            GlobalStoreServerComponentFactory componentFactory) {
            this.fileStore = fileStore;
            this.nodeStore = nodeStore;
            this.httpServer = httpServer;
            this.wallet = wallet;
            this.blobStore = blobStore;
            this.componentFactory = componentFactory;
        }
    }
}
