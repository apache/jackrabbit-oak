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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterConfig;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterService;
import org.apache.jackrabbit.oak.segment.consensus.bootstrap.ValidatorBootstrap;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StandbyPromotionCoordinatorTest {

    private final StandbyPromotionCoordinator coordinator = new StandbyPromotionCoordinator();

    @Test
    public void testResolveBootstrapTargetUsesConfiguredPrimary() {
        StandbyPromotionCoordinator.BootstrapTarget target = coordinator.resolveBootstrapTarget(
            "bootstrap-node",
            9001,
            Collections.singletonList("http://validator-1:8090"),
            8090
        );

        assertNotNull(target);
        assertEquals("bootstrap-node", target.getHost());
        assertEquals(9001, target.getPort());
    }

    @Test
    public void testResolveBootstrapTargetFallsBackToFirstPeer() {
        StandbyPromotionCoordinator.BootstrapTarget target = coordinator.resolveBootstrapTarget(
            "",
            0,
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"),
            8090
        );

        assertNotNull(target);
        assertEquals("validator-1", target.getHost());
        assertEquals(8091, target.getPort());
    }

    @Test
    public void testResolveBootstrapTargetReturnsNullWhenNoPrimaryExists() {
        StandbyPromotionCoordinator.BootstrapTarget target = coordinator.resolveBootstrapTarget(
            "",
            0,
            Collections.<String>emptyList(),
            8090
        );

        assertNull(target);
    }

    @Test
    public void testBootstrapAndPromoteDelegatesToValidatorBootstrap() throws Exception {
        ValidatorBootstrap bootstrap = mock(ValidatorBootstrap.class);
        Runnable onPromoted = mock(Runnable.class);
        StandbyPromotionCoordinator.BootstrapTarget target =
            new StandbyPromotionCoordinator.BootstrapTarget("validator-1", 8091);

        coordinator.bootstrapAndPromote(bootstrap, target, onPromoted);

        verify(bootstrap).bootstrapFromPrimary("validator-1", 8091, onPromoted);
    }

    @Test
    public void testDeferredStartupUsesStoredConfigAndCapturesLauncher() throws Exception {
        FileStore fileStore = mock(FileStore.class);
        NodeStore nodeStore = mock(NodeStore.class);
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        EthereumWallet wallet = mock(EthereumWallet.class);
        BlobStore blobStore = mock(BlobStore.class);
        AeronClusterService clusterService = mock(AeronClusterService.class);
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            null,
            launcher,
            Collections.singletonList("validator-0"),
            7
        );

        when(clusterService.getConfig()).thenReturn(config);
        when(config.observeElections()).thenReturn(true);
        when(config.logClusterStateDetails()).thenReturn(true);
        when(clusterService.startCluster(
            fileStore,
            nodeStore,
            httpServer,
            wallet,
            "/tmp/test-store",
            blobStore,
            "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"),
            true,
            true
        )).thenReturn(startupResult);

        StandbyPromotionCoordinator.DeferredAeronStartup deferredStartup =
            coordinator.startDeferredCluster(
                mock(GlobalStoreServerComponentFactory.class),
                clusterService,
                new StandbyPromotionCoordinator.DeferredAeronStartupContext(
                    fileStore,
                    nodeStore,
                    httpServer,
                    wallet,
                    "/tmp/test-store",
                    blobStore,
                    "http://validator-0:8090",
                    Arrays.asList("http://validator-1:8090", "http://validator-2:8090")
                )
            );

        verify(clusterService).startCluster(
            fileStore,
            nodeStore,
            httpServer,
            wallet,
            "/tmp/test-store",
            blobStore,
            "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"),
            true,
            true
        );
        assertSame(clusterService, deferredStartup.getAeronClusterService());
        assertSame(startupResult, deferredStartup.getStartupResult());
        assertSame(launcher, deferredStartup.getStartupResult().getLauncher());
    }

    @Test
    public void testDeferredStartupBuildsStandaloneServiceWhenOsgiServiceIsMissing() throws Exception {
        FileStore fileStore = mock(FileStore.class);
        NodeStore nodeStore = mock(NodeStore.class);
        SegmentHttpServer httpServer = mock(SegmentHttpServer.class);
        EthereumWallet wallet = mock(EthereumWallet.class);
        BlobStore blobStore = mock(BlobStore.class);
        GlobalStoreServerComponentFactory componentFactory = mock(GlobalStoreServerComponentFactory.class);
        AeronClusterService clusterService = mock(AeronClusterService.class);
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            null,
            launcher,
            Collections.singletonList("validator-0"),
            11
        );

        when(componentFactory.createAeronClusterService()).thenReturn(clusterService);
        when(clusterService.startCluster(
            fileStore,
            nodeStore,
            httpServer,
            wallet,
            "/tmp/test-store",
            blobStore,
            "http://validator-0:8090",
            Collections.<String>emptyList(),
            false,
            false
        )).thenReturn(startupResult);

        StandbyPromotionCoordinator.DeferredAeronStartup deferredStartup =
            coordinator.startDeferredCluster(
                componentFactory,
                null,
                new StandbyPromotionCoordinator.DeferredAeronStartupContext(
                    fileStore,
                    nodeStore,
                    httpServer,
                    wallet,
                    "/tmp/test-store",
                    blobStore,
                    "http://validator-0:8090",
                    Collections.<String>emptyList()
                )
            );

        verify(componentFactory).createAeronClusterService();
        verify(clusterService).startCluster(
            fileStore,
            nodeStore,
            httpServer,
            wallet,
            "/tmp/test-store",
            blobStore,
            "http://validator-0:8090",
            Collections.<String>emptyList(),
            false,
            false
        );
        assertSame(clusterService, deferredStartup.getAeronClusterService());
        assertSame(launcher, deferredStartup.getStartupResult().getLauncher());
    }

    @Test
    public void testDeferredStartupRejectsMissingSelfUrl() throws Exception {
        AeronClusterService clusterService = mock(AeronClusterService.class);

        try {
            coordinator.startDeferredCluster(
                mock(GlobalStoreServerComponentFactory.class),
                clusterService,
                new StandbyPromotionCoordinator.DeferredAeronStartupContext(
                    mock(FileStore.class),
                    mock(NodeStore.class),
                    mock(SegmentHttpServer.class),
                    mock(EthereumWallet.class),
                    "/tmp/test-store",
                    mock(BlobStore.class),
                    null,
                    Collections.<String>emptyList()
                )
            );
            fail("Expected IOException");
        } catch (IOException e) {
            assertEquals("Aeron Cluster bootstrap: selfUrl not stored", e.getMessage());
        }

        verifyNoInteractions(clusterService);
    }
}
