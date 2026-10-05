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
import java.lang.reflect.Proxy;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class StandbyModeStartupCoordinatorTest {

    private final StandbyModeStartupCoordinator coordinator = new StandbyModeStartupCoordinator();

    @Test
    public void testInitializeUsesConfiguredPrimaryAndCapturesResolvedRuntimeUrls() throws Exception {
        TestContext testContext = newTestContext();

        StandbyModeStartupCoordinator.StartupResult result = coordinator.initialize(
            testContext.startupContext("bootstrap-node", 9001)
        );

        verify(testContext.bootstrap).bootstrapFromPrimary(eq("bootstrap-node"), eq(9001), any(Runnable.class));
        assertEquals("http://validator-0:8090", result.getSelfUrl());
        assertEquals(Arrays.asList("http://validator-1:8090", "http://validator-2:8090"), result.getPeerUrls());
        verifyNoInteractions(testContext.clusterService);
    }

    @Test
    public void testInitializeFallsBackToFirstPeerWhenPrimaryMissing() throws Exception {
        TestContext testContext = newTestContext();

        StandbyModeStartupCoordinator.StartupResult result = coordinator.initialize(
            testContext.startupContext("", 0)
        );

        verify(testContext.bootstrap).bootstrapFromPrimary(eq("validator-1"), eq(8091), any(Runnable.class));
        assertEquals(Arrays.asList("http://validator-1:8090", "http://validator-2:8090"), result.getPeerUrls());
    }

    @Test
    public void testInitializeRejectsStandbyModeWithoutPrimaryOrPeers() throws Exception {
        TestContext testContext = newTestContext();
        testContext.aeronConfig = aeronConfig("http://validator-0:8090", new String[0], false, false);

        try {
            coordinator.initialize(testContext.startupContext("", 0));
            fail("Expected IOException");
        } catch (IOException e) {
            assertEquals("STANDBY mode requires bootstrap.primary.host or consensus.peers", e.getMessage());
        }

        verifyNoInteractions(testContext.bootstrap);
        verifyNoInteractions(testContext.clusterService);
        verifyNoInteractions(testContext.httpServer);
    }

    @Test
    public void testPromotionStartsDeferredClusterAndHttpServer() throws Exception {
        TestContext testContext = newTestContext();
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            null,
            launcher,
            Arrays.asList("validator-0", "validator-1"),
            5
        );

        doAnswer(invocation -> {
            Runnable onPromoted = invocation.getArgument(2);
            onPromoted.run();
            return null;
        }).when(testContext.bootstrap).bootstrapFromPrimary(anyString(), anyInt(), any(Runnable.class));
        when(testContext.clusterService.startCluster(
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

        StandbyModeStartupCoordinator.StartupResult result = coordinator.initialize(
            testContext.startupContext("bootstrap-node", 9001)
        );

        assertEquals("http://validator-0:8090", result.getSelfUrl());
        verify(testContext.clusterService).startCluster(
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
        );
        verify(testContext.httpServer).start();
        assertSame(testContext.clusterService, testContext.recordedService);
        assertSame(launcher, testContext.recordedLauncher);
    }

    @Test
    public void testPromotionBuildsStandaloneClusterServiceWhenExistingServiceMissing() throws Exception {
        TestContext testContext = newTestContext();
        AeronClusterService fallbackService = mock(AeronClusterService.class);
        AeronClusterLauncher fallbackLauncher = mock(AeronClusterLauncher.class);
        AeronClusterStartupResult startupResult = new AeronClusterStartupResult(
            null,
            fallbackLauncher,
            Collections.singletonList("validator-0"),
            9
        );

        doAnswer(invocation -> {
            Runnable onPromoted = invocation.getArgument(2);
            onPromoted.run();
            return null;
        }).when(testContext.bootstrap).bootstrapFromPrimary(anyString(), anyInt(), any(Runnable.class));
        when(testContext.componentFactory.createAeronClusterService()).thenReturn(fallbackService);
        when(fallbackService.getConfig()).thenReturn(testContext.aeronConfig);
        when(fallbackService.startCluster(
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

        coordinator.initialize(testContext.startupContext("bootstrap-node", 9001, null));

        verify(testContext.componentFactory).createAeronClusterService();
        verify(testContext.httpServer).start();
        assertSame(fallbackService, testContext.recordedService);
        assertSame(fallbackLauncher, testContext.recordedLauncher);
    }

    @Test
    public void testPromotionFailureDoesNotRecordRuntimeOrStartHttpServer() throws Exception {
        TestContext testContext = newTestContext();

        doAnswer(invocation -> {
            Runnable onPromoted = invocation.getArgument(2);
            onPromoted.run();
            return null;
        }).when(testContext.bootstrap).bootstrapFromPrimary(anyString(), anyInt(), any(Runnable.class));
        when(testContext.clusterService.startCluster(
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
        )).thenThrow(new IOException("boom"));

        coordinator.initialize(testContext.startupContext("bootstrap-node", 9001));

        verify(testContext.httpServer, never()).start();
        assertNull(testContext.recordedService);
        assertNull(testContext.recordedLauncher);
    }

    private static TestContext newTestContext() {
        TestContext context = new TestContext();
        context.bootstrap = mock(ValidatorBootstrap.class);
        context.fileStore = mock(FileStore.class, RETURNS_DEEP_STUBS);
        context.nodeStore = mock(NodeStore.class);
        context.httpServer = mock(SegmentHttpServer.class);
        context.wallet = mock(EthereumWallet.class);
        context.blobStore = mock(BlobStore.class);
        context.clusterService = mock(AeronClusterService.class);
        context.componentFactory = mock(GlobalStoreServerComponentFactory.class);
        context.aeronConfig = aeronConfig(
            "http://validator-0:8090",
            new String[] {"http://validator-1:8090", "http://validator-2:8090"},
            true,
            true
        );
        when(context.clusterService.getConfig()).thenReturn(context.aeronConfig);
        return context;
    }

    private static AeronClusterConfig aeronConfig(String selfUrl,
                                                  String[] peerUrls,
                                                  boolean observeElections,
                                                  boolean logClusterStateDetails) {
        return (AeronClusterConfig) Proxy.newProxyInstance(
            AeronClusterConfig.class.getClassLoader(),
            new Class<?>[] {AeronClusterConfig.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "selfUrl":
                        return selfUrl;
                    case "peerUrls":
                        return peerUrls;
                    case "observeElections":
                        return observeElections;
                    case "logClusterStateDetails":
                        return logClusterStateDetails;
                    case "annotationType":
                        return AeronClusterConfig.class;
                    default:
                        return method.getDefaultValue();
                }
            }
        );
    }

    private static final class TestContext {
        private ValidatorBootstrap bootstrap;
        private FileStore fileStore;
        private NodeStore nodeStore;
        private SegmentHttpServer httpServer;
        private EthereumWallet wallet;
        private BlobStore blobStore;
        private AeronClusterService clusterService;
        private GlobalStoreServerComponentFactory componentFactory;
        private AeronClusterConfig aeronConfig;
        private AeronClusterService recordedService;
        private AeronClusterLauncher recordedLauncher;

        private StandbyModeStartupCoordinator.StartupContext startupContext(String bootstrapPrimaryHost,
                                                                           int bootstrapPrimaryPort) {
            return startupContext(bootstrapPrimaryHost, bootstrapPrimaryPort, clusterService);
        }

        private StandbyModeStartupCoordinator.StartupContext startupContext(String bootstrapPrimaryHost,
                                                                           int bootstrapPrimaryPort,
                                                                           AeronClusterService existingService) {
            return new StandbyModeStartupCoordinator.StartupContext(
                8090,
                "/tmp/test-store",
                bootstrapPrimaryHost,
                bootstrapPrimaryPort,
                aeronConfig,
                bootstrap,
                fileStore,
                nodeStore,
                httpServer,
                wallet,
                blobStore,
                existingService,
                componentFactory,
                deferredStartup -> {
                    recordedService = deferredStartup.getAeronClusterService();
                    recordedLauncher = deferredStartup.getStartupResult().getLauncher();
                }
            );
        }
    }
}
