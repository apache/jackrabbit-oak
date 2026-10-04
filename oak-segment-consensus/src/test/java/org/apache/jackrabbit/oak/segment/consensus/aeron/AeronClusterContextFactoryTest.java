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

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.driver.Configuration;
import io.aeron.driver.MaxMulticastFlowControl;
import io.aeron.driver.media.UdpChannel;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class AeronClusterContextFactoryTest {

    @Test
    public void createUsesConfiguredClusterBasePort() {
        String previous = System.getProperty(AeronClusterTopology.PORT_BASE_PROPERTY);
        System.setProperty(AeronClusterTopology.PORT_BASE_PROPERTY, "9400");
        try {
            ClusteredService clusteredService = mock(ClusteredService.class);
            AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
                1,
                new File("target/aeron-context-factory"),
                clusteredService,
                "aeron-test-dir",
                "172.20.1.8",
                Arrays.asList("172.20.1.5", "peer-1", "172.20.1.8"),
                mock(ShutdownSignalBarrier.class),
                component -> { },
                16384,
                16384,
                65536,
                60000,
                524288,
                new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(5), "test"),
                throwable -> { },
                throwable -> { },
                throwable -> { }
            );

            assertEquals(
                "1,peer-1:9502,peer-1:9503,peer-1:9504,peer-1:9505,peer-1:9501",
                contexts.consensusModuleContext.clusterMembers().split("\\|")[1]
            );
            assertEquals(
                AeronClusterTopology.consensusLogChannel(9400, 1, "172.20.1.8", 524288),
                contexts.consensusModuleContext.logChannel()
            );
            assertEquals(
                AeronClusterTopology.archiveControlChannel(9400, 1, "172.20.1.8", 524288),
                contexts.archiveContext.controlChannel()
            );
        } finally {
            restorePortBase(previous);
        }
    }

    @Test
    public void terminationHooksOfConsensusModuleAndServiceContainerRunTheShutdownPath() {
        java.util.List<String> terminated = new java.util.ArrayList<>();
        AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
            1,
            new File("target/aeron-context-factory"),
            mock(ClusteredService.class),
            "aeron-test-dir",
            "172.20.1.8",
            Arrays.asList("172.20.1.5", "172.20.1.6", "172.20.1.8"),
            mock(ShutdownSignalBarrier.class),
            terminated::add,
            16384,
            16384,
            65536,
            60000,
            524288,
            new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(5), "test"),
            throwable -> { },
            throwable -> { },
            throwable -> { }
        );

        contexts.consensusModuleContext.terminationHook().run();
        contexts.clusteredServiceContext.terminationHook().run();
        contexts.freshCopy().consensusModuleContext.terminationHook().run();

        assertEquals(Arrays.asList("Consensus Module", "Clustered Service", "Consensus Module"), terminated);
    }

    @Test
    public void createBuildsDriverAndArchiveContextsFromInputs() {
        ClusteredService clusteredService = mock(ClusteredService.class);
        AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
            2,
            new File("target/aeron-context-factory"),
            clusteredService,
            "aeron-test-dir",
            "172.20.1.7",
            Arrays.asList("172.20.1.5", "peer-1", "172.20.1.7"),
            mock(ShutdownSignalBarrier.class),
            component -> { },
            32768,
            65536,
            131072,
            45000,
            262144,
            new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(7), "test"),
            throwable -> { },
            throwable -> { },
            throwable -> { }
        );

        assertEquals("aeron-test-dir", contexts.mediaDriverContext.aeronDirectoryName());
        assertEquals(32768, contexts.mediaDriverContext.socketSndbufLength());
        assertEquals(65536, contexts.mediaDriverContext.socketRcvbufLength());
        assertEquals(131072, contexts.mediaDriverContext.publicationTermBufferLength());
        assertEquals(45000L, contexts.mediaDriverContext.driverTimeoutMs());

        assertEquals(new File("target/aeron-context-factory/archive"), contexts.archiveContext.archiveDir());
        assertEquals(
            AeronClusterTopology.archiveControlChannel(2, "172.20.1.7", 262144),
            contexts.archiveContext.controlChannel()
        );
        assertEquals(
            AeronClusterTopology.replicationChannel("172.20.1.7"),
            contexts.archiveContext.replicationChannel()
        );
        assertEquals("aeron:ipc?term-length=64k", contexts.archiveContext.localControlChannel());
        assertEquals(
            "aeron:udp?endpoint=172.20.1.7:0",
            contexts.archiveContext.archiveClientContext().controlResponseChannel()
        );
    }

    @Test
    public void createBuildsConsensusAndServiceContextsFromInputs() {
        ClusteredService clusteredService = mock(ClusteredService.class);
        AeronClusterLauncher.SessionTimeoutConfig sessionTimeoutConfig =
            new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(5), "test");
        AeronClusterContextFactory.LaunchContexts contexts = AeronClusterContextFactory.create(
            1,
            new File("target/aeron-context-factory"),
            clusteredService,
            "aeron-test-dir",
            "172.20.1.8",
            Arrays.asList("172.20.1.5", "peer-1", "172.20.1.8"),
            mock(ShutdownSignalBarrier.class),
            component -> { },
            16384,
            16384,
            65536,
            60000,
            524288,
            sessionTimeoutConfig,
            throwable -> { },
            throwable -> { },
            throwable -> { }
        );

        assertEquals(1, contexts.consensusModuleContext.clusterMemberId());
        assertEquals(
            AeronClusterTopology.clusterMembers(Arrays.asList("172.20.1.5", "peer-1", "172.20.1.8")),
            contexts.consensusModuleContext.clusterMembers()
        );
        assertEquals("aeron:udp?term-length=524288", contexts.consensusModuleContext.ingressChannel());
        assertEquals(
            AeronClusterTopology.consensusLogChannel(1, "172.20.1.8", 524288),
            contexts.consensusModuleContext.logChannel()
        );
        assertEquals(
            AeronClusterTopology.replicationChannel("172.20.1.8"),
            contexts.consensusModuleContext.replicationChannel()
        );
        assertEquals(sessionTimeoutConfig.timeoutNs, contexts.consensusModuleContext.sessionTimeoutNs());

        assertEquals("aeron-test-dir", contexts.clusteredServiceContext.aeronDirectoryName());
        assertEquals(new File("target/aeron-context-factory/cluster"), contexts.clusteredServiceContext.clusterDir());
        assertSame(clusteredService, contexts.clusteredServiceContext.clusteredService());
        assertEquals("aeron:ipc?term-length=64k", contexts.clusteredServiceContext.archiveContext().controlRequestChannel());
        assertEquals("aeron:ipc?term-length=64k", contexts.clusteredServiceContext.archiveContext().controlResponseChannel());
    }

    @Test
    public void bothContextsCarryTheLogFormatAppVersionAndKeepAeronsMajorVersionValidator() {
        AeronClusterContextFactory.LaunchContexts contexts = createDefault().freshCopy();

        int appVersion = AeronClusterContextFactory.APP_VERSION;
        assertEquals(2, SemanticVersion.major(appVersion));
        assertEquals(appVersion, contexts.consensusModuleContext.appVersion());
        assertEquals(appVersion, contexts.clusteredServiceContext.appVersion());
        // null until conclude(), which installs AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR
        assertNull(contexts.consensusModuleContext.appVersionValidator());
        assertNull(contexts.clusteredServiceContext.appVersionValidator());

        AppVersionValidator validator = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
        assertTrue(validator.isVersionCompatible(appVersion, SemanticVersion.compose(2, 7, 3)));
        assertFalse(validator.isVersionCompatible(appVersion, SemanticVersion.compose(1, 0, 0)));
        assertFalse(validator.isVersionCompatible(appVersion, SemanticVersion.compose(3, 0, 0)));
        assertFalse("a log written with Aeron's default appVersion must be rejected",
            validator.isVersionCompatible(appVersion, SemanticVersion.compose(0, 0, 1)));
    }

    @Test
    public void logChannelUsesAeronsDefaultMaxMulticastFlowControl() {
        AeronClusterContextFactory.LaunchContexts contexts = createDefault().freshCopy();

        // null until conclude(), which installs Configuration.multicastFlowControlSupplier()
        assertNull(contexts.mediaDriverContext.multicastFlowControlSupplier());
        // the MDC log channel has no fc= parameter, so the default supplier picks Max for it
        UdpChannel logChannel = UdpChannel.parse(contexts.consensusModuleContext.logChannel());
        assertTrue(logChannel.isMultiDestination());
        assertTrue(Configuration.multicastFlowControlSupplier().newInstance(logChannel, 100, 1L)
            instanceof MaxMulticastFlowControl);
    }

    private static AeronClusterContextFactory.LaunchContexts createDefault() {
        return AeronClusterContextFactory.create(
            1,
            new File("target/aeron-context-factory"),
            mock(ClusteredService.class),
            "aeron-test-dir",
            "172.20.1.8",
            Arrays.asList("172.20.1.5", "172.20.1.6", "172.20.1.8"),
            mock(ShutdownSignalBarrier.class),
            component -> { },
            16384,
            16384,
            65536,
            60000,
            524288,
            new AeronClusterLauncher.SessionTimeoutConfig(TimeUnit.MINUTES.toNanos(5), "test"),
            throwable -> { },
            throwable -> { },
            throwable -> { }
        );
    }

    private static void restorePortBase(String previous) {
        if (previous == null) {
            System.clearProperty(AeronClusterTopology.PORT_BASE_PROPERTY);
        } else {
            System.setProperty(AeronClusterTopology.PORT_BASE_PROPERTY, previous);
        }
    }
}
