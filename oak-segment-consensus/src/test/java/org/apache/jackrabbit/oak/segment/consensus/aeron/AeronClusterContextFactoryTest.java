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

import io.aeron.cluster.service.ClusteredService;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
                new AeronClusterLauncher.SessionTimeoutConfig(5, TimeUnit.MINUTES.toNanos(5), "test", "staging"),
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
            new AeronClusterLauncher.SessionTimeoutConfig(5, TimeUnit.MINUTES.toNanos(5), "test", "staging"),
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
            new AeronClusterLauncher.SessionTimeoutConfig(7, TimeUnit.MINUTES.toNanos(7), "test", "dev"),
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
            new AeronClusterLauncher.SessionTimeoutConfig(5, TimeUnit.MINUTES.toNanos(5), "test", "staging");
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

    private static void restorePortBase(String previous) {
        if (previous == null) {
            System.clearProperty(AeronClusterTopology.PORT_BASE_PROPERTY);
        } else {
            System.setProperty(AeronClusterTopology.PORT_BASE_PROPERTY, previous);
        }
    }
}
