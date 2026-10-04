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
import java.util.List;
import java.util.function.Consumer;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;

final class AeronClusterContextFactory {

    /**
     * Version of the replicated log format, written into every new leadership term event and snapshot and
     * checked by Aeron's default validator, which requires the same MAJOR on the consensus module and the
     * service container (a mismatch terminates the member instead of replaying the log differently).
     * <p>
     * Bump MAJOR on any change to message encoding, message templates, apply semantics, or the inputs that
     * make apply deterministic (cluster time, term, timers, genesis); bump MINOR or PATCH otherwise.
     * Version 2 covers the codec, cluster timestamps, batch DELETE routing, term from the log, the applied-log
     * watermark, the durability tally, transaction timers, deterministic genesis and contentCount decrement.
     */
    static final int APP_VERSION = SemanticVersion.compose(2, 0, 0);

    private static final String MAX_CONCURRENT_SESSIONS_PROPERTY = "oak.cluster.max.concurrent.sessions";

    static LaunchContexts create(int nodeId,
                                 File baseDir,
                                 ClusteredService clusteredService,
                                 String aeronDirName,
                                 String myIPAddress,
                                 List<String> ipAddresses,
                                 ShutdownSignalBarrier barrier,
                                 Consumer<String> onTermination,
                                 int socketSndbufLength,
                                 int socketRcvbufLength,
                                 int publicationTermBufferLength,
                                 int driverTimeoutMs,
                                 int clusterTermLengthBytes,
                                 AeronClusterLauncher.SessionTimeoutConfig sessionTimeoutConfig,
                                 ErrorHandler mediaDriverErrorHandler,
                                 ErrorHandler consensusModuleErrorHandler,
                                 ErrorHandler clusteredServiceErrorHandler) {
        int clusterBasePort = AeronClusterTopology.getPortBase();
        MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirName)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .socketSndbufLength(socketSndbufLength)
                .socketRcvbufLength(socketRcvbufLength)
                .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
                .terminationHook(barrier::signal)
                .errorHandler(mediaDriverErrorHandler)
                .publicationTermBufferLength(publicationTermBufferLength)
                .conductorIdleStrategy(new org.agrona.concurrent.BackoffIdleStrategy(100, 100, 1000, 1000000))
                .driverTimeoutMs(driverTimeoutMs);

        AeronArchive.Context replicationArchiveContext = new AeronArchive.Context()
                .controlResponseChannel("aeron:udp?endpoint=" + myIPAddress + ":0");

        Archive.Context archiveContext = new Archive.Context()
                .aeronDirectoryName(aeronDirName)
                .archiveDir(new File(baseDir, "archive"))
                .controlChannel(AeronClusterTopology.archiveControlChannel(clusterBasePort, nodeId, myIPAddress, clusterTermLengthBytes))
                .replicationChannel(AeronClusterTopology.replicationChannel(myIPAddress))
                .archiveClientContext(replicationArchiveContext)
                .localControlChannel("aeron:ipc?term-length=64k")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED);

        AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
                .lock(NoOpLock.INSTANCE)
                .controlRequestChannel(archiveContext.localControlChannel())
                .controlResponseChannel(archiveContext.localControlChannel())
                .aeronDirectoryName(aeronDirName);

        ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
                .errorHandler(consensusModuleErrorHandler)
                .clusterMemberId(nodeId)
                .clusterMembers(AeronClusterTopology.clusterMembers(clusterBasePort, ipAddresses))
                .clusterDir(new File(baseDir, "cluster"))
                .ingressChannel("aeron:udp?term-length=" + clusterTermLengthBytes)
                .logChannel(AeronClusterTopology.consensusLogChannel(clusterBasePort, nodeId, myIPAddress, clusterTermLengthBytes))
                .replicationChannel(AeronClusterTopology.replicationChannel(myIPAddress))
                .sessionTimeoutNs(sessionTimeoutConfig.timeoutNs)
                .appVersion(APP_VERSION)
                .terminationHook(() -> onTermination.accept("Consensus Module"))
                .archiveContext(aeronArchiveContext.clone());

        int maxConcurrentSessions = Integer.getInteger(MAX_CONCURRENT_SESSIONS_PROPERTY, 0);
        if (maxConcurrentSessions > 0) {
            consensusModuleContext.maxConcurrentSessions(maxConcurrentSessions);
        }

        ClusteredServiceContainer.Context clusteredServiceContext =
                new ClusteredServiceContainer.Context()
                        .aeronDirectoryName(aeronDirName)
                        .archiveContext(aeronArchiveContext.clone())
                        .clusterDir(new File(baseDir, "cluster"))
                        .clusteredService(clusteredService)
                        .appVersion(APP_VERSION)
                        .terminationHook(() -> onTermination.accept("Clustered Service"))
                        .errorHandler(clusteredServiceErrorHandler);

        return new LaunchContexts(
            mediaDriverContext,
            archiveContext,
            consensusModuleContext,
            clusteredServiceContext
        );
    }

    static final class LaunchContexts {
        final MediaDriver.Context mediaDriverContext;
        final Archive.Context archiveContext;
        final ConsensusModule.Context consensusModuleContext;
        final ClusteredServiceContainer.Context clusteredServiceContext;

        LaunchContexts(MediaDriver.Context mediaDriverContext,
                       Archive.Context archiveContext,
                       ConsensusModule.Context consensusModuleContext,
                       ClusteredServiceContainer.Context clusteredServiceContext) {
            this.mediaDriverContext = mediaDriverContext;
            this.archiveContext = archiveContext;
            this.consensusModuleContext = consensusModuleContext;
            this.clusteredServiceContext = clusteredServiceContext;
        }

        LaunchContexts freshCopy() {
            return new LaunchContexts(
                mediaDriverContext.clone(),
                archiveContext.clone(),
                consensusModuleContext.clone(),
                clusteredServiceContext.clone()
            );
        }
    }
}
