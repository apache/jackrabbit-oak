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

import org.apache.jackrabbit.oak.segment.consensus.util.SegmentReplicator;

/**
 * Factory for Aeron engine components.
 *
 * <p>Centralizes construction to keep AeronConsensusEngine free of hardcoded
 * instantiations while keeping wiring consistent.</p>
 */
final class AeronEngineComponentFactory {

    private AeronEngineComponentFactory() {
        // utility
    }

    static AeronMessageCodec createMessageCodec() {
        return new AeronMessageCodec();
    }

    static SegmentReplicator createSegmentReplicator(org.apache.jackrabbit.oak.segment.file.FileStore fileStore) {
        return new SegmentReplicator(fileStore);
    }

    static org.apache.jackrabbit.oak.segment.consensus.queue.BackpressureManager createBackpressureManager() {
        return new org.apache.jackrabbit.oak.segment.consensus.queue.BackpressureManager();
    }

    static SnapshotService createSnapshotService() {
        return new SnapshotService();
    }

    static LeaderDiscoveryService createLeaderDiscoveryService(java.util.Map<Integer, String> nodeIdToUrl,
                                                               java.util.List<String> peerUrls,
                                                               String selfUrl) {
        LeaderDiscoveryService service = new LeaderDiscoveryService(nodeIdToUrl, peerUrls);
        service.setSelfUrl(selfUrl);
        return service;
    }

    static MessageDispatcher createMessageDispatcher(MessageDispatcher.WriteCallback writeCallback) {
        return new MessageDispatcher(writeCallback);
    }

    static HeadStateService createHeadStateService(org.apache.jackrabbit.oak.segment.file.FileStore fileStore) {
        return new HeadStateService(fileStore);
    }

    static AeronIngressHandler createIngressHandler(AeronMessageCodec codec,
                                                    MessageDispatcher dispatcher,
                                                    Runnable heartbeatCallback,
                                                    java.util.function.BiConsumer<String,
                                                        org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition> genesisCallback) {
        AeronIngressHandler handler = new AeronIngressHandler(codec, dispatcher);
        handler.setHeartbeatCallback(heartbeatCallback);
        handler.setGenesisCallback(genesisCallback);
        return handler;
    }

    static AeronSessionManager createSessionManager(Runnable heartbeatCallback,
                                                    java.util.function.Consumer<String> reconnectCallback) {
        return new AeronSessionManager(heartbeatCallback, reconnectCallback);
    }

    static AeronHealthService createHealthService() {
        return new AeronHealthService();
    }

    static AeronLeaderTracker createLeaderTracker(LeaderDiscoveryService leaderDiscoveryService) {
        return new AeronLeaderTracker(leaderDiscoveryService);
    }

    static AeronIngressWritePayloadBuilder createIngressWritePayloadBuilder() {
        return new AeronIngressWritePayloadBuilder();
    }

    static AeronIngressControlPayloadBuilder createIngressControlPayloadBuilder() {
        return new AeronIngressControlPayloadBuilder();
    }

    static AeronInternalClusterClientConnector createInternalClusterClientConnector() {
        return new AeronInternalClusterClientConnector();
    }
}
