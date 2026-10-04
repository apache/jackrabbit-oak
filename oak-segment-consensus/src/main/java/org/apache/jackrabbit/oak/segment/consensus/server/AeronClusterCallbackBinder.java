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

import org.agrona.concurrent.AgentTerminationException;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AeronClusterCallbackBinder {

    private static final Logger log = LoggerFactory.getLogger(AeronClusterCallbackBinder.class);

    void bind(AeronConsensusEngine aeronEngine, SegmentHttpServer httpServer) {
        aeronEngine.setWriteApplicationCallback(new AeronConsensusEngine.WriteApplicationCallback() {
            @Override
            public void applyReplicatedWrite(String walletAddress, String path, String contentType, String message,
                                             String signature, String intentToken, String blobId, String mimeType, String ipfsCid,
                                             MutationAuditMetadata auditMetadata) {
                httpServer.getConsensusApiHandler().applyReplicatedWriteWithAuditMetadata(
                    walletAddress, path, contentType, message, signature, intentToken, blobId, mimeType, ipfsCid,
                    auditMetadata
                );
            }

            @Override
            public void applyReplicatedDelete(String walletAddress, String path, String signature,
                                              MutationAuditMetadata auditMetadata) {
                httpServer.getConsensusApiHandler().applyReplicatedDeleteWithAuditMetadata(
                    walletAddress, path, signature, auditMetadata
                );
            }
        });
        log.info("   ✅ Write application callback configured");

        aeronEngine.setGCCallback(new AeronConsensusEngine.GCApplicationCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC, long clusterTime) {
                GCProposalManager manager = httpServer.getContext().gcProposalManager;
                if (manager == null) {
                    log.warn("⚠️  GC proposal manager not initialized - cannot apply replicated GC proposal");
                    return;
                }
                manager.applyReplicatedProposal(
                    proposalId,
                    proposerWallet,
                    targetRevision,
                    estimatedReclaimableSizeMB,
                    estimatedCostUSDC,
                    clusterTime
                );
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason, long clusterTime) {
                GCProposalManager manager = httpServer.getContext().gcProposalManager;
                if (manager == null) {
                    log.warn("⚠️  GC proposal manager not initialized - cannot apply replicated GC vote");
                    return;
                }
                manager.voteOnProposal(proposalId, validatorId, approve, reason != null ? reason : "", clusterTime);
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
                GCProposalManager manager = httpServer.getContext().gcProposalManager;
                if (manager == null) {
                    log.warn("⚠️  GC proposal manager not initialized - cannot apply replicated GC execute");
                    return;
                }
                manager.applyReplicatedExecute(proposalId, executorId);
            }
        });
        log.info("   ✅ GC application callback configured");
    }
}
