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
package org.apache.jackrabbit.oak.segment.consensus.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

final class ProposalPersistenceStore {
    private static final Logger log = LoggerFactory.getLogger(ProposalPersistenceStore.class);

    private final Path storeFile;

    ProposalPersistenceStore(Path directory) {
        this.storeFile = directory.resolve("queued-proposals.bin");
        try {
            Files.createDirectories(directory);
        } catch (Exception e) {
            log.warn("Failed to create proposal persistence directory {}: {}", directory, e.getMessage());
        }
    }

    List<QueuedProposal> load() {
        if (!Files.exists(storeFile)) {
            return new ArrayList<>();
        }
        try (ObjectInputStream in = new ObjectInputStream(Files.newInputStream(storeFile))) {
            Object data = in.readObject();
            if (!(data instanceof List)) {
                return new ArrayList<>();
            }
            List<?> rawList = (List<?>) data;
            if (rawList.isEmpty()) {
                return new ArrayList<>();
            }
            Object first = rawList.get(0);
            if (first instanceof StoredProposal) {
                List<QueuedProposal> proposals = new ArrayList<>(rawList.size());
                for (Object item : rawList) {
                    if (item instanceof StoredProposal) {
                        proposals.add(((StoredProposal) item).toQueuedProposal());
                    }
                }
                return proposals;
            }
            if (first instanceof QueuedProposal) {
                @SuppressWarnings("unchecked")
                List<QueuedProposal> legacy = (List<QueuedProposal>) data;
                return legacy != null ? legacy : new ArrayList<QueuedProposal>();
            }
        } catch (Exception e) {
            log.warn("Failed to load persisted proposals: {}", e.getMessage());
        }
        return new ArrayList<>();
    }

    void save(Collection<QueuedProposal> proposals) {
        try {
            List<StoredProposal> manifest = new ArrayList<>();
            for (QueuedProposal proposal : proposals) {
                if (proposal != null) {
                    manifest.add(StoredProposal.from(proposal));
                }
            }
            Path tempFile = storeFile.resolveSibling(storeFile.getFileName() + ".tmp");
            try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(tempFile))) {
                out.writeObject(manifest);
            }
            Files.move(tempFile, storeFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            log.warn("Failed to persist proposals: {}", e.getMessage());
        }
    }

    private static final class StoredProposal implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final String proposalId;
        private final String ethereumTxHash;
        private final long timestamp;
        private final long timeoutTimestamp;
        private final ProposalState state;
        private final Long confirmedBlock;
        private final String rejectionReason;
        private final QueuedProposal.ProposalType type;
        private final String walletAddress;
        private final String path;
        private final String contentType;
        private final String signature;
        private final long epoch;
        private final Long observedEpoch;
        private final Long finalizedEpoch;
        private final String transactionId;
        private final String correlationId;
        private final org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier tier;
        private final String intentToken;
        private final String blobId;
        private final String mimeType;
        private final String ipfsCid;
        private final int retryCount;
        private final long lastRetryTimestamp;
        private final long verifiedTimestampMs;
        private final String payloadRef;
        private final long payloadSizeBytes;
        private final String payloadSha256;
        private final DurabilityState durabilityState;
        private final long durabilityTimestamp;
        private final String durabilityError;
        private final String durableHead;
        private boolean appendedToLog;

        private StoredProposal(String proposalId,
                               String ethereumTxHash,
                               long timestamp,
                               long timeoutTimestamp,
                               ProposalState state,
                               Long confirmedBlock,
                               String rejectionReason,
                               QueuedProposal.ProposalType type,
                               String walletAddress,
                               String path,
                               String contentType,
                               String signature,
                               long epoch,
                               Long observedEpoch,
                               Long finalizedEpoch,
                               String transactionId,
                               String correlationId,
                               org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier tier,
                               String intentToken,
                               String blobId,
                               String mimeType,
                               String ipfsCid,
                               int retryCount,
                               long lastRetryTimestamp,
                               long verifiedTimestampMs,
                               String payloadRef,
                               long payloadSizeBytes,
                               String payloadSha256,
                               DurabilityState durabilityState,
                               long durabilityTimestamp,
                               String durabilityError,
                               String durableHead) {
            this.proposalId = proposalId;
            this.ethereumTxHash = ethereumTxHash;
            this.timestamp = timestamp;
            this.timeoutTimestamp = timeoutTimestamp;
            this.state = state;
            this.confirmedBlock = confirmedBlock;
            this.rejectionReason = rejectionReason;
            this.type = type;
            this.walletAddress = walletAddress;
            this.path = path;
            this.contentType = contentType;
            this.signature = signature;
            this.epoch = epoch;
            this.observedEpoch = observedEpoch;
            this.finalizedEpoch = finalizedEpoch;
            this.transactionId = transactionId;
            this.correlationId = correlationId;
            this.tier = tier;
            this.intentToken = intentToken;
            this.blobId = blobId;
            this.mimeType = mimeType;
            this.ipfsCid = ipfsCid;
            this.retryCount = retryCount;
            this.lastRetryTimestamp = lastRetryTimestamp;
            this.verifiedTimestampMs = verifiedTimestampMs;
            this.payloadRef = payloadRef;
            this.payloadSizeBytes = payloadSizeBytes;
            this.payloadSha256 = payloadSha256;
            this.durabilityState = durabilityState;
            this.durabilityTimestamp = durabilityTimestamp;
            this.durabilityError = durabilityError;
            this.durableHead = durableHead;
        }

        static StoredProposal from(QueuedProposal proposal) {
            StoredProposal stored = new StoredProposal(
                proposal.getProposalId(),
                proposal.getEthereumTxHash(),
                proposal.getTimestamp(),
                proposal.getTimeoutTimestamp(),
                proposal.getState(),
                proposal.getConfirmedBlock(),
                proposal.getRejectionReason(),
                proposal.getType(),
                proposal.getWalletAddress(),
                proposal.getPath(),
                proposal.getContentType(),
                proposal.getSignature(),
                proposal.getEpoch(),
                proposal.getObservedEpoch(),
                proposal.getFinalizedEpoch(),
                proposal.getTransactionId(),
                proposal.getCorrelationId(),
                proposal.getTier(),
                proposal.getIntentToken(),
                proposal.getBlobId(),
                proposal.getMimeType(),
                proposal.getIpfsCid(),
                proposal.getRetryCount(),
                proposal.getLastRetryTimestamp(),
                proposal.getVerifiedTimestampMs(),
                proposal.getPayloadRef(),
                proposal.getPayloadSizeBytes(),
                proposal.getPayloadSha256(),
                proposal.getDurabilityState(),
                proposal.getDurabilityTimestamp(),
                proposal.getDurabilityError(),
                proposal.getDurableHead()
            );
            stored.appendedToLog = proposal.isAppendedToLog();
            return stored;
        }

        QueuedProposal toQueuedProposal() {
            QueuedProposal proposal = new QueuedProposal(
                proposalId,
                ethereumTxHash,
                null,
                timestamp,
                timeoutTimestamp,
                state
            );
            proposal.setConfirmedBlock(confirmedBlock);
            proposal.setRejectionReason(rejectionReason);
            proposal.setType(type != null ? type : QueuedProposal.ProposalType.WRITE);
            proposal.setWalletAddress(walletAddress);
            proposal.setPath(path);
            proposal.setContentType(contentType);
            proposal.setSignature(signature);
            proposal.setEpoch(epoch);
            proposal.setObservedEpoch(observedEpoch);
            proposal.setFinalizedEpoch(finalizedEpoch);
            proposal.setTransactionId(transactionId);
            proposal.setCorrelationId(correlationId);
            proposal.setTier(tier);
            proposal.setIntentToken(intentToken);
            proposal.setBlobId(blobId);
            proposal.setMimeType(mimeType);
            proposal.setIpfsCid(ipfsCid);
            proposal.setPayloadRef(payloadRef);
            proposal.setPayloadSizeBytes(payloadSizeBytes);
            proposal.setPayloadSha256(payloadSha256);
            proposal.setVerifiedTimestampMs(verifiedTimestampMs);
            proposal.restoreDurabilityState(
                durabilityState != null ? durabilityState : DurabilityState.PENDING,
                durabilityTimestamp,
                durableHead,
                durabilityError
            );
            proposal.restoreRetryState(retryCount, lastRetryTimestamp);
            if (appendedToLog) {
                proposal.markAppendedToLog();
            }
            return proposal;
        }
    }
}
