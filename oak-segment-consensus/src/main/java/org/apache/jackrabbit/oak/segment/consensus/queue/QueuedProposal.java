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

import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;

/**
 * Queued proposal waiting for Ethereum confirmation.
 */
public class QueuedProposal implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    /** Proposal type (WRITE or DELETE) */
    public enum ProposalType {
        WRITE,
        DELETE
    }

    private final String proposalId;
    private final String ethereumTxHash;
    private final long timestamp;
    private volatile long timeoutTimestamp;
    private volatile ProposalState state;
    private volatile Long confirmedBlock;
    private volatile String rejectionReason;

    // Wallet-based write/delete fields
    private volatile ProposalType type = ProposalType.WRITE; // Default to WRITE for backward compatibility
    private volatile String walletAddress;
    private volatile String path;
    private volatile String contentType;
    private volatile String message;
    private volatile String payloadRef;
    private volatile long payloadSizeBytes = 0L;
    private volatile String payloadSha256;
    private volatile String signature;
    private volatile long epoch; // Compatibility overlay epoch for queue/reporting surfaces
    private volatile Long observedEpoch;
    private volatile Long finalizedEpoch;
    private volatile String transactionId;
    private volatile String correlationId;
    private volatile org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier tier =
        org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier.STANDARD; // Payment tier for priority handling
    private volatile String intentToken; // Intent token for lazy binary upload (ADR 020)
    private volatile String blobId; // Oak blob ID for eager binary upload
    private volatile String mimeType; // MIME type for binary
    private volatile String ipfsCid; // IPFS CID from client-side upload (ADR 016)
    private volatile int retryCount = 0; // Number of times this proposal has been retried
    private volatile long lastRetryTimestamp = 0; // Timestamp of last retry attempt
    private volatile long verifiedTimestampMs = 0; // Timestamp when proposal entered verified release scheduling
    private volatile boolean appendedToLog; // Set when a PROCESSED proposal is re-queued for replay

    // Durability tracking (ADR 026)
    private volatile DurabilityState durabilityState = DurabilityState.PENDING;
    private volatile long durabilityTimestamp = 0;
    private volatile String durabilityError;
    private volatile String durableHead;

    public QueuedProposal(
            String proposalId,
            String ethereumTxHash,
            Object unused, // For compatibility, not used
            long timestamp,
            long timeoutTimestamp,
            ProposalState state) {
        this.proposalId = proposalId;
        this.ethereumTxHash = ethereumTxHash;
        this.timestamp = timestamp;
        this.timeoutTimestamp = timeoutTimestamp;
        this.state = state;
    }

    public String getProposalId() {
        return proposalId;
    }

    public String getEthereumTxHash() {
        return ethereumTxHash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getWalletAddress() {
        return walletAddress;
    }

    public void setWalletAddress(String walletAddress) {
        this.walletAddress = walletAddress;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void clearMessage() {
        this.message = null;
    }

    public String getPayloadRef() {
        return payloadRef;
    }

    public void setPayloadRef(String payloadRef) {
        this.payloadRef = payloadRef;
    }

    public long getPayloadSizeBytes() {
        return payloadSizeBytes;
    }

    public void setPayloadSizeBytes(long payloadSizeBytes) {
        this.payloadSizeBytes = payloadSizeBytes;
    }

    public String getPayloadSha256() {
        return payloadSha256;
    }

    public void setPayloadSha256(String payloadSha256) {
        this.payloadSha256 = payloadSha256;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public DurabilityState getDurabilityState() {
        return durabilityState;
    }

    public long getDurabilityTimestamp() {
        return durabilityTimestamp;
    }

    public String getDurabilityError() {
        return durabilityError;
    }

    public String getDurableHead() {
        return durableHead;
    }

    public void setDurabilityState(DurabilityState durabilityState, String durableHead, String durabilityError) {
        this.durabilityState = durabilityState;
        this.durableHead = durableHead;
        this.durabilityError = durabilityError;
        this.durabilityTimestamp = System.currentTimeMillis();
    }

    void restoreDurabilityState(DurabilityState durabilityState,
                                long durabilityTimestamp,
                                String durableHead,
                                String durabilityError) {
        this.durabilityState = durabilityState != null ? durabilityState : DurabilityState.PENDING;
        this.durableHead = durableHead;
        this.durabilityError = durabilityError;
        this.durabilityTimestamp = durabilityTimestamp;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public Long getFinalizedEpoch() {
        return finalizedEpoch;
    }

    public void setFinalizedEpoch(Long finalizedEpoch) {
        this.finalizedEpoch = finalizedEpoch;
    }

    public Long getObservedEpoch() {
        return observedEpoch;
    }

    public void setObservedEpoch(Long observedEpoch) {
        this.observedEpoch = observedEpoch;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public long getTimeoutTimestamp() {
        return timeoutTimestamp;
    }

    public void overrideTimeoutTimestamp(long timeoutTimestamp) {
        this.timeoutTimestamp = timeoutTimestamp;
    }

    public ProposalState getState() {
        return state;
    }

    public void setState(ProposalState state) {
        this.state = state;
    }

    public Long getConfirmedBlock() {
        return confirmedBlock;
    }

    public void setConfirmedBlock(Long confirmedBlock) {
        this.confirmedBlock = confirmedBlock;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }

    public void setRejectionReason(String rejectionReason) {
        this.rejectionReason = rejectionReason;
    }

    public org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier getTier() {
        return tier;
    }

    public void setTier(org.apache.jackrabbit.oak.segment.consensus.economics.ValidatorEarningsTracker.PaymentTier tier) {
        this.tier = tier;
    }

    public ProposalType getType() {
        return type;
    }

    public void setType(ProposalType type) {
        this.type = type;
    }

    public String getIntentToken() {
        return intentToken;
    }

    public void setIntentToken(String intentToken) {
        this.intentToken = intentToken;
    }

    public String getBlobId() {
        return blobId;
    }

    public void setBlobId(String blobId) {
        this.blobId = blobId;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public MutationAuditMetadata toAuditMetadata() {
        return new MutationAuditMetadata(
            type == ProposalType.DELETE ? MutationAuditMetadata.Operation.DELETE : MutationAuditMetadata.Operation.WRITE,
            transactionId,
            correlationId,
            proposalId,
            ethereumTxHash,
            confirmedBlock,
            observedEpoch,
            finalizedEpoch
        );
    }

    /**
     * Get the IPFS CID from client-side upload (ADR 016).
     *
     * @return IPFS CID string, or null if not provided
     */
    public String getIpfsCid() {
        return ipfsCid;
    }

    /**
     * Set the IPFS CID from client-side upload (ADR 016).
     *
     * @param ipfsCid IPFS CID string
     */
    public void setIpfsCid(String ipfsCid) {
        this.ipfsCid = ipfsCid;
    }

    /**
     * Get the number of times this proposal has been retried.
     *
     * @return retry count (0 = first attempt)
     */
    public int getRetryCount() {
        return retryCount;
    }

    public long getVerifiedTimestampMs() {
        return verifiedTimestampMs;
    }

    public void setVerifiedTimestampMs(long verifiedTimestampMs) {
        this.verifiedTimestampMs = verifiedTimestampMs;
    }

    /**
     * Increment the retry count and update the last retry timestamp.
     *
     * @return the new retry count
     */
    public int incrementRetryCount() {
        this.lastRetryTimestamp = System.currentTimeMillis();
        return ++this.retryCount;
    }

    /**
     * Get the timestamp of the last retry attempt.
     *
     * @return timestamp in milliseconds, or 0 if never retried
     */
    public long getLastRetryTimestamp() {
        return lastRetryTimestamp;
    }

    boolean isAppendedToLog() {
        return appendedToLog;
    }

    void markAppendedToLog() {
        this.appendedToLog = true;
    }

    void restoreRetryState(int retryCount, long lastRetryTimestamp) {
        this.retryCount = Math.max(0, retryCount);
        this.lastRetryTimestamp = Math.max(0L, lastRetryTimestamp);
    }
}
