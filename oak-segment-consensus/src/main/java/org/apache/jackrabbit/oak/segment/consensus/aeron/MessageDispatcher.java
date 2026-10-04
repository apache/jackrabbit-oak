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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.consensus.service.InvalidProposalException;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonParser;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Service responsible for dispatching incoming Aeron messages to appropriate handlers.
 * 
 * <p>Extracted from AeronConsensusEngine to reduce complexity and improve testability.
 * This service handles message parsing, validation, and routing based on SBE template IDs.
 * 
 * <p><strong>OSGi Component:</strong> Stateless service that can be injected via @Reference.
 * Lifecycle managed by OSGi framework. No configuration required (stateless).
 * 
 * <p><strong>Responsibilities:</strong>
 * <ul>
 *   <li>Parse SBE-encoded messages from DirectBuffer</li>
 *   <li>Validate message structure and required fields</li>
 *   <li>Route messages to appropriate callbacks</li>
 *   <li>Handle HEAD broadcasts, writes, deletes, and batches</li>
 * </ul>
 */
@Component(
    service = MessageDispatcher.class,
    immediate = true,
    configurationPolicy = ConfigurationPolicy.OPTIONAL,
    property = {
        "service.description=Aeron Message Dispatcher",
        "service.vendor=Apache Software Foundation"
    }
)
public class MessageDispatcher {
    
    private static final Logger log = LoggerFactory.getLogger(MessageDispatcher.class);
    
    /**
     * Callback interface for write operations.
     */
    public interface WriteCallback {
        default void applyWrite(String walletAddress, String path, String contentType,
                                String message, String signature, String intentToken,
                                String blobId, String mimeType, String ipfsCid,
                                MutationAuditMetadata auditMetadata) {
            applyWrite(
                walletAddress,
                path,
                contentType,
                message,
                signature,
                intentToken,
                blobId,
                mimeType,
                ipfsCid,
                auditMetadata != null ? auditMetadata.getProposalId() : null
            );
        }

        default void applyWrite(String walletAddress, String path, String contentType,
                                String message, String signature, String intentToken,
                                String blobId, String mimeType, String ipfsCid, String proposalId) {
            throw new UnsupportedOperationException("Write callback must implement applyWrite");
        }

        default void applyDelete(String walletAddress, String path, String signature,
                                 MutationAuditMetadata auditMetadata) {
            applyDelete(walletAddress, path, signature, auditMetadata != null ? auditMetadata.getProposalId() : null);
        }

        default void applyDelete(String walletAddress, String path, String signature, String proposalId) {
            throw new UnsupportedOperationException("Write callback must implement applyDelete");
        }
    }
    
    /**
     * Callback interface for GC operations.
     */
    public interface GCCallback {
        void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                           long estimatedReclaimableSizeMB, String estimatedCostUSDC);
        void applyGCVote(String proposalId, int validatorId, boolean approve, String reason);
        void applyGCExecute(String proposalId, int executorId);
    }

    /**
     * Callback interface for per-member durability reports (ADR 026).
     */
    public interface DurabilityCallback {
        /**
         * @param clusterTime timestamp of the log entry
         */
        void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success, String error,
                                long clusterTime);
    }

    /**
     * Callback interface for explicit transaction boundary messages.
     */
    public interface TransactionCallback {
        void onStartTransaction(String transactionId, String correlationId, long timeoutMs, String initiatorWallet);
        void onCommitTransaction(String transactionId, String correlationId);
        void onAbortTransaction(String transactionId, String correlationId, String reason);
    }
    
    private WriteCallback writeCallback;
    private GCCallback gcCallback;
    private DurabilityCallback durabilityCallback;
    private TransactionCallback transactionCallback;
    private LongSupplier termProvider;
    private AppliedLogPosition replayFloor = AppliedLogPosition.NONE;
    private static final long STALE_TERM_LOG_INTERVAL_MS = 5000;
    private final AtomicLong lastStaleTermLogMs = new AtomicLong(0);
    private final AtomicInteger staleTermSuppressed = new AtomicInteger(0);
    
    /**
     * Create a new message dispatcher (default constructor for OSGi).
     */
    public MessageDispatcher() {
        this.writeCallback = null;
        this.gcCallback = null;
        this.durabilityCallback = null;
        this.transactionCallback = null;
    }
    
    /**
     * Create a new message dispatcher with callbacks (for programmatic use).
     * 
     * @param writeCallback callback for write/delete operations
     */
    public MessageDispatcher(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
        this.gcCallback = null;
        this.durabilityCallback = null;
        this.transactionCallback = null;
    }
    
    /**
     * OSGi lifecycle: Activate component.
     */
    @Activate
    protected void activate() {
        log.info("✅ MessageDispatcher activated (OSGi)");
    }
    
    /**
     * OSGi lifecycle: Deactivate component.
     */
    @Deactivate
    protected void deactivate() {
        log.info("✅ MessageDispatcher deactivated (OSGi)");
    }
    
    /**
     * Set callbacks (for OSGi injection).
     */
    public void setCallbacks(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
        log.info("✅ MessageDispatcher callbacks set");
    }
    
    /**
     * Set GC callback (for OSGi injection or programmatic use).
     */
    public void setGCCallback(GCCallback gcCallback) {
        this.gcCallback = gcCallback;
        log.info("✅ MessageDispatcher GC callback set");
    }

    public void setDurabilityCallback(DurabilityCallback durabilityCallback) {
        this.durabilityCallback = durabilityCallback;
        log.info("✅ MessageDispatcher durability callback set");
    }

    public void setTransactionCallback(TransactionCallback transactionCallback) {
        this.transactionCallback = transactionCallback;
        log.info("✅ MessageDispatcher transaction callback set");
    }

    public void setTermProvider(LongSupplier termProvider) {
        this.termProvider = termProvider;
        log.info("✅ MessageDispatcher term provider set");
    }
    
    /**
     * Dispatch an incoming Aeron message to the appropriate handler.
     * 
     * <p>Uses {@link SimpleMessageHeader} to decode the SBE header format:
     * <ul>
     *   <li>Bytes 0-1: blockLength (payload size)</li>
     *   <li>Bytes 2-3: templateId (message type)</li>
     *   <li>Bytes 4-5: schemaId</li>
     *   <li>Bytes 6-7: version</li>
     * </ul>
     * 
     * @param timestamp message timestamp
     * @param buffer message buffer
     * @param offset buffer offset
     * @param length message length
     * @return true if message was successfully processed
     */
    public boolean dispatch(long timestamp, DirectBuffer buffer, int offset, int length) {
        return dispatch(timestamp, -1L, buffer, offset, length);
    }

    /**
     * Sets the applied watermark read from the Oak store at startup. Oak mutations of log entries at or below
     * it are already in the store, so replay skips them: WRITE_PROPOSAL, DELETE_PROPOSAL, WRITE_BATCH items and
     * GENESIS_PROPOSAL, which record the watermark in their own merge. Entries that rebuild in-memory or derived
     * state are always processed: QUEUE_SEGMENT, SEGMENT_PERSISTED, ACK_SEGMENT_PERSISTED, GC_PROPOSAL, GC_VOTE,
     * GC_EXECUTE (no Oak merge: FileStore cleanup plus in-memory proposal state) and the transaction commands.
     */
    public void setReplayFloor(AppliedLogPosition floor) {
        this.replayFloor = floor != null ? floor : AppliedLogPosition.NONE;
        log.info("Replay floor set from Oak store: {}", replayFloor);
    }

    public AppliedLogPosition getReplayFloor() {
        return replayFloor;
    }

    /**
     * The watermark to record with the Oak mutation of item {@code itemIndex} of the entry ending at
     * {@code logPosition}, or null when the position is unknown (direct calls outside the cluster log).
     */
    AppliedLogPosition entryPosition(long logPosition, int itemIndex) {
        if (logPosition < 0) {
            return null;
        }
        return new AppliedLogPosition(logPosition, itemIndex, termProvider != null ? termProvider.getAsLong() : -1L);
    }

    boolean isAlreadyApplied(AppliedLogPosition entry) {
        if (entry == null || !replayFloor.covers(entry.position(), entry.item())) {
            return false;
        }
        log.debug("Skipping replayed entry already in the Oak store: {} (floor {})", entry, replayFloor);
        return true;
    }

    /**
     * @param logPosition Aeron log position at the end of the entry ({@code Header.position()}), identical on
     *                    every member; -1 when unknown, which disables the watermark
     */
    public boolean dispatch(long timestamp, long logPosition, DirectBuffer buffer, int offset, int length) {
        // Validate minimum message length for SBE header
        if (length < SimpleMessageHeader.ENCODED_LENGTH) {
            log.warn("⚠️  Message too short: {} bytes (minimum {} for SBE header)", 
                length, SimpleMessageHeader.ENCODED_LENGTH);
            return false;
        }
        
        // Decode SBE header using SimpleMessageHeader
        SimpleMessageHeader.HeaderInfo header = SimpleMessageHeader.decode(buffer, offset);
        
        log.debug("📬 Received message: templateId={}, blockLength={}, length={}", 
            header.templateId, header.blockLength, length);
        
        // Validate payload length matches header
        int payloadLength = length - SimpleMessageHeader.ENCODED_LENGTH;
        if (payloadLength < header.blockLength) {
            log.warn("⚠️  Message payload shorter than header blockLength: {} < {}", 
                payloadLength, header.blockLength);
            return false;
        }
        
        // Decode the whole frame: blockLength is 16 bits and wraps for JSON payloads over 64 KiB
        int payloadOffset = offset + SimpleMessageHeader.ENCODED_LENGTH;
        
        switch (header.templateId) {
            case SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL:
                return handleWriteProposal(timestamp, logPosition, buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_DELETE_PROPOSAL:
                return handleDeleteProposal(timestamp, logPosition, buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_WRITE_BATCH:
                return handleWriteBatch(timestamp, logPosition, buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL:
                return handleGCProposal(buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_GC_VOTE:
                return handleGCVote(buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE:
                return handleGCExecute(buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED:
                return handleSegmentPersisted(timestamp, buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_QUEUE_SEGMENT:
            case SimpleMessageHeader.TEMPLATE_ID_ACK_SEGMENT_PERSISTED:
                // No longer sent: durability is decided from SEGMENT_PERSISTED; older logs may still hold them
                log.debug("Ignoring legacy durability entry templateId={}", header.templateId);
                return true;

            case SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION:
                return handleStartTransaction(buffer, payloadOffset, payloadLength);

            case SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION:
                return handleCommitTransaction(buffer, payloadOffset, payloadLength);

            case SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION:
                return handleAbortTransaction(buffer, payloadOffset, payloadLength);
                
            case SimpleMessageHeader.TEMPLATE_ID_GENESIS_PROPOSAL:
                log.info("🎬 GENESIS proposal received - delegating to genesis callback");
                // Genesis is handled specially by AeronConsensusEngine
                return true;
                
            case SimpleMessageHeader.TEMPLATE_ID_SNAPSHOT:
                log.debug("📸 Snapshot message received (handled separately)");
                return true;
                
            default:
                log.warn("Unknown template ID: {}", header.templateId);
                return false;
        }
    }
    
    /**
     * Handle WRITE_PROPOSAL message (template ID 100).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     */
    private boolean handleWriteProposal(long timestamp, long logPosition, DirectBuffer buffer,
                                        int payloadOffset, int payloadLength) {
        AppliedLogPosition entry = entryPosition(logPosition, 0);
        if (isAlreadyApplied(entry)) {
            return true;
        }
        Map<String, Object> json = readPayloadOrNull(buffer, payloadOffset, payloadLength, "write proposal");
        if (json == null) {
            return false;
        }
        log.debug("✈️  Processing write proposal: {} bytes", payloadLength);
        return applyWriteItem(json, timestamp, entry, "write proposal");
    }
    
    /**
     * Handle DELETE_PROPOSAL message (template ID 101).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     */
    private boolean handleDeleteProposal(long timestamp, long logPosition, DirectBuffer buffer,
                                         int payloadOffset, int payloadLength) {
        AppliedLogPosition entry = entryPosition(logPosition, 0);
        if (isAlreadyApplied(entry)) {
            return true;
        }
        Map<String, Object> json = readPayloadOrNull(buffer, payloadOffset, payloadLength, "delete proposal");
        if (json == null) {
            return false;
        }
        log.debug("🗑️  Processing delete proposal: {} bytes", payloadLength);
        return applyDeleteItem(json, timestamp, entry, "delete proposal");
    }
    
    /**
     * Handle WRITE_BATCH message (template ID 106).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     * @return number of proposals successfully processed
     */
    private boolean handleWriteBatch(long timestamp, long logPosition, DirectBuffer buffer,
                                     int payloadOffset, int payloadLength) {
        if (logPosition >= 0 && logPosition < replayFloor.position()) {
            return true;
        }
        Map<String, Object> json = readPayloadOrNull(buffer, payloadOffset, payloadLength, "write batch");
        if (json == null) {
            return false;
        }
        log.debug("📦 Processing write batch: {} bytes", payloadLength);
        
        // Parse batch JSON: {"batch":[{...},{...}]}
        Object batch = json.get("batch");
        if (!(batch instanceof List)) {
            log.error("❌ Invalid batch format: missing batch array");
            return false;
        }
        List<?> proposals = (List<?>) batch;
        
        log.debug("   Batch contains {} proposals", proposals.size());
        
        // Each item stands alone: an invalid item is skipped on every member; a node-local failure propagates
        // and stops this member before any later item is applied.
        int successCount = 0;
        for (int itemIndex = 0; itemIndex < proposals.size(); itemIndex++) {
            Object entry = proposals.get(itemIndex);
            AppliedLogPosition itemPosition = entryPosition(logPosition, itemIndex);
            if (isAlreadyApplied(itemPosition)) {
                continue;
            }
            if (!(entry instanceof Map)) {
                log.warn("Invalid proposal in batch: not a JSON object");
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> proposalJson = (Map<String, Object>) entry;
            // Items carry "operation"; items without it decode as writes
            boolean applied = "DELETE".equals(stringField(proposalJson, "operation"))
                ? applyDeleteItem(proposalJson, timestamp, itemPosition, "batch item " + itemIndex)
                : applyWriteItem(proposalJson, timestamp, itemPosition, "batch item " + itemIndex);
            if (applied) {
                successCount++;
            }
        }
        
        log.debug("✅ Batch processed: {}/{} proposals successful", successCount, proposals.size());
        lastBatchSize = successCount;
        return successCount > 0;
    }

    private boolean applyWriteItem(Map<String, Object> json, long timestamp, AppliedLogPosition entry, String what) {
        String walletAddress = stringField(json, "walletAddress");
        String path = stringField(json, "path");
        String contentType = stringField(json, "contentType");
        String message = stringField(json, "message");
        String signature = stringField(json, "signature");
        String intentToken = stringField(json, "intentToken"); // ADR 020
        String blobId = stringField(json, "blobId");
        String mimeType = stringField(json, "mimeType");
        String ipfsCid = stringField(json, "ipfsCid"); // ADR 016
        MutationAuditMetadata auditMetadata = extractAuditMetadata(
            json,
            MutationAuditMetadata.Operation.WRITE,
            stringField(json, "proposalId")
        ).withClusterTimestamp(timestamp).withAppliedLogPosition(entry);

        if (walletAddress == null || path == null) {
            log.warn("Invalid {}: missing required fields (wallet={}, path={})", what,
                walletAddress != null, path != null);
            return false;
        }
        if (isStaleTerm(longField(json, "term"))) {
            return false;
        }
        log.debug("✅ Applying write: wallet={}, path={}, intentToken={}",
            walletAddress, path, intentToken != null ? intentToken : "none");
        try {
            requireWriteCallback().applyWrite(walletAddress, path, contentType, message, signature,
                intentToken, blobId, mimeType, ipfsCid, auditMetadata);
        } catch (RuntimeException e) {
            return rejectIfInvalid(e, what);
        }
        return true;
    }

    private boolean applyDeleteItem(Map<String, Object> json, long timestamp, AppliedLogPosition entry, String what) {
        String walletAddress = stringField(json, "walletAddress");
        String path = stringField(json, "path");
        String signature = stringField(json, "signature");
        MutationAuditMetadata auditMetadata = extractAuditMetadata(
            json,
            MutationAuditMetadata.Operation.DELETE,
            stringField(json, "proposalId")
        ).withClusterTimestamp(timestamp).withAppliedLogPosition(entry);

        if (walletAddress == null || path == null) {
            log.warn("Invalid {}: missing required fields", what);
            return false;
        }
        if (isStaleTerm(longField(json, "term"))) {
            return false;
        }
        log.info("🗑️  Applying delete: wallet={}, path={}", walletAddress, path);
        try {
            requireWriteCallback().applyDelete(walletAddress, path, signature, auditMetadata);
        } catch (RuntimeException e) {
            return rejectIfInvalid(e, what);
        }
        return true;
    }

    /** A missing callback is node-local wiring, not a property of the entry: fail rather than skip it. */
    private WriteCallback requireWriteCallback() {
        if (writeCallback == null) {
            throw new IllegalStateException("Write callback not set - cannot apply replicated mutation");
        }
        return writeCallback;
    }

    private static boolean rejectIfInvalid(RuntimeException e, String what) {
        if (!InvalidProposalException.isCauseOf(e)) {
            throw e;
        }
        log.warn("❌ Rejected invalid {}: {}", what, e.getMessage());
        return false;
    }

    /** Parsing is a pure function of the replicated bytes, so a parse failure is the same on every member. */
    private static Map<String, Object> readPayloadOrNull(DirectBuffer buffer, int payloadOffset, int payloadLength,
                                                       String what) {
        try {
            return readPayload(buffer, payloadOffset, payloadLength);
        } catch (RuntimeException e) {
            log.warn("❌ Rejected unparseable {}: {}", what, e.getMessage());
            return null;
        }
    }
    
    private boolean isStaleTerm(Long proposalTerm) {
        if (termProvider == null) {
            return false;
        }
        long currentTerm = termProvider.getAsLong();
        if (proposalTerm == null) {
            log.warn("⚠️  Proposal missing term; accepting for compatibility (currentTerm={})", currentTerm);
            return false;
        }
        if (proposalTerm < currentTerm) {
            logStaleTermRejected(proposalTerm, currentTerm);
            return true;
        }
        return false;
    }

    private void logStaleTermRejected(Long proposalTerm, long currentTerm) {
        long now = System.currentTimeMillis();
        long last = lastStaleTermLogMs.get();
        if ((now - last) >= STALE_TERM_LOG_INTERVAL_MS && lastStaleTermLogMs.compareAndSet(last, now)) {
            int suppressed = staleTermSuppressed.getAndSet(0);
            if (suppressed > 0) {
                log.warn("❌ Rejecting proposal from stale term: proposalTerm={}, currentTerm={} (RATE LIMITED - suppressed {} in last {}ms)",
                    proposalTerm, currentTerm, suppressed, STALE_TERM_LOG_INTERVAL_MS);
            } else {
                log.warn("❌ Rejecting proposal from stale term: proposalTerm={}, currentTerm={} (RATE LIMITED)",
                    proposalTerm, currentTerm);
            }
        } else {
            staleTermSuppressed.incrementAndGet();
        }
    }
    
    private static Map<String, Object> readPayload(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        byte[] jsonBytes = new byte[payloadLength];
        buffer.getBytes(payloadOffset, jsonBytes);
        return JsonParser.parseObject(new String(jsonBytes, StandardCharsets.UTF_8));
    }

    private static String stringField(Map<String, Object> json, String field) {
        Object value = json.get(field);
        return value instanceof String ? (String) value : null;
    }

    private static Long longField(Map<String, Object> json, String field) {
        Object value = json.get(field);
        return value instanceof Long ? (Long) value : null;
    }

    private static Boolean booleanField(Map<String, Object> json, String field) {
        Object value = json.get(field);
        return value instanceof Boolean ? (Boolean) value : null;
    }

    private MutationAuditMetadata extractAuditMetadata(Map<String, Object> json,
                                                      MutationAuditMetadata.Operation defaultOperation,
                                                      String fallbackProposalId) {
        String operationValue = stringField(json, "operation");
        MutationAuditMetadata.Operation operation = defaultOperation;
        if (operationValue != null) {
            try {
                operation = MutationAuditMetadata.Operation.valueOf(operationValue);
            } catch (IllegalArgumentException e) {
                log.debug("Ignoring unknown operation '{}' in replicated payload", operationValue);
            }
        }
        String proposalId = stringField(json, "proposalId");
        return new MutationAuditMetadata(
            operation,
            stringField(json, "transactionId"),
            stringField(json, "correlationId"),
            proposalId != null ? proposalId : fallbackProposalId,
            stringField(json, "ethereumTxHash"),
            longField(json, "confirmedBlockNumber"),
            longField(json, "ethereumObservedEpoch"),
            longField(json, "ethereumFinalizedEpoch")
        );
    }
    
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GC MESSAGE HANDLERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    /**
     * Handle GC_PROPOSAL message (template ID 103).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     */
    private boolean handleGCProposal(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (gcCallback == null) {
            log.warn("⚠️  GC callback not set - cannot process GC proposal");
            return false;
        }
        
        try {
            // Extract JSON payload
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);
            
            // Parse GC proposal fields
            String proposalId = stringField(json, "proposalId");
            String proposerWallet = stringField(json, "proposerWallet");
            String targetRevision = stringField(json, "targetRevision");
            Long estimatedReclaimableSizeMB = longField(json, "estimatedReclaimableSizeMB");
            String estimatedCostUSDC = stringField(json, "estimatedCostUSDC");
            
            if (proposalId == null || proposerWallet == null) {
                log.warn("Invalid GC proposal: missing required fields (proposalId={}, proposerWallet={})", 
                    proposalId, proposerWallet);
                return false;
            }
            
            log.info("🗑️  Received GC proposal: id={}, proposer={}, targetRevision={}", 
                proposalId, proposerWallet, targetRevision);
            
            // Delegate to callback
            gcCallback.applyGCProposal(proposalId, proposerWallet, targetRevision,
                estimatedReclaimableSizeMB != null ? estimatedReclaimableSizeMB : 0L,
                estimatedCostUSDC);
            
            return true;
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle GC proposal", e);
            return false;
        }
    }
    
    /**
     * Handle GC_VOTE message (template ID 104).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     */
    private boolean handleGCVote(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (gcCallback == null) {
            log.warn("⚠️  GC callback not set - cannot process GC vote");
            return false;
        }
        
        try {
            // Extract JSON payload
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);
            
            // Parse GC vote fields
            String proposalId = stringField(json, "proposalId");
            Long validatorIdLong = longField(json, "validatorId");
            Boolean approve = booleanField(json, "approve");
            String reason = stringField(json, "reason");
            
            if (proposalId == null || validatorIdLong == null || approve == null) {
                log.warn("Invalid GC vote: missing required fields");
                return false;
            }
            
            int validatorId = validatorIdLong.intValue();
            
            log.info("🗳️  Received GC vote: proposalId={}, validatorId={}, approve={}", 
                proposalId, validatorId, approve);
            
            // Delegate to callback
            gcCallback.applyGCVote(proposalId, validatorId, approve, reason);
            
            return true;
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle GC vote", e);
            return false;
        }
    }
    
    /**
     * Handle GC_EXECUTE message (template ID 105).
     * 
     * @param buffer message buffer
     * @param payloadOffset offset to JSON payload (after SBE header)
     * @param payloadLength length of JSON payload
     */
    private boolean handleGCExecute(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (gcCallback == null) {
            log.warn("⚠️  GC callback not set - cannot process GC execute");
            return false;
        }
        
        try {
            // Extract JSON payload
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);
            
            // Parse GC execute fields
            String proposalId = stringField(json, "proposalId");
            Long executorIdLong = longField(json, "executorId");
            
            if (proposalId == null || executorIdLong == null) {
                log.warn("Invalid GC execute: missing required fields");
                return false;
            }
            
            int executorId = executorIdLong.intValue();
            
            log.info("⚡ Received GC execute command: proposalId={}, executorId={}", 
                proposalId, executorId);
            
            // Delegate to callback
            gcCallback.applyGCExecute(proposalId, executorId);
            
            return true;
            
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle GC execute", e);
            return false;
        }
    }
    
    /**
     * Get the number of proposals processed in the last batch.
     * Used for metrics tracking.
     */
    public int getLastBatchSize() {
        return lastBatchSize;
    }
    
    private volatile int lastBatchSize = 0;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DURABILITY MESSAGE HANDLERS (ADR 026)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private boolean handleSegmentPersisted(long timestamp, DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (durabilityCallback == null) {
            log.warn("⚠️  Durability callback not set - cannot process segment persisted");
            return false;
        }

        try {
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);

            String proposalId = stringField(json, "proposalId");
            Long memberIdLong = longField(json, "memberId");
            String durableHead = stringField(json, "durableHead");
            Boolean success = booleanField(json, "success");
            String error = stringField(json, "error");

            if (proposalId == null || memberIdLong == null || success == null) {
                log.warn("Invalid segment persisted: missing required fields");
                return false;
            }

            durabilityCallback.onSegmentPersisted(
                proposalId,
                memberIdLong.intValue(),
                durableHead,
                success,
                error,
                timestamp
            );
            return true;
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle segment persisted", e);
            return false;
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TRANSACTION MESSAGE HANDLERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private boolean handleStartTransaction(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (transactionCallback == null) {
            log.warn("⚠️  Transaction callback not set - cannot process start transaction");
            return false;
        }

        try {
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);

            String transactionId = stringField(json, "transactionId");
            String correlationId = stringField(json, "correlationId");
            Long timeoutMs = longField(json, "timeoutMs");
            String initiatorWallet = stringField(json, "initiatorWallet");
            Long proposalTerm = longField(json, "term");

            if (transactionId == null) {
                log.warn("Invalid start transaction: missing transactionId");
                return false;
            }
            if (isStaleTerm(proposalTerm)) {
                return false;
            }

            transactionCallback.onStartTransaction(
                transactionId,
                correlationId,
                timeoutMs != null ? timeoutMs : 30000L,
                initiatorWallet
            );
            return true;
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle start transaction", e);
            return false;
        }
    }

    private boolean handleCommitTransaction(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (transactionCallback == null) {
            log.warn("⚠️  Transaction callback not set - cannot process commit transaction");
            return false;
        }

        try {
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);

            String transactionId = stringField(json, "transactionId");
            String correlationId = stringField(json, "correlationId");
            Long proposalTerm = longField(json, "term");

            if (transactionId == null) {
                log.warn("Invalid commit transaction: missing transactionId");
                return false;
            }
            if (isStaleTerm(proposalTerm)) {
                return false;
            }

            transactionCallback.onCommitTransaction(transactionId, correlationId);
            return true;
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle commit transaction", e);
            return false;
        }
    }

    private boolean handleAbortTransaction(DirectBuffer buffer, int payloadOffset, int payloadLength) {
        if (transactionCallback == null) {
            log.warn("⚠️  Transaction callback not set - cannot process abort transaction");
            return false;
        }

        try {
            Map<String, Object> json = readPayload(buffer, payloadOffset, payloadLength);

            String transactionId = stringField(json, "transactionId");
            String correlationId = stringField(json, "correlationId");
            String reason = stringField(json, "reason");
            Long proposalTerm = longField(json, "term");

            if (transactionId == null) {
                log.warn("Invalid abort transaction: missing transactionId");
                return false;
            }
            if (isStaleTerm(proposalTerm)) {
                return false;
            }

            transactionCallback.onAbortTransaction(transactionId, correlationId, reason);
            return true;
        } catch (AgentTerminationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to handle abort transaction", e);
            return false;
        }
    }
}
