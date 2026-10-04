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
package org.apache.jackrabbit.oak.segment.consensus.service;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

/**
 * Explicit audit metadata carried through the replicated apply boundary.
 *
 * <p>Epoch fields are informational only and may be absent.
 */
public final class MutationAuditMetadata {

    public enum Operation {
        WRITE,
        DELETE
    }

    private final Operation operation;
    private final String transactionId;
    private final String correlationId;
    private final String proposalId;
    private final String ethereumTxHash;
    private final Long confirmedBlockNumber;
    private final Long ethereumObservedEpoch;
    private final Long ethereumFinalizedEpoch;
    private final Long clusterTimestamp;

    public MutationAuditMetadata(@NotNull Operation operation,
                                 @Nullable String transactionId,
                                 @Nullable String correlationId,
                                 @Nullable String proposalId,
                                 @Nullable String ethereumTxHash,
                                 @Nullable Long confirmedBlockNumber,
                                 @Nullable Long ethereumObservedEpoch,
                                 @Nullable Long ethereumFinalizedEpoch) {
        this(operation, transactionId, correlationId, proposalId, ethereumTxHash, confirmedBlockNumber,
            ethereumObservedEpoch, ethereumFinalizedEpoch, null);
    }

    private MutationAuditMetadata(@NotNull Operation operation,
                                  @Nullable String transactionId,
                                  @Nullable String correlationId,
                                  @Nullable String proposalId,
                                  @Nullable String ethereumTxHash,
                                  @Nullable Long confirmedBlockNumber,
                                  @Nullable Long ethereumObservedEpoch,
                                  @Nullable Long ethereumFinalizedEpoch,
                                  @Nullable Long clusterTimestamp) {
        this.operation = Objects.requireNonNull(operation, "operation");
        this.transactionId = normalize(transactionId);
        this.correlationId = normalize(correlationId);
        this.proposalId = normalize(proposalId);
        this.ethereumTxHash = normalize(ethereumTxHash);
        this.confirmedBlockNumber = confirmedBlockNumber;
        this.ethereumObservedEpoch = ethereumObservedEpoch;
        this.ethereumFinalizedEpoch = ethereumFinalizedEpoch;
        this.clusterTimestamp = clusterTimestamp;
    }

    @NotNull
    public static MutationAuditMetadata write(@Nullable String transactionId,
                                              @Nullable String correlationId,
                                              @Nullable String proposalId,
                                              @Nullable String ethereumTxHash,
                                              @Nullable Long confirmedBlockNumber,
                                              @Nullable Long ethereumObservedEpoch,
                                              @Nullable Long ethereumFinalizedEpoch) {
        return new MutationAuditMetadata(
            Operation.WRITE,
            transactionId,
            correlationId,
            proposalId,
            ethereumTxHash,
            confirmedBlockNumber,
            ethereumObservedEpoch,
            ethereumFinalizedEpoch
        );
    }

    @NotNull
    public static MutationAuditMetadata delete(@Nullable String transactionId,
                                               @Nullable String correlationId,
                                               @Nullable String proposalId,
                                               @Nullable String ethereumTxHash,
                                               @Nullable Long confirmedBlockNumber,
                                               @Nullable Long ethereumObservedEpoch,
                                               @Nullable Long ethereumFinalizedEpoch) {
        return new MutationAuditMetadata(
            Operation.DELETE,
            transactionId,
            correlationId,
            proposalId,
            ethereumTxHash,
            confirmedBlockNumber,
            ethereumObservedEpoch,
            ethereumFinalizedEpoch
        );
    }

    @NotNull
    public Operation getOperation() {
        return operation;
    }

    @Nullable
    public String getTransactionId() {
        return transactionId;
    }

    @Nullable
    public String getCorrelationId() {
        return correlationId;
    }

    @Nullable
    public String getProposalId() {
        return proposalId;
    }

    @Nullable
    public String getEthereumTxHash() {
        return ethereumTxHash;
    }

    @Nullable
    public Long getConfirmedBlockNumber() {
        return confirmedBlockNumber;
    }

    @Nullable
    public Long getEthereumObservedEpoch() {
        return ethereumObservedEpoch;
    }

    @Nullable
    public Long getEthereumFinalizedEpoch() {
        return ethereumFinalizedEpoch;
    }

    /**
     * Aeron cluster-log timestamp of the replicated command; identical on every member.
     */
    @Nullable
    public Long getClusterTimestamp() {
        return clusterTimestamp;
    }

    @NotNull
    public MutationAuditMetadata withClusterTimestamp(long nextClusterTimestamp) {
        return new MutationAuditMetadata(
            operation,
            transactionId,
            correlationId,
            proposalId,
            ethereumTxHash,
            confirmedBlockNumber,
            ethereumObservedEpoch,
            ethereumFinalizedEpoch,
            nextClusterTimestamp
        );
    }

    @NotNull
    public MutationAuditMetadata withOperation(@NotNull Operation nextOperation) {
        if (operation == nextOperation) {
            return this;
        }
        return new MutationAuditMetadata(
            nextOperation,
            transactionId,
            correlationId,
            proposalId,
            ethereumTxHash,
            confirmedBlockNumber,
            ethereumObservedEpoch,
            ethereumFinalizedEpoch,
            clusterTimestamp
        );
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
