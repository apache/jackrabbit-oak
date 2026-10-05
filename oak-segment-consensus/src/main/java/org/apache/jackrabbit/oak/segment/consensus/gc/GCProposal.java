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
package org.apache.jackrabbit.oak.segment.consensus.gc;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a GC (garbage collection) proposal that requires validator consensus.
 * 
 * <p>State machine: PENDING → VOTING → APPROVED → EXECUTING → COMPLETED
 *                   PENDING → VOTING → REJECTED</p>
 *
 * <p>Time fields are cluster timestamps of the log entries that changed the proposal, never a local clock,
 * so every member, and a replay, decides expiry alike.</p>
 */
public class GCProposal {

    public static final long DEFAULT_TTL_MS = 24 * 60 * 60 * 1000L;
    
    public enum GCProposalState {
        PENDING,      // Proposal created, waiting for replication
        VOTING,       // Validators voting on proposal
        APPROVED,     // Quorum reached, approved for execution
        REJECTED,     // Quorum reached, rejected
        EXECUTING,    // GC execution in progress
        COMPLETED,    // GC execution completed successfully
        FAILED        // GC execution failed
    }
    
    public String proposalId;
    public String proposerWallet;
    public String targetRevision; // null = use HEAD
    public long estimatedReclaimableSizeMB;
    public BigDecimal estimatedCostUSDC;
    public long fragmentationOverheadMB;
    public BigDecimal fragmentationCostUSDC;
    public String paymentProof; // Ethereum transaction hash (optional for POC)
    public GCProposalState state;
    public Map<Integer, GCVote> votes; // validatorId -> vote
    public long createdAt;
    public long expiresAt; // Proposal expiration timestamp
    public GCExecutionResult executionResult;
    
    public GCProposal() {
        this.votes = new HashMap<>();
        this.state = GCProposalState.PENDING;
    }
    
    /**
     * Add a vote to this proposal.
     */
    public void addVote(int validatorId, boolean approve, String reason, long clusterTime) {
        GCVote vote = new GCVote();
        vote.proposalId = this.proposalId;
        vote.validatorId = validatorId;
        vote.approve = approve;
        vote.reason = reason;
        vote.timestamp = clusterTime;
        
        this.votes.put(validatorId, vote);
        
        // Transition to VOTING state if first vote
        if (this.state == GCProposalState.PENDING) {
            this.state = GCProposalState.VOTING;
        }
    }
    
    /**
     * Whether the proposal has expired at the given cluster time.
     */
    public boolean isExpiredAt(long clusterTime) {
        return clusterTime > expiresAt;
    }
    
    /**
     * Get count of approve votes.
     */
    public int getApproveVoteCount() {
        return (int) votes.values().stream()
            .filter(v -> v.approve)
            .count();
    }
    
    /**
     * Get count of reject votes.
     */
    public int getRejectVoteCount() {
        return (int) votes.values().stream()
            .filter(v -> !v.approve)
            .count();
    }
    
    /**
     * Get total vote count.
     */
    public int getTotalVoteCount() {
        return votes.size();
    }
}

