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
package org.apache.jackrabbit.oak.segment.consensus.metrics;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

/**
 * Prometheus metrics for the Oak Segment Consensus system.
 * 
 * Tracks:
 * - Segment write operations (throughput, latency, errors)
 * - Consensus protocol metrics (proposals, commits, leader elections)
 * - Replication metrics (time, failures)
 * - System health indicators
 */
public class ConsensusMetrics {
    
    // ========== Segment Write Metrics ==========
    
    /**
     * Total number of segment writes attempted.
     */
    public static final Counter segmentWritesTotal = Counter.build()
            .name("oak_segment_writes_total")
            .help("Total number of segment write operations")
            .labelNames("status", "validator") // status: success, failure, timeout
            .register();
    
    /**
     * Histogram of segment write latencies (seconds).
     */
    public static final Histogram segmentWriteLatency = Histogram.build()
            .name("oak_segment_write_latency_seconds")
            .help("Latency of segment write operations")
            .labelNames("validator")
            .buckets(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0) // 10ms to 10s
            .register();
    
    /**
     * Summary of segment sizes written (bytes).
     */
    public static final Summary segmentSizeBytes = Summary.build()
            .name("oak_segment_size_bytes")
            .help("Size distribution of segments written")
            .labelNames("validator")
            .quantile(0.5, 0.05)   // median ± 5%
            .quantile(0.95, 0.01)  // 95th percentile ± 1%
            .quantile(0.99, 0.001) // 99th percentile ± 0.1%
            .register();
    
    // ========== Consensus Protocol Metrics ==========
    
    /**
     * Total number of consensus proposals initiated.
     */
    public static final Counter consensusProposalsTotal = Counter.build()
            .name("oak_consensus_proposals_total")
            .help("Total number of consensus proposals")
            .labelNames("status") // status: committed, rejected, timeout
            .register();
    
    /**
     * Histogram of consensus commit latencies (seconds).
     */
    public static final Histogram consensusCommitLatency = Histogram.build()
            .name("oak_consensus_commit_latency_seconds")
            .help("Time from proposal to commit")
            .buckets(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5) // 1ms to 500ms
            .register();
    
    /**
     * Current leader epoch number.
     */
    public static final Gauge leaderEpoch = Gauge.build()
            .name("oak_consensus_leader_epoch")
            .help("Current leader epoch (increments on leader change)")
            .register();
    
    /**
     * Is this validator currently the leader? (0 or 1)
     */
    public static final Gauge isLeader = Gauge.build()
            .name("oak_consensus_is_leader")
            .help("Whether this validator is currently the leader (1=leader, 0=follower)")
            .register();
    
    /**
     * Total number of leader elections.
     */
    public static final Counter leaderElectionsTotal = Counter.build()
            .name("oak_consensus_leader_elections_total")
            .help("Total number of leader elections")
            .register();
    
    /**
     * Total number of leader heartbeats sent.
     */
    public static final Counter heartbeatsSentTotal = Counter.build()
            .name("oak_consensus_heartbeats_sent_total")
            .help("Total number of heartbeats sent by leader")
            .register();
    
    /**
     * Total number of heartbeats received.
     */
    public static final Counter heartbeatsReceivedTotal = Counter.build()
            .name("oak_consensus_heartbeats_received_total")
            .help("Total number of heartbeats received from leader")
            .labelNames("leader_id")
            .register();
    
    /**
     * Time since last heartbeat received (seconds).
     */
    public static final Gauge timeSinceLastHeartbeat = Gauge.build()
            .name("oak_consensus_time_since_last_heartbeat_seconds")
            .help("Seconds since last heartbeat from leader")
            .register();
    
    // ========== Replication Metrics ==========
    
    /**
     * Total number of segment replications.
     */
    public static final Counter replicationsTotal = Counter.build()
            .name("oak_segment_replications_total")
            .help("Total number of segment replication operations")
            .labelNames("status", "target_validator") // status: success, failure, timeout
            .register();
    
    /**
     * Histogram of replication latencies (seconds).
     */
    public static final Histogram replicationLatency = Histogram.build()
            .name("oak_segment_replication_latency_seconds")
            .help("Time to replicate segment to a follower")
            .labelNames("target_validator")
            .buckets(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0) // 10ms to 5s
            .register();
    
    /**
     * Current replication queue depth (segments waiting to be replicated).
     */
    public static final Gauge replicationQueueDepth = Gauge.build()
            .name("oak_segment_replication_queue_depth")
            .help("Number of segments in replication queue")
            .register();
    
    // ========== Validator Network Health ==========
    
    /**
     * Number of validators currently reachable.
     */
    public static final Gauge validatorsReachable = Gauge.build()
            .name("oak_validators_reachable")
            .help("Number of validators currently responding to health checks")
            .register();
    
    /**
     * Total number of validator failures detected.
     */
    public static final Counter validatorFailuresTotal = Counter.build()
            .name("oak_validator_failures_total")
            .help("Total number of validator failure events detected")
            .labelNames("validator_id", "failure_type") // failure_type: timeout, network_error, http_error
            .register();
    
    /**
     * Total number of validator recoveries (back online after failure).
     */
    public static final Counter validatorRecoveriesTotal = Counter.build()
            .name("oak_validator_recoveries_total")
            .help("Total number of validator recovery events")
            .labelNames("validator_id")
            .register();
    
    // ========== DAG Chain Metrics ==========
    
    /**
     * Current height of the DAG chain (number of blocks).
     */
    public static final Gauge dagChainHeight = Gauge.build()
            .name("oak_dag_chain_height")
            .help("Current height of the DAG blockchain")
            .register();
    
    /**
     * Total number of transactions in the chain.
     */
    public static final Counter dagTransactionsTotal = Counter.build()
            .name("oak_dag_transactions_total")
            .help("Total number of transactions committed to the chain")
            .register();
    
    /**
     * Current number of pending transactions (not yet in a block).
     */
    public static final Gauge dagPendingTransactions = Gauge.build()
            .name("oak_dag_pending_transactions")
            .help("Number of transactions waiting to be included in a block")
            .register();
    
    // ========== HTTP Client Metrics (for consensus read-mount transport) ==========
    
    /**
     * Total number of HTTP requests to validators.
     */
    public static final Counter httpRequestsTotal = Counter.build()
            .name("oak_http_requests_total")
            .help("Total number of HTTP requests to validators")
            .labelNames("method", "endpoint", "status") // method: GET/POST, status: 2xx/4xx/5xx
            .register();

    /**
     * Total number of IPFS policy rejections on write proposals.
     */
    public static final Counter ipfsPolicyRejectionsTotal = Counter.build()
            .name("oak_api_ipfs_policy_rejections_total")
            .help("Total number of write rejections due to IPFS supply-chain policy enforcement")
            .labelNames("reason")
            .register();

    /**
     * Total number of enterprise ipfsCid write requests accepted by policy checks.
     */
    public static final Counter enterpriseCidAcceptedTotal = Counter.build()
            .name("oak_api_ipfs_enterprise_cid_accepted_total")
            .help("Total number of enterprise client ipfsCid write requests accepted by policy checks")
            .register();
    
    /**
     * Histogram of HTTP request latencies (seconds).
     */
    public static final Histogram httpRequestLatency = Histogram.build()
            .name("oak_http_request_latency_seconds")
            .help("HTTP request latency to validators")
            .labelNames("method", "endpoint")
            .buckets(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
            .register();
    
    // ========== MediaDriver Health Metrics ==========
    
    /**
     * Number of MediaDriver crashes detected (resets on successful startup).
     */
    public static final Gauge mediaDriverCrashCount = Gauge.build()
            .name("oak_mediadriver_crash_count")
            .help("Number of MediaDriver crashes detected (resets on successful startup)")
            .register();
    
    /**
     * Whether MediaDriver has crashed (1=crashed, 0=healthy).
     */
    public static final Gauge mediaDriverHasCrashed = Gauge.build()
            .name("oak_mediadriver_has_crashed")
            .help("Whether MediaDriver has crashed (1=crashed, 0=healthy)")
            .register();
    
    /**
     * Whether force bootstrap is required (1=required, 0=not required).
     */
    public static final Gauge mediaDriverForceBootstrap = Gauge.build()
            .name("oak_mediadriver_force_bootstrap")
            .help("Whether force bootstrap is required after multiple crashes (1=required, 0=not required)")
            .register();
    
    // ========== System Resource Metrics ==========
    
    /**
     * Current number of segments stored locally.
     */
    public static final Gauge segmentsStoredTotal = Gauge.build()
            .name("oak_segments_stored_total")
            .help("Number of Oak TAR segment-store files on this validator")
            .register();
    
    /**
     * Disk space used by segments (bytes).
     */
    public static final Gauge segmentsDiskUsageBytes = Gauge.build()
            .name("oak_segments_disk_usage_bytes")
            .help("Logical bytes in Oak TAR segment files; excludes Aeron runtime files and filesystem allocation overhead")
            .register();
    
    /**
     * Number of active connections to this validator.
     */
    public static final Gauge activeConnections = Gauge.build()
            .name("oak_active_connections")
            .help("Number of active HTTP connections to this validator")
            .register();
    
    // ========== Utility Methods ==========
    
    /**
     * Record a successful segment write with timing.
     * 
     * @param validatorId The validator ID
     * @param latencySeconds Write latency in seconds
     * @param segmentSizeBytes Size of the segment in bytes
     */
    public static void recordSegmentWrite(String validatorId, double latencySeconds, long segmentSizeBytes) {
        segmentWritesTotal.labels("success", validatorId).inc();
        segmentWriteLatency.labels(validatorId).observe(latencySeconds);
        ConsensusMetrics.segmentSizeBytes.labels(validatorId).observe(segmentSizeBytes);
    }
    
    /**
     * Record a failed segment write.
     * 
     * @param validatorId The validator ID
     * @param status Error status (failure, timeout, etc.)
     */
    public static void recordSegmentWriteFailure(String validatorId, String status) {
        segmentWritesTotal.labels(status, validatorId).inc();
    }
    
    /**
     * Record a consensus operation.
     * 
     * @param status Outcome (committed, rejected, timeout)
     * @param latencySeconds Time from proposal to completion
     */
    public static void recordConsensusOperation(String status, double latencySeconds) {
        consensusProposalsTotal.labels(status).inc();
        if ("committed".equals(status)) {
            consensusCommitLatency.observe(latencySeconds);
        }
    }
    
    /**
     * Record a replication operation.
     * 
     * @param targetValidator Target validator ID
     * @param status Outcome (success, failure, timeout)
     * @param latencySeconds Replication time in seconds
     */
    public static void recordReplication(String targetValidator, String status, double latencySeconds) {
        replicationsTotal.labels(status, targetValidator).inc();
        if ("success".equals(status)) {
            replicationLatency.labels(targetValidator).observe(latencySeconds);
        }
    }
    
    /**
     * Update leader status.
     * 
     * @param isCurrentlyLeader true if this validator is the leader
     * @param epoch Current leader epoch
     */
    public static void updateLeaderStatus(boolean isCurrentlyLeader, long epoch) {
        isLeader.set(isCurrentlyLeader ? 1 : 0);
        leaderEpoch.set(epoch);
    }
    
    /**
     * Record a leader election event.
     */
    public static void recordLeaderElection() {
        leaderElectionsTotal.inc();
    }
    
    // ====================================================================================
    // LEADER CLAIM PROTOCOL METRICS (World-Class Consensus)
    // ====================================================================================
    
    /**
     * Total number of leadership claims by result.
     * Status: accepted, stale_or_future, byzantine, duplicate, timeout_failover
     */
    public static final Counter leadershipClaimsTotal = Counter.build()
            .name("oak_consensus_leadership_claims_total")
            .help("Total number of leadership claims by result.")
            .labelNames("status")
            .register();
    
    /**
     * Leadership claim latency (from claim broadcast to acceptance).
     */
    public static final Histogram leadershipClaimLatency = Histogram.build()
            .name("oak_consensus_leadership_claim_latency_seconds")
            .help("Leadership claim latency in seconds.")
            .buckets(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0)
            .register();
    
    /**
     * Number of followers that accepted the claim broadcast.
     */
    public static final Histogram leadershipClaimBroadcastSuccess = Histogram.build()
            .name("oak_consensus_leadership_claim_broadcast_success")
            .help("Number of followers that accepted the claim broadcast.")
            .buckets(1, 2, 3, 5, 10, 20, 50)
            .register();
    
    /**
     * Total number of failover elections triggered by missing claims.
     */
    public static final Counter failoverElectionsTotal = Counter.build()
            .name("oak_consensus_failover_elections_total")
            .help("Total number of failover elections triggered by missing claims.")
            .register();
    
    /**
     * Number of validators currently marked as OFFLINE.
     */
    public static final Gauge offlineValidatorsCount = Gauge.build()
            .name("oak_consensus_offline_validators")
            .help("Number of validators currently marked as OFFLINE.")
            .register();
    
    // ====================================================================================
    // PHASE 2: QUORUM-BASED ACCEPTANCE METRICS (Split-Brain Prevention)
    // ====================================================================================
    
    /**
     * Total number of claim ACKs by result.
     * Status: accepted, invalid_signature, unknown_epoch, mismatch
     */
    public static final Counter claimAcksTotal = Counter.build()
            .name("oak_consensus_claim_acks_total")
            .help("Total claim acknowledgments by result")
            .labelNames("result")
            .register();
    
    /**
     * Time to reach quorum for leadership claims.
     */
    public static final Histogram quorumWaitTime = Histogram.build()
            .name("oak_consensus_quorum_wait_seconds")
            .help("Time to reach quorum for claims")
            .buckets(0.5, 1.0, 2.0, 5.0, 10.0)
            .register();
    
    /**
     * Record a leadership claim result.
     * 
     * @param status Result status (accepted, stale_or_future, byzantine, duplicate, timeout_failover, invalid_signature)
     */
    public static void recordLeadershipClaimResult(String status) {
        leadershipClaimsTotal.labels(status).inc();
        if ("timeout_failover".equals(status)) {
            failoverElectionsTotal.inc();
        }
    }
    
    /**
     * Record a claim ACK result (PHASE 2).
     * 
     * @param result Result status (accepted, invalid_signature, unknown_epoch, mismatch)
     */
    public static void recordClaimAckResult(String result) {
        claimAcksTotal.labels(result).inc();
    }
    
    /**
     * Record quorum wait time (PHASE 2).
     * 
     * @param timeMs Time in milliseconds to reach quorum
     */
    public static void recordQuorumWaitTime(long timeMs) {
        quorumWaitTime.observe(timeMs / 1000.0);
    }
    
    /**
     * Record leadership claim latency.
     * 
     * @param latencyMs Latency in milliseconds
     */
    public static void recordLeadershipClaimLatency(long latencyMs) {
        leadershipClaimLatency.observe(latencyMs / 1000.0);
    }
    
    /**
     * Record leadership claim broadcast results.
     * 
     * @param successCount Number of followers that accepted the claim
     * @param totalCount   Total number of followers
     */
    public static void recordLeadershipClaimBroadcast(int successCount, int totalCount) {
        leadershipClaimBroadcastSuccess.observe(successCount);
    }
    
    /**
     * Record an HTTP request to a validator.
     * 
     * @param method HTTP method (GET, POST, etc.)
     * @param endpoint Endpoint path
     * @param statusCode HTTP status code
     * @param latencySeconds Request latency in seconds
     */
    public static void recordHttpRequest(String method, String endpoint, int statusCode, double latencySeconds) {
        String statusBucket = statusCode < 400 ? "2xx" : (statusCode < 500 ? "4xx" : "5xx");
        httpRequestsTotal.labels(method, endpoint, statusBucket).inc();
        httpRequestLatency.labels(method, endpoint).observe(latencySeconds);
    }

    /**
     * Record an IPFS policy rejection reason.
     *
     * @param reason policy rejection reason
     */
    public static void recordIpfsPolicyRejection(String reason) {
        ipfsPolicyRejectionsTotal.labels(reason == null ? "unknown" : reason).inc();
    }

    /**
     * Record accepted enterprise ipfsCid request.
     */
    public static void recordEnterpriseCidAccepted() {
        enterpriseCidAcceptedTotal.inc();
    }
    
    // ====================================================================================
    // HTTP Connection Pool Metrics
    // ====================================================================================
    
    /**
     * Number of connections currently leased from the HTTP client pool.
     */
    public static final Gauge httpPoolConnectionsLeased = Gauge.build()
            .name("oak_http_pool_connections_leased")
            .help("Number of connections currently leased from the HTTP client pool.")
            .register();

    /**
     * Number of connection requests waiting for a connection from the pool.
     */
    public static final Gauge httpPoolConnectionsPending = Gauge.build()
            .name("oak_http_pool_connections_pending")
            .help("Number of connection requests waiting for a connection from the pool.")
            .register();

    /**
     * Number of idle connections available in the pool.
     */
    public static final Gauge httpPoolConnectionsAvailable = Gauge.build()
            .name("oak_http_pool_connections_available")
            .help("Number of idle connections available in the pool.")
            .register();

    /**
     * Maximum number of connections allowed in the pool.
     */
    public static final Gauge httpPoolConnectionsMax = Gauge.build()
            .name("oak_http_pool_connections_max")
            .help("Maximum number of connections allowed in the pool.")
            .register();
    
    /**
     * Update HTTP connection pool metrics.
     * 
     * @param leased Number of leased connections
     * @param pending Number of pending connection requests
     * @param available Number of available connections
     * @param max Maximum pool size
     */
    public static void updateHttpPoolMetrics(int leased, int pending, int available, int max) {
        httpPoolConnectionsLeased.set(leased);
        httpPoolConnectionsPending.set(pending);
        httpPoolConnectionsAvailable.set(available);
        httpPoolConnectionsMax.set(max);
    }
    
    /**
     * Private constructor - this is a utility class with only static methods.
     */
    private ConsensusMetrics() {
        // Utility class, no instances
    }
}
