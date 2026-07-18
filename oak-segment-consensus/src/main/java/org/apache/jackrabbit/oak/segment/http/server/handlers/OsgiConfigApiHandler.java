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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterTuningIntrospection;
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfigIntrospection;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimeConfigValueResolver;
import org.apache.jackrabbit.oak.segment.consensus.config.RuntimePropertySourceRegistry;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueTuningIntrospection;
import org.apache.jackrabbit.oak.segment.http.server.AuthTokenValidator;
import org.apache.jackrabbit.oak.segment.http.server.RateLimiter;
import org.apache.jackrabbit.oak.segment.http.server.RateLimiterTuningIntrospection;
import org.apache.jackrabbit.oak.segment.http.server.TlsConfiguration;
import org.apache.jackrabbit.oak.segment.http.server.util.JsonOutputUtil;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only config introspection endpoints for OSGi-governed runtime knobs.
 */
public class OsgiConfigApiHandler {

    public void handleEffectiveConfig(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "config.osgi.v1");
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("components", buildComponents());
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    public void handleConfigSources(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "config.osgi.sources.v1");
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("sources", buildSourcesMap());
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    public void handleConfigSchema(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "config.osgi.schema.v1");
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("schema", buildSchema());
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    public void handleCoverage(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        List<Map<String, Object>> schema = buildSchema();
        Set<String> exposedKeys = new LinkedHashSet<>();
        for (Map<String, Object> entry : schema) {
            Object raw = entry.get("key");
            if (raw != null) {
                exposedKeys.add(String.valueOf(raw));
            }
        }

        Set<String> knownTunables = buildKnownTunables();
        List<String> missing = new ArrayList<>();
        for (String key : knownTunables) {
            if (!exposedKeys.contains(key)) {
                missing.add(key);
            }
        }

        List<String> extra = new ArrayList<>();
        for (String key : exposedKeys) {
            if (!knownTunables.contains(key)) {
                extra.add(key);
            }
        }

        int known = knownTunables.size();
        int exposed = exposedKeys.size();
        double coveragePercent = known == 0 ? 100.0 : (exposed * 100.0) / known;

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("knownTunables", known);
        summary.put("exposedTunables", exposed);
        summary.put("missingTunables", missing.size());
        summary.put("extraExposedTunables", extra.size());
        summary.put("coveragePercent", Math.round(coveragePercent * 10.0d) / 10.0d);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "config.osgi.coverage.v1");
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("summary", summary);
        payload.put("missing", missing);
        payload.put("extra", extra);
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    public void handleDelta(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=UTF-8");

        List<Map<String, Object>> schema = buildSchema();
        Map<String, Object> effective = flattenComponents(buildComponents());
        Map<String, Map<String, Object>> schemaByKey = new LinkedHashMap<>();
        for (Map<String, Object> entry : schema) {
            Object rawKey = entry.get("key");
            if (rawKey != null) {
                schemaByKey.put(String.valueOf(rawKey), entry);
            }
        }

        List<Map<String, Object>> changed = new ArrayList<>();
        List<Map<String, Object>> unchanged = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> schemaEntry : schemaByKey.entrySet()) {
            String key = schemaEntry.getKey();
            Map<String, Object> meta = schemaEntry.getValue();
            Object defaultValue = meta.get("default");
            Object currentValue = effective.get(key);

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", key);
            row.put("current", currentValue);
            row.put("default", defaultValue);
            row.put("risk", meta.get("risk"));
            row.put("reloadMode", meta.get("reloadMode"));
            row.put("changed", !looselyEqual(currentValue, defaultValue));
            row.put("justification", justificationFor(key, currentValue, defaultValue));

            if (Boolean.TRUE.equals(row.get("changed"))) {
                changed.add(row);
            } else {
                unchanged.add(row);
            }
        }

        int expertChanged = countRisk(changed, "expert-only");
        int guardedChanged = countRisk(changed, "guarded");
        int safeChanged = countRisk(changed, "safe");

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalKeys", schemaByKey.size());
        summary.put("changedKeys", changed.size());
        summary.put("unchangedKeys", unchanged.size());
        summary.put("expertOnlyChanged", expertChanged);
        summary.put("guardedChanged", guardedChanged);
        summary.put("safeChanged", safeChanged);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contractVersion", "config.osgi.delta.v1");
        payload.put("generatedAtMs", System.currentTimeMillis());
        payload.put("summary", summary);
        payload.put("changed", changed);
        payload.put("unchanged", unchanged);
        response.getWriter().write(JsonOutputUtil.toJson(payload));
    }

    private Map<String, Object> buildComponents() {
        Map<String, Object> components = new LinkedHashMap<>();
        components.put("aeronClusterTuning", AeronClusterTuningIntrospection.effectiveValues());
        components.put("proposalQueueTuning", ProposalQueueTuningIntrospection.effectiveValues());
        components.put("rateLimiterTuning", RateLimiterTuningIntrospection.effectiveValues());
        components.put("tlsTuning", buildTlsTuning());
        components.put("tokenAuthTuning", buildTokenAuthTuning());
        components.put("fileStoreFlushTuning", buildFileStoreFlushTuning());
        components.put("gcEconomicsTuning", buildGcEconomicsTuning());
        components.put("blockchainTuning", BlockchainConfigIntrospection.effectiveValues());
        components.put("nodeRuntimeTuning", buildNodeRuntimeTuning());
        components.put("runtimeUiTuning", buildRuntimeUiTuning());
        return components;
    }

    private Map<String, Object> buildSourcesMap() {
        Map<String, Object> sources = new LinkedHashMap<>();
        sources.put("aeronClusterTuning", AeronClusterTuningIntrospection.source());
        sources.put("proposalQueueTuning", ProposalQueueTuningIntrospection.source());
        sources.put("rateLimiterTuning", RateLimiterTuningIntrospection.source());
        sources.put("tlsTuning", RuntimePropertySourceRegistry.getSource("tlsTuning", "system-properties"));
        sources.put("tokenAuthTuning", RuntimePropertySourceRegistry.getSource("tokenAuthTuning", "system-properties-or-env"));
        sources.put("fileStoreFlushTuning", RuntimePropertySourceRegistry.getSource("fileStoreFlushTuning", "system-properties"));
        sources.put("gcEconomicsTuning", RuntimePropertySourceRegistry.getSource("gcEconomicsTuning", "system-properties"));
        sources.put("blockchainTuning", BlockchainConfigIntrospection.source());
        sources.put("nodeRuntimeTuning", RuntimePropertySourceRegistry.getSource("nodeRuntimeTuning", "system-properties-or-env"));
        sources.put("runtimeUiTuning", RuntimePropertySourceRegistry.getSource("runtimeUiTuning", "system-properties"));
        return sources;
    }

    private List<Map<String, Object>> buildSchema() {
        List<Map<String, Object>> schema = new ArrayList<>();

        schema.add(schemaEntry(
            "aeronClusterTuning.enabled",
            "boolean",
            true,
            "startup-only",
            "guarded",
            "Enable Aeron cluster service",
            "osgi:AeronClusterConfig.enabled"));
        schema.add(schemaEntry(
            "aeronClusterTuning.node_id",
            "int",
            0,
            "startup-only",
            "guarded",
            "Aeron cluster node id",
            "osgi:AeronClusterConfig.nodeId"));
        schema.add(schemaEntry(
            "aeronClusterTuning.self_url_configured",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Whether an explicit self URL is configured",
            "osgi:AeronClusterConfig.selfUrl"));
        schema.add(schemaEntry(
            "aeronClusterTuning.peer_urls_count",
            "int",
            0,
            "startup-only",
            "guarded",
            "Number of configured peer URLs",
            "osgi:AeronClusterConfig.peerUrls"));
        schema.add(schemaEntry(
            "aeronClusterTuning.observe_elections",
            "boolean",
            true,
            "startup-only",
            "safe",
            "Observe elections before genesis writes",
            "osgi:AeronClusterConfig.observeElections"));
        schema.add(schemaEntry(
            "aeronClusterTuning.log_cluster_state_details",
            "boolean",
            false,
            "startup-only",
            "safe",
            "Enable verbose cluster startup logging",
            "osgi:AeronClusterConfig.logClusterStateDetails"));
        schema.add(schemaEntry(
            "aeronClusterTuning.cluster_environment",
            "string",
            "",
            "startup-only",
            "guarded",
            "Environment profile for Aeron timeout defaults",
            "oak.cluster.environment"));
        schema.add(schemaEntry(
            "aeronClusterTuning.session_timeout_minutes",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Aeron session timeout override",
            "oak.cluster.session.timeout.minutes"));
        schema.add(schemaEntry(
            "aeronClusterTuning.media_driver_timeout_ms",
            "int",
            0,
            "startup-only",
            "expert-only",
            "MediaDriver timeout override",
            "oak.cluster.media.driver.timeout.ms"));
        schema.add(schemaEntry(
            "aeronClusterTuning.cluster_term_length_bytes",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Aeron cluster term length",
            "oak.cluster.term.length.bytes"));
        schema.add(schemaEntry(
            "aeronClusterTuning.peer_probe_mode",
            "string",
            "",
            "startup-only",
            "guarded",
            "Peer probe mode for health checks",
            "oak.health.peerProbeMode"));
        schema.add(schemaEntry(
            "aeronClusterTuning.socket_send_buffer_bytes",
            "string",
            "auto",
            "startup-only",
            "guarded",
            "Aeron socket send buffer size",
            "aeron.socket.so_sndbuf"));
        schema.add(schemaEntry(
            "aeronClusterTuning.socket_receive_buffer_bytes",
            "string",
            "auto",
            "startup-only",
            "guarded",
            "Aeron socket receive buffer size",
            "aeron.socket.so_rcvbuf"));
        schema.add(schemaEntry(
            "aeronClusterTuning.publication_term_buffer_length_bytes",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Aeron publication term buffer length",
            "oak.cluster.publication.term.buffer.length.bytes"));
        schema.add(schemaEntry(
            "aeronClusterTuning.heartbeat_max_age_ms",
            "long",
            0L,
            "startup-only",
            "guarded",
            "Heartbeat age threshold for liveness",
            "oak.cluster.heartbeat.maxAgeMs"));
        schema.add(schemaEntry(
            "aeronClusterTuning.reachability_cache_ms",
            "long",
            0L,
            "startup-only",
            "guarded",
            "Peer reachability cache duration",
            "oak.cluster.reachability.cacheMs"));
        schema.add(schemaEntry(
            "aeronClusterTuning.reachability_connect_timeout_ms",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Peer reachability connect timeout",
            "oak.cluster.reachability.connectTimeoutMs"));
        schema.add(schemaEntry(
            "aeronClusterTuning.reachability_read_timeout_ms",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Peer reachability read timeout",
            "oak.cluster.reachability.readTimeoutMs"));
        schema.add(schemaEntry(
            "aeronClusterTuning.reconnect_max_attempts",
            "int",
            0,
            "startup-only",
            "guarded",
            "Peer reconnect attempt ceiling",
            "oak.cluster.reconnect.maxAttempts"));
        schema.add(schemaEntry(
            "aeronClusterTuning.max_concurrent_sessions",
            "int",
            0,
            "startup-only",
            "expert-only",
            "ConsensusModule max concurrent sessions override",
            "oak.cluster.max.concurrent.sessions"));
        schema.add(schemaEntry(
            "aeronClusterTuning.delete_aeron_dirs_on_startup",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Delete Aeron runtime dirs during startup",
            "aeron.delete.dirs.on.startup"));
        schema.add(schemaEntry(
            "aeronClusterTuning.beacon_api_url",
            "string",
            "https://beaconcha.in/api",
            "startup-only",
            "guarded",
            "Beacon API base URL",
            "ethereum.beacon.api.url"));

        schema.add(schemaEntry(
            "proposalQueueTuning.max_message_batch",
            "int",
            10,
            "runtime-readable",
            "guarded",
            "Max Aeron batches per sender cycle",
            "oak.proposal.batch.max"));
        schema.add(schemaEntry(
            "proposalQueueTuning.finalization_chunk_size",
            "int",
            3,
            "runtime-readable",
            "guarded",
            "Max proposals released per chunk when draining verified work to Aeron",
            "oak.proposal.finalization.chunk.size"));
        schema.add(schemaEntry(
            "proposalQueueTuning.finalization_chunk_delay_ms",
            "long",
            0L,
            "runtime-readable",
            "guarded",
            "Optional delay between verified release chunks; 0 disables pacing",
            "oak.proposal.finalization.chunk.delay.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.max_pending_messages",
            "long",
            10000,
            "runtime-readable",
            "expert-only",
            "Backpressure pending-message ceiling",
            "oak.consensus.max.pending.messages"));
        schema.add(schemaEntry(
            "proposalQueueTuning.backpressure_timeout_ms",
            "long",
            30000,
            "runtime-readable",
            "expert-only",
            "Timeout while waiting under backpressure",
            "oak.consensus.backpressure.timeout.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.persistence_flush_interval_ms",
            "long",
            250,
            "runtime-readable",
            "guarded",
            "Durability flush interval",
            "oak.proposal.persistence.flush.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.persistence_flush_batch",
            "int",
            100,
            "runtime-readable",
            "guarded",
            "Durability flush batch threshold",
            "oak.proposal.persistence.flush.batch"));
        schema.add(schemaEntry(
            "proposalQueueTuning.persistence_enabled",
            "boolean",
            true,
            "runtime-readable",
            "guarded",
            "Enable persistent queue snapshots",
            "oak.proposal.persistence.enabled"));
        schema.add(schemaEntry(
            "proposalQueueTuning.confirmation_timeout_ms",
            "long",
            300000L,
            "runtime-readable",
            "guarded",
            "Proposal confirmation timeout",
            "oak.proposal.confirmation.timeout.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.required_confirmations",
            "int",
            1,
            "runtime-readable",
            "guarded",
            "Minimum payment confirmations before verifier release",
            "oak.proposal.confirmation.required"));
        schema.add(schemaEntry(
            "proposalQueueTuning.restore_timeout_ms",
            "long",
            300000L,
            "runtime-readable",
            "guarded",
            "Proposal restore timeout",
            "oak.proposal.restore.timeout.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.verifier_threads",
            "int",
            1,
            "runtime-readable",
            "expert-only",
            "Verifier worker thread count",
            "oak.proposal.verifier.threads"));
        schema.add(schemaEntry(
            "proposalQueueTuning.processed_retention_ms",
            "long",
            600000L,
            "runtime-readable",
            "guarded",
            "Retention of processed proposal ids",
            "oak.proposal.processed.retention.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.backpressure_park_nanos",
            "long",
            1000000L,
            "runtime-readable",
            "expert-only",
            "Backpressure park interval",
            "oak.consensus.backpressure.park.nanos"));
        schema.add(schemaEntry(
            "proposalQueueTuning.counter_rotation_interval_ms",
            "long",
            86400000L,
            "runtime-readable",
            "guarded",
            "Counter rotation interval",
            "oak.proposal.counter.rotation.ms"));
        schema.add(schemaEntry(
            "proposalQueueTuning.release_mode",
            "string",
            "adaptive-active",
            "runtime-readable",
            "guarded",
            "Verified release pipeline mode (adaptive-shadow or adaptive-active; epoch is accepted as a deprecated alias for adaptive-active)",
            "oak.proposal.release.mode"));
        schema.add(schemaEntry(
            "proposalQueueTuning.validator_hosted_binary_upload_enabled",
            "boolean",
            true,
            "runtime-readable",
            "guarded",
            "Enable validator-hosted binary upload handling",
            "oak.proposal.validator.binary.upload.enabled"));

        schema.add(schemaEntry(
            "rateLimiterTuning.enabled",
            "boolean",
            true,
            "startup-only",
            "safe",
            "Enable HTTP rate limiting",
            RateLimiter.PROP_ENABLED));
        schema.add(schemaEntry(
            "rateLimiterTuning.requests_per_second",
            "int",
            100,
            "startup-only",
            "guarded",
            "Per-client request budget",
            RateLimiter.PROP_REQUESTS_PER_SECOND));
        schema.add(schemaEntry(
            "rateLimiterTuning.write_rps",
            "int",
            10,
            "startup-only",
            "guarded",
            "Per-wallet write budget",
            RateLimiter.PROP_WRITE_RPS));
        schema.add(schemaEntry(
            "rateLimiterTuning.burst_size",
            "int",
            200,
            "startup-only",
            "guarded",
            "Per-client burst size",
            RateLimiter.PROP_BURST_SIZE));
        schema.add(schemaEntry(
            "rateLimiterTuning.global_rps",
            "int",
            1000,
            "startup-only",
            "guarded",
            "Global request budget",
            RateLimiter.PROP_GLOBAL_RPS));
        schema.add(schemaEntry(
            "rateLimiterTuning.warn_logging_enabled",
            "boolean",
            true,
            "startup-only",
            "safe",
            "Enable throttling warning logs",
            RateLimiter.PROP_WARN_LOGGING_ENABLED));
        schema.add(schemaEntry(
            "rateLimiterTuning.warn_log_interval_ms",
            "long",
            30000L,
            "startup-only",
            "safe",
            "Warning log rate-limit interval",
            RateLimiter.PROP_WARN_LOG_INTERVAL_MS));
        schema.add(schemaEntry(
            "rateLimiterTuning.warn_log_sample_size",
            "int",
            250,
            "startup-only",
            "safe",
            "Minimum throttles before warning log",
            RateLimiter.PROP_WARN_LOG_SAMPLE_SIZE));

        schema.add(schemaEntry(
            "tlsTuning.enabled",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Enable TLS listener",
            TlsConfiguration.PROP_TLS_ENABLED));
        schema.add(schemaEntry(
            "tlsTuning.keystore_type",
            "string",
            "PKCS12",
            "startup-only",
            "guarded",
            "TLS keystore format",
            TlsConfiguration.PROP_KEYSTORE_TYPE));
        schema.add(schemaEntry(
            "tlsTuning.keystore_path_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether TLS keystore path is configured",
            TlsConfiguration.PROP_KEYSTORE_PATH));
        schema.add(schemaEntry(
            "tlsTuning.truststore_path_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether TLS truststore path is configured",
            TlsConfiguration.PROP_TRUSTSTORE_PATH));
        schema.add(schemaEntry(
            "tlsTuning.client_auth",
            "string",
            "none",
            "startup-only",
            "expert-only",
            "TLS client auth mode",
            TlsConfiguration.PROP_CLIENT_AUTH));
        schema.add(schemaEntry(
            "tlsTuning.protocols",
            "string",
            "TLSv1.2,TLSv1.3",
            "startup-only",
            "guarded",
            "Allowed TLS protocol list",
            TlsConfiguration.PROP_PROTOCOLS));
        schema.add(schemaEntry(
            "tlsTuning.ciphers_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether explicit cipher suites are configured",
            TlsConfiguration.PROP_CIPHERS));

        schema.add(schemaEntry(
            "tokenAuthTuning.auth_token_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether API auth token is configured",
            AuthTokenValidator.TOKEN_PROPERTY_NAME + "|" + AuthTokenValidator.TOKEN_ENV_VAR_NAME));

        schema.add(schemaEntry(
            "fileStoreFlushTuning.flush_interval_ms",
            "long",
            250L,
            "startup-only",
            "guarded",
            "FileStore async flush interval",
            "oak.filestore.flush.ms"));
        schema.add(schemaEntry(
            "fileStoreFlushTuning.flush_batch",
            "int",
            100,
            "startup-only",
            "guarded",
            "FileStore async flush batch threshold",
            "oak.filestore.flush.batch"));

        schema.add(schemaEntry(
            "gcEconomicsTuning.usdc_per_mb",
            "string",
            "0.10",
            "runtime-readable",
            "guarded",
            "Mock GC cost per MB",
            "gc.usdc.per.mb"));
        schema.add(schemaEntry(
            "blockchainTuning.mode",
            "string",
            "mock",
            "startup-only",
            "guarded",
            "Blockchain mode override",
            "oak.blockchain.mode"));
        schema.add(schemaEntry(
            "blockchainTuning.contract_address",
            "string",
            "",
            "startup-only",
            "guarded",
            "Blockchain contract address override",
            "oak.blockchain.contractAddress"));
        schema.add(schemaEntry(
            "blockchainTuning.rpc_url_configured",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Whether RPC URL is configured via OSGi/env/system",
            "oak.blockchain.rpcUrl"));
        schema.add(schemaEntry(
            "blockchainTuning.gas_price_gwei",
            "long",
            3L,
            "runtime-readable",
            "guarded",
            "Gas price assumption for write estimate math",
            "oak.blockchain.gasPriceGwei"));
        schema.add(schemaEntry(
            "blockchainTuning.gas_write_standard",
            "long",
            74534L,
            "runtime-readable",
            "guarded",
            "Measured gas units for STANDARD write path",
            "oak.blockchain.gas.write.standard"));
        schema.add(schemaEntry(
            "blockchainTuning.gas_write_express",
            "long",
            74534L,
            "runtime-readable",
            "guarded",
            "Measured gas units for EXPRESS write path",
            "oak.blockchain.gas.write.express"));
        schema.add(schemaEntry(
            "blockchainTuning.gas_write_priority",
            "long",
            74534L,
            "runtime-readable",
            "guarded",
            "Measured gas units for PRIORITY write path",
            "oak.blockchain.gas.write.priority"));

        schema.add(schemaEntry(
            "nodeRuntimeTuning.consensus_enabled",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Enable consensus startup",
            "consensus.enabled"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.consensus_mode",
            "string",
            "aeron",
            "startup-only",
            "guarded",
            "Consensus mode selector",
            "consensus.mode"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.wallet_keystore_path_configured",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Whether node wallet keystore path is configured",
            "wallet.keystore.path"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.standby_bootstrap_enabled",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Enable pre-cluster standby bootstrap",
            "consensus.aeron.standby.bootstrap.enabled"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.bootstrap_primary_host_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether bootstrap primary host is configured",
            "bootstrap.primary.host"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.bootstrap_primary_port",
            "int",
            0,
            "startup-only",
            "expert-only",
            "Bootstrap primary standby port",
            "bootstrap.primary.port"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.blobstore_type",
            "string",
            "",
            "startup-only",
            "guarded",
            "BlobStore backend type",
            "blobstore.type|BLOBSTORE_TYPE"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.ipfs_api_endpoint_configured",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Whether IPFS API endpoint is configured",
            "ipfs.api.endpoint|IPFS_API_ENDPOINT"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.http_port",
            "int",
            0,
            "startup-only",
            "safe",
            "Optional plain HTTP port when TLS is enabled",
            "http.port"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.sharding_num_shards",
            "int",
            1,
            "startup-only",
            "guarded",
            "Configured shard count",
            "sharding.numShards|NUM_SHARDS"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.mock_epoch_duration_seconds",
            "long",
            300L,
            "runtime-readable",
            "safe",
            "Mock-mode epoch duration in seconds",
            "oak.mock.epoch.duration.seconds|OAK_MOCK_EPOCH_DURATION_SECONDS"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.aeron_dir_name_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether a custom Aeron directory is configured",
            "aeron.dir.name"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.aeron_cluster_hostnames_configured",
            "boolean",
            false,
            "startup-only",
            "expert-only",
            "Whether explicit Aeron hostnames are configured",
            "aeron.cluster.hostnames"));
        schema.add(schemaEntry(
            "nodeRuntimeTuning.proposal_persistence_dir_configured",
            "boolean",
            false,
            "startup-only",
            "guarded",
            "Whether an explicit proposal persistence directory is configured",
            "oak.proposal.persistence.dir|OAK_PROPOSAL_PERSISTENCE_DIR"));

        schema.add(schemaEntry(
            "runtimeUiTuning.browser_ui_enabled",
            "boolean",
            true,
            "startup-only",
            "safe",
            "Enable in-process browser UI routes",
            "oak.http.browser.ui.enabled"));
        schema.add(schemaEntry(
            "runtimeUiTuning.external_dashboard_url_configured",
            "boolean",
            false,
            "startup-only",
            "safe",
            "Whether external dashboard URL is configured",
            "oak.dashboard.external.url"));
        return schema;
    }

    private Map<String, Object> buildTlsTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("enabled", readBoolean(TlsConfiguration.PROP_TLS_ENABLED, false));
        values.put("keystore_type", readString(TlsConfiguration.PROP_KEYSTORE_TYPE, "PKCS12"));
        values.put("keystore_path_configured", RuntimeConfigValueResolver.hasConfiguredValue(TlsConfiguration.PROP_KEYSTORE_PATH));
        values.put("truststore_path_configured", RuntimeConfigValueResolver.hasConfiguredValue(TlsConfiguration.PROP_TRUSTSTORE_PATH));
        values.put("client_auth", readString(TlsConfiguration.PROP_CLIENT_AUTH, "none"));
        values.put("protocols", readString(TlsConfiguration.PROP_PROTOCOLS, "TLSv1.2,TLSv1.3"));
        values.put("ciphers_configured", RuntimeConfigValueResolver.hasConfiguredValue(TlsConfiguration.PROP_CIPHERS));
        return values;
    }

    private Map<String, Object> buildTokenAuthTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        boolean tokenConfigured = RuntimeConfigValueResolver.hasConfiguredValue(
            AuthTokenValidator.TOKEN_PROPERTY_NAME,
            AuthTokenValidator.TOKEN_ENV_VAR_NAME);
        values.put("auth_token_configured", tokenConfigured);
        return values;
    }

    private Map<String, Object> buildFileStoreFlushTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("flush_interval_ms", readLong("oak.filestore.flush.ms", 250L));
        values.put("flush_batch", readInt("oak.filestore.flush.batch", 100));
        return values;
    }

    private Map<String, Object> buildGcEconomicsTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("usdc_per_mb", readString("gc.usdc.per.mb", "0.10"));
        return values;
    }

    private Map<String, Object> buildRuntimeUiTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("browser_ui_enabled", readBoolean("oak.http.browser.ui.enabled", true));
        values.put("external_dashboard_url_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("oak.dashboard.external.url"));
        return values;
    }

    private Map<String, Object> buildNodeRuntimeTuning() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("consensus_enabled", readBoolean("consensus.enabled", false));
        values.put("consensus_mode", readString("consensus.mode", "aeron"));
        values.put("wallet_keystore_path_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("wallet.keystore.path"));
        values.put("standby_bootstrap_enabled",
            readBoolean("consensus.aeron.standby.bootstrap.enabled", false));
        values.put("bootstrap_primary_host_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("bootstrap.primary.host"));
        values.put("bootstrap_primary_port", readInt("bootstrap.primary.port", 0));
        values.put("blobstore_type",
            RuntimeConfigValueResolver.readString("blobstore.type", "BLOBSTORE_TYPE", ""));
        values.put("ipfs_api_endpoint_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("ipfs.api.endpoint", "IPFS_API_ENDPOINT"));
        values.put("http_port", readInt("http.port", 0));
        values.put("sharding_num_shards",
            RuntimeConfigValueResolver.readInt("sharding.numShards", "NUM_SHARDS", 1));
        values.put("mock_epoch_duration_seconds",
            RuntimeConfigValueResolver.readLongEnvFirst(
                "oak.mock.epoch.duration.seconds",
                "OAK_MOCK_EPOCH_DURATION_SECONDS",
                300L));
        values.put("aeron_dir_name_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("aeron.dir.name"));
        values.put("aeron_cluster_hostnames_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("aeron.cluster.hostnames"));
        values.put("proposal_persistence_dir_configured",
            RuntimeConfigValueResolver.hasConfiguredValue("oak.proposal.persistence.dir", "OAK_PROPOSAL_PERSISTENCE_DIR"));
        return values;
    }

    private Set<String> buildKnownTunables() {
        Set<String> known = new LinkedHashSet<>();
        for (Map<String, Object> entry : buildSchema()) {
            Object raw = entry.get("key");
            if (raw != null) {
                known.add(String.valueOf(raw));
            }
        }
        return known;
    }

    private int countCsv(String raw) {
        if (!hasText(raw)) {
            return 0;
        }
        int count = 0;
        String[] split = raw.split(",");
        for (String token : split) {
            if (hasText(token)) {
                count++;
            }
        }
        return count;
    }

    private static boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static Map<String, Object> flattenComponents(Map<String, Object> components) {
        Map<String, Object> flat = new LinkedHashMap<>();
        for (Map.Entry<String, Object> component : components.entrySet()) {
            String componentName = component.getKey();
            Object rawValue = component.getValue();
            if (!(rawValue instanceof Map)) {
                continue;
            }
            Map<?, ?> values = (Map<?, ?>) rawValue;
            for (Map.Entry<?, ?> valueEntry : values.entrySet()) {
                if (valueEntry.getKey() == null) {
                    continue;
                }
                flat.put(componentName + "." + valueEntry.getKey(), valueEntry.getValue());
            }
        }
        return flat;
    }

    private static boolean looselyEqual(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if ("auto".equals(String.valueOf(b))) {
            // "auto" means platform-managed default, not a concrete numeric target.
            return true;
        }
        if (a instanceof Number && b instanceof Number) {
            double da = ((Number) a).doubleValue();
            double db = ((Number) b).doubleValue();
            return Double.compare(da, db) == 0;
        }
        return String.valueOf(a).equals(String.valueOf(b));
    }

    private static int countRisk(List<Map<String, Object>> rows, String risk) {
        int count = 0;
        for (Map<String, Object> row : rows) {
            Object raw = row.get("risk");
            if (raw != null && risk.equals(String.valueOf(raw))) {
                count++;
            }
        }
        return count;
    }

    private static String justificationFor(String key, Object currentValue, Object defaultValue) {
        if ("proposalQueueTuning.verifier_threads".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Raised to reduce verifier queue pressure and mempool buildup during sustained load.";
        }
        if ("proposalQueueTuning.finalization_chunk_size".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Adjusted to increase per-cycle verified release throughput under backlog.";
        }
        if ("proposalQueueTuning.finalization_chunk_delay_ms".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Adjusted to control optional pacing between verified release chunks.";
        }
        if ("proposalQueueTuning.max_message_batch".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Adjusted to tune Aeron sender batching and reduce queue drain latency.";
        }
        if ("proposalQueueTuning.persistence_flush_batch".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Adjusted to amortize flush I/O under write pressure.";
        }
        if ("proposalQueueTuning.persistence_flush_interval_ms".equals(key)
            && !looselyEqual(currentValue, defaultValue)) {
            return "Adjusted to balance durability latency against flush overhead.";
        }
        return null;
    }

    private static int readInt(String key, int defaultValue) {
        return RuntimeConfigValueResolver.readInt(key, defaultValue);
    }

    private static long readLong(String key, long defaultValue) {
        return RuntimeConfigValueResolver.readLong(key, defaultValue);
    }

    private static boolean readBoolean(String key, boolean defaultValue) {
        return RuntimeConfigValueResolver.readBoolean(key, defaultValue);
    }

    private static String readString(String key, String defaultValue) {
        return RuntimeConfigValueResolver.readString(key, defaultValue);
    }

    private Map<String, Object> schemaEntry(String key,
                                            String type,
                                            Object defaultValue,
                                            String reloadMode,
                                            String risk,
                                            String description,
                                            String systemPropertyAlias) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("key", key);
        entry.put("type", type);
        entry.put("default", defaultValue);
        entry.put("reloadMode", reloadMode);
        entry.put("risk", risk);
        entry.put("description", description);
        entry.put("systemPropertyAlias", systemPropertyAlias);
        return entry;
    }
}
