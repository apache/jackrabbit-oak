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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only introspection helpers for effective Aeron cluster tuning.
 */
public final class AeronClusterTuningIntrospection {

    private AeronClusterTuningIntrospection() {
    }

    public static Map<String, Object> effectiveValues() {
        Map<String, Object> values = new LinkedHashMap<>();
        AeronClusterRuntimeRegistry.Snapshot snapshot = AeronClusterRuntimeRegistry.snapshot();
        String effectiveSelfUrl = snapshot.selfUrl != null
            ? snapshot.selfUrl
            : trimToNull(readString("consensus.self.url", null));
        int effectivePeerCount = snapshot.peerUrls.isEmpty()
            ? countConfiguredValues(readString("consensus.peers", ""))
            : snapshot.peerUrls.size();
        values.put("enabled", readBoolean("consensus.enabled", snapshot.enabled));
        values.put("node_id", readInt("aeron.cluster.nodeId", snapshot.nodeId));
        values.put("self_url_configured", effectiveSelfUrl != null);
        values.put("peer_urls_count", effectivePeerCount);
        values.put("observe_elections", snapshot.observeElections);
        values.put("log_cluster_state_details", snapshot.logClusterStateDetails);
        values.put("cluster_base_port", readInt(AeronClusterTopology.PORT_BASE_PROPERTY, AeronClusterTopology.getPortBase()));
        values.put("cluster_environment", readString("oak.cluster.environment", ""));
        values.put("session_timeout_minutes", readInt("oak.cluster.session.timeout.minutes", 0));
        values.put("media_driver_timeout_ms", readInt("oak.cluster.media.driver.timeout.ms", 0));
        values.put("socket_send_buffer_bytes", readInt("aeron.socket.so_sndbuf", 0));
        values.put("socket_receive_buffer_bytes", readInt("aeron.socket.so_rcvbuf", 0));
        values.put("publication_term_buffer_length_bytes", readInt("oak.cluster.publication.term.buffer.length.bytes", 0));
        values.put("cluster_term_length_bytes", readInt("oak.cluster.term.length.bytes", 0));
        values.put("heartbeat_max_age_ms", readLong("oak.cluster.heartbeat.maxAgeMs", 0L));
        values.put("reachability_cache_ms", readLong("oak.cluster.reachability.cacheMs", 0L));
        values.put("reachability_connect_timeout_ms", readInt("oak.cluster.reachability.connectTimeoutMs", 0));
        values.put("reachability_read_timeout_ms", readInt("oak.cluster.reachability.readTimeoutMs", 0));
        values.put("reconnect_max_attempts", readInt("oak.cluster.reconnect.maxAttempts", 0));
        values.put("max_concurrent_sessions", readInt("oak.cluster.max.concurrent.sessions", 0));
        values.put("peer_probe_mode", readString("oak.health.peerProbeMode", ""));
        values.put("delete_aeron_dirs_on_startup", readBoolean("aeron.delete.dirs.on.startup", false));
        values.put("beacon_api_url", readString("ethereum.beacon.api.url", "https://beaconcha.in/api"));
        return values;
    }

    public static String source() {
        return AeronClusterTuningSourceRegistry.getSource();
    }

    private static int readInt(String key, int defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long readLong(String key, long defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean readBoolean(String key, boolean defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    private static String readString(String key, String defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null) {
            return defaultValue;
        }
        return raw;
    }

    private static int countConfiguredValues(String csv) {
        if (csv == null || csv.trim().isEmpty()) {
            return 0;
        }
        int count = 0;
        for (String value : csv.split(",")) {
            if (!value.trim().isEmpty()) {
                count++;
            }
        }
        return count;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
