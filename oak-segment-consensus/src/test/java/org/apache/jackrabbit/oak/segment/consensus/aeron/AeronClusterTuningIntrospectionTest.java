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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AeronClusterTuningIntrospectionTest {

    @After
    public void tearDown() throws Exception {
        AeronClusterRuntimeRegistry.clear();
        resetSourceRegistry();
        clearProperties();
    }

    @Test
    public void effectiveValuesReadRuntimeSnapshotAndSystemProperties() {
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.enabled()).thenReturn(false);
        when(config.nodeId()).thenReturn(3);
        when(config.selfUrl()).thenReturn(" http://self ");
        when(config.peerUrls()).thenReturn(new String[] { " http://peer-1 " });
        when(config.observeElections()).thenReturn(false);
        when(config.logClusterStateDetails()).thenReturn(true);
        AeronClusterRuntimeRegistry.update(config);

        System.setProperty(AeronClusterTopology.PORT_BASE_PROPERTY, "9100");
        System.setProperty("oak.cluster.environment", "prod");
        System.setProperty("oak.cluster.session.timeout.minutes", "5");
        System.setProperty("oak.cluster.media.driver.timeout.ms", "60000");
        System.setProperty("aeron.socket.so_sndbuf", "4096");
        System.setProperty("aeron.socket.so_rcvbuf", "8192");
        System.setProperty("oak.cluster.publication.term.buffer.length.bytes", "16384");
        System.setProperty("oak.cluster.term.length.bytes", "32768");
        System.setProperty("oak.cluster.heartbeat.maxAgeMs", "30000");
        System.setProperty("oak.cluster.reachability.cacheMs", "1000");
        System.setProperty("oak.cluster.reachability.connectTimeoutMs", "2000");
        System.setProperty("oak.cluster.reachability.readTimeoutMs", "3000");
        System.setProperty("oak.cluster.reconnect.maxAttempts", "7");
        System.setProperty("oak.health.peerProbeMode", "http");
        System.setProperty("aeron.delete.dirs.on.startup", "true");
        System.setProperty("ethereum.beacon.api.url", "https://example.test/api");

        Map<String, Object> values = AeronClusterTuningIntrospection.effectiveValues();

        assertEquals(false, values.get("enabled"));
        assertEquals(3, values.get("node_id"));
        assertEquals(true, values.get("self_url_configured"));
        assertEquals(1, values.get("peer_urls_count"));
        assertEquals(false, values.get("observe_elections"));
        assertEquals(true, values.get("log_cluster_state_details"));
        assertEquals(9100, values.get("cluster_base_port"));
        assertEquals("prod", values.get("cluster_environment"));
        assertEquals(5, values.get("session_timeout_minutes"));
        assertEquals(60000, values.get("media_driver_timeout_ms"));
        assertEquals(4096, values.get("socket_send_buffer_bytes"));
        assertEquals(8192, values.get("socket_receive_buffer_bytes"));
        assertEquals(16384, values.get("publication_term_buffer_length_bytes"));
        assertEquals(32768, values.get("cluster_term_length_bytes"));
        assertEquals(30000L, values.get("heartbeat_max_age_ms"));
        assertEquals(1000L, values.get("reachability_cache_ms"));
        assertEquals(2000, values.get("reachability_connect_timeout_ms"));
        assertEquals(3000, values.get("reachability_read_timeout_ms"));
        assertEquals(7, values.get("reconnect_max_attempts"));
        assertEquals("http", values.get("peer_probe_mode"));
        assertEquals(true, values.get("delete_aeron_dirs_on_startup"));
        assertEquals("https://example.test/api", values.get("beacon_api_url"));
    }

    @Test
    public void effectiveValuesFallBackForMissingBlankAndInvalidProperties() throws Exception {
        System.setProperty(AeronClusterTopology.PORT_BASE_PROPERTY, "bad");
        System.setProperty("oak.cluster.session.timeout.minutes", "bad");
        System.setProperty("oak.cluster.media.driver.timeout.ms", " ");
        System.setProperty("oak.cluster.heartbeat.maxAgeMs", "bad");
        System.setProperty("oak.cluster.reachability.cacheMs", " ");
        System.setProperty("aeron.delete.dirs.on.startup", " ");
        AeronClusterTuningSourceRegistry.markOsgiSource();

        Map<String, Object> values = AeronClusterTuningIntrospection.effectiveValues();

        assertEquals(9000, values.get("cluster_base_port"));
        assertEquals("", values.get("cluster_environment"));
        assertEquals(0, values.get("session_timeout_minutes"));
        assertEquals(0, values.get("media_driver_timeout_ms"));
        assertEquals(0L, values.get("heartbeat_max_age_ms"));
        assertEquals(0L, values.get("reachability_cache_ms"));
        assertEquals(false, values.get("delete_aeron_dirs_on_startup"));
        assertEquals("https://beaconcha.in/api", values.get("beacon_api_url"));
        assertFalse((Boolean) values.get("self_url_configured"));
        assertEquals(0, values.get("peer_urls_count"));
        assertEquals("osgi-config-admin", AeronClusterTuningIntrospection.source());

        resetSourceRegistry();
    }

    @Test
    public void effectiveValuesUseStandaloneRuntimeTopologyWhenOsgiSnapshotIsEmpty() {
        System.setProperty("consensus.enabled", "true");
        System.setProperty("aeron.cluster.nodeId", "2");
        System.setProperty("consensus.self.url", "http://localhost:8094");
        System.setProperty("consensus.peers", "http://localhost:8090,http://localhost:8092");

        Map<String, Object> values = AeronClusterTuningIntrospection.effectiveValues();

        assertEquals(true, values.get("enabled"));
        assertEquals(2, values.get("node_id"));
        assertEquals(true, values.get("self_url_configured"));
        assertEquals(2, values.get("peer_urls_count"));
    }

    private static void clearProperties() {
        System.clearProperty(AeronClusterTopology.PORT_BASE_PROPERTY);
        System.clearProperty("consensus.enabled");
        System.clearProperty("aeron.cluster.nodeId");
        System.clearProperty("consensus.self.url");
        System.clearProperty("consensus.peers");
        System.clearProperty("oak.cluster.environment");
        System.clearProperty("oak.cluster.session.timeout.minutes");
        System.clearProperty("oak.cluster.media.driver.timeout.ms");
        System.clearProperty("aeron.socket.so_sndbuf");
        System.clearProperty("aeron.socket.so_rcvbuf");
        System.clearProperty("oak.cluster.publication.term.buffer.length.bytes");
        System.clearProperty("oak.cluster.term.length.bytes");
        System.clearProperty("oak.cluster.heartbeat.maxAgeMs");
        System.clearProperty("oak.cluster.reachability.cacheMs");
        System.clearProperty("oak.cluster.reachability.connectTimeoutMs");
        System.clearProperty("oak.cluster.reachability.readTimeoutMs");
        System.clearProperty("oak.cluster.reconnect.maxAttempts");
        System.clearProperty("oak.health.peerProbeMode");
        System.clearProperty("aeron.delete.dirs.on.startup");
        System.clearProperty("ethereum.beacon.api.url");
    }

    private static void resetSourceRegistry() throws Exception {
        Field field = AeronClusterTuningSourceRegistry.class.getDeclaredField("SOURCE");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<String> source = (AtomicReference<String>) field.get(null);
        source.set("system-properties");
    }
}
