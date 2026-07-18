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

import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterLauncher;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronPrometheusMetrics;
import org.apache.jackrabbit.oak.segment.consensus.aeron.CrashHandler;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.metrics.ConsensusMetrics;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.junit.Test;
import io.prometheus.client.exporter.common.TextFormat;

import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class MetricsHandlerTest {

    @Test
    public void testHandleMetricsWithEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getCurrentRole()).thenReturn(ValidatorRole.LEADER);
        when(engine.isLeader()).thenReturn(true);
        when(engine.getCurrentEpoch()).thenReturn(42);
        when(engine.getCurrentTerm()).thenReturn(7);
        when(engine.getReachableValidatorCount()).thenReturn(2);
        when(engine.getTotalMemberCount()).thenReturn(3);
        when(engine.getQuorumSize()).thenReturn(2);
        when(engine.getHeartbeatAgeMs()).thenReturn(123L);
        when(engine.isClusterHealthy()).thenReturn(true);
        when(engine.getUnhealthyReason()).thenReturn(null);
        Map<String, Object> lagStatus = new HashMap<>();
        lagStatus.put("role", "LEADER");
        lagStatus.put("myLogPosition", 10L);
        lagStatus.put("leaderLogPosition", 10L);
        lagStatus.put("replicationLag", 0L);
        lagStatus.put("lagThreshold", 5L);
        lagStatus.put("healthy", true);
        when(engine.getReplicationLagStatus()).thenReturn(lagStatus);

        MetricsHandler handler = new MetricsHandler(
            engine,
            Paths.get("/tmp/store"),
            Collections.singletonMap("c1", new Object()),
            Collections.singletonMap("v1", new Object()),
            new ServerContext(null, null, Paths.get("/tmp/store"), "http://localhost:8090")
        );

        handler.handleMetrics(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"metrics.json.v1\""));
        assertTrue(json.contains("\"role\":\"LEADER\""));
        assertTrue(json.contains("\"reachableValidators\":2"));
        assertTrue(json.contains("\"replicationLag\":0"));
        assertTrue(json.contains("\"registeredClients\":1"));
        assertTrue(json.contains("\"registeredValidators\":1"));
    }

    @Test
    public void testHandleMetricsWithoutEngine() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        MetricsHandler handler = new MetricsHandler(
            null,
            Paths.get("/tmp/store"),
            Collections.emptyMap(),
            Collections.emptyMap(),
            new ServerContext(null, null, Paths.get("/tmp/store"), "http://localhost:8090")
        );

        handler.handleMetrics(response);

        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"metrics.json.v1\""));
        assertTrue(json.contains("\"consensus\":null"));
        assertTrue(json.contains("\"replication\":null"));
    }

    @Test
    public void testHandleMetricsIncludesUnhealthyReasonReplicationReasonAndIpfsPolicy() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));

        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
        when(engine.isLeader()).thenReturn(false);
        when(engine.getCurrentEpoch()).thenReturn(9);
        when(engine.getCurrentTerm()).thenReturn(3);
        when(engine.getReachableValidatorCount()).thenReturn(1);
        when(engine.getTotalMemberCount()).thenReturn(3);
        when(engine.getQuorumSize()).thenReturn(2);
        when(engine.getHeartbeatAgeMs()).thenReturn(999L);
        when(engine.isClusterHealthy()).thenReturn(false);
        when(engine.getUnhealthyReason()).thenReturn("leader_unreachable");

        Map<String, Object> lagStatus = new HashMap<>();
        lagStatus.put("role", "FOLLOWER");
        lagStatus.put("myLogPosition", 11L);
        lagStatus.put("leaderLogPosition", 15L);
        lagStatus.put("replicationLag", 4L);
        lagStatus.put("lagThreshold", 2L);
        lagStatus.put("healthy", false);
        lagStatus.put("reason", "lagging");
        when(engine.getReplicationLagStatus()).thenReturn(lagStatus);

        ServerContext context = new ServerContext(null, null, Paths.get("/tmp/store"), "http://localhost:8090");
        context.apiIpfsPolicyRejectAmbiguousSource.set(2);
        context.apiIpfsPolicyRejectNonEnterpriseCid.set(3);
        context.apiIpfsPolicyRejectUnknownCid.set(5);
        context.apiIpfsPolicyRejectCidServiceUnavailable.set(7);
        context.apiIpfsPolicyAcceptedEnterpriseCid.set(11);

        MetricsHandler handler = new MetricsHandler(
            engine,
            null,
            Collections.emptyMap(),
            Collections.emptyMap(),
            context
        );

        handler.handleMetrics(response);

        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"metrics.json.v1\""));
        assertTrue(json.contains("\"unhealthyReason\":\"leader_unreachable\""));
        assertTrue(json.contains("\"reason\":\"lagging\""));
        assertTrue(json.contains("\"storePath\":\"\""));
        assertTrue(json.contains("\"acceptedEnterpriseCid\":11"));
        assertTrue(json.contains("\"rejectedUnknownCid\":5"));
    }

    @Test
    public void testHandlePrometheusMetricsExportsDynamicGaugeValues() throws Exception {
        Path storeDir = Files.createTempDirectory("metrics-handler");
        try {
            Files.write(storeDir.resolve("data00000a.tar"), new byte[] {1, 2, 3});
            Files.write(storeDir.resolve("data00001a.tar"), new byte[] {4});
            Files.createDirectories(storeDir.resolve("nested"));
            Files.write(storeDir.resolve("nested").resolve("notes.txt"), new byte[] {5, 6});

            StringWriter body = new StringWriter();
            HttpServletResponse response = mock(HttpServletResponse.class);
            when(response.getWriter()).thenReturn(new PrintWriter(body));

            AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
            when(engine.isLeader()).thenReturn(true);
            when(engine.getCurrentEpoch()).thenReturn(17);
            when(engine.getReachableValidatorCount()).thenReturn(4);
            when(engine.getLastHeartbeatTime()).thenReturn(System.currentTimeMillis() - 2_000L);

            CrashHandler crashHandler = mock(CrashHandler.class);
            when(crashHandler.getCrashCount()).thenReturn(3);
            when(crashHandler.hasCrashed()).thenReturn(true);
            when(crashHandler.shouldForceBootstrap()).thenReturn(true);

            AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);
            when(launcher.getCrashHandler()).thenReturn(crashHandler);

            AeronPrometheusMetrics prometheusMetrics = mock(AeronPrometheusMetrics.class);

            ServerContext context = new ServerContext(null, null, storeDir, "http://localhost:8090");
            context.aeronClusterLauncher = launcher;
            context.aeronPrometheusMetrics = prometheusMetrics;

            MetricsHandler handler = new MetricsHandler(
                null,
                storeDir,
                Collections.singletonMap("c1", new Object()),
                Map.of("v1", new Object(), "v2", new Object()),
                context
            );
            context.setAeronConsensusEngine(engine);

            handler.handlePrometheusMetrics(response);

            verify(response).setContentType(TextFormat.CONTENT_TYPE_004);
            verify(response).setStatus(HttpServletResponse.SC_OK);
            verify(prometheusMetrics).updateGaugeValues();

            String metrics = body.toString();
            assertTrue(metrics.contains("oak_consensus_is_leader 1.0"));
            assertTrue(metrics.contains("oak_consensus_leader_epoch 17.0"));
            assertTrue(metrics.contains("oak_validators_reachable 4.0"));
            assertTrue(metrics.contains("oak_segments_stored_total 2.0"));
            assertTrue(metrics.contains("oak_segments_disk_usage_bytes 4.0"));
            assertTrue(metrics.contains("oak_active_connections 3.0"));
            assertTrue(metrics.contains("oak_mediadriver_crash_count 3.0"));
            assertTrue(metrics.contains("oak_mediadriver_has_crashed 1.0"));
            assertTrue(metrics.contains("oak_mediadriver_force_bootstrap 1.0"));
            assertTrue(metrics.contains("oak_consensus_time_since_last_heartbeat_seconds"));
        } finally {
            deleteRecursively(storeDir);
        }
    }

    @Test
    public void testHandlePrometheusMetricsResetsCrashMetricsAndIgnoresAeronMetricErrors() throws Exception {
        Path storeDir = Files.createTempDirectory("metrics-handler-empty");
        try {
            StringWriter body = new StringWriter();
            HttpServletResponse response = mock(HttpServletResponse.class);
            when(response.getWriter()).thenReturn(new PrintWriter(body));

            ConsensusMetrics.mediaDriverCrashCount.set(9);
            ConsensusMetrics.mediaDriverHasCrashed.set(1);
            ConsensusMetrics.mediaDriverForceBootstrap.set(1);

            AeronPrometheusMetrics prometheusMetrics = mock(AeronPrometheusMetrics.class);
            doThrow(new RuntimeException("boom")).when(prometheusMetrics).updateGaugeValues();

            ServerContext context = new ServerContext(null, null, storeDir.resolve("missing"), "http://localhost:8090");
            context.aeronPrometheusMetrics = prometheusMetrics;

            MetricsHandler handler = new MetricsHandler(
                null,
                context.storeDirectory,
                Collections.emptyMap(),
                Collections.emptyMap(),
                context
            );

            handler.handlePrometheusMetrics(response);

            String metrics = body.toString();
            assertTrue(metrics.contains("oak_mediadriver_crash_count 0.0"));
            assertTrue(metrics.contains("oak_mediadriver_has_crashed 0.0"));
            assertTrue(metrics.contains("oak_mediadriver_force_bootstrap 0.0"));
            assertTrue(metrics.contains("oak_active_connections 0.0"));
        } finally {
            deleteRecursively(storeDir);
        }
    }

    private static void deleteRecursively(Path path) throws Exception {
        if (!Files.exists(path)) {
            return;
        }
        try (java.util.stream.Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
