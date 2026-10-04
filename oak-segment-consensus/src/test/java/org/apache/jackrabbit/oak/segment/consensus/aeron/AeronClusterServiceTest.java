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

import org.apache.jackrabbit.oak.segment.consensus.server.AeronClusterStartupResult;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AeronClusterServiceTest {

    private static final String[] PROPERTIES_TO_CLEAR = new String[] {
        "aeron.cluster.nodeId",
        "ethereum.beacon.api.url",
        "oak.cluster.environment",
        "oak.cluster.session.timeout.minutes",
        "oak.cluster.media.driver.timeout.ms",
        "aeron.socket.so_sndbuf",
        "aeron.socket.so_rcvbuf",
        "oak.cluster.publication.term.buffer.length.bytes",
        "oak.cluster.term.length.bytes",
        "oak.cluster.heartbeat.maxAgeMs",
        "oak.cluster.reachability.cacheMs",
        "oak.cluster.reachability.connectTimeoutMs",
        "oak.cluster.reachability.readTimeoutMs",
        "oak.cluster.reconnect.maxAttempts",
        "oak.health.peerProbeMode",
        "aeron.delete.dirs.on.startup",
        "aeron.cluster.hostnames"
    };

    @After
    public void tearDown() throws Exception {
        for (String property : PROPERTIES_TO_CLEAR) {
            System.clearProperty(property);
        }
        AeronClusterRuntimeRegistry.clear();
        resetTuningSource();
    }

    @Test
    public void testActivatePublishesRuntimeSnapshotAndEnabledState() {
        AeronClusterService service = new AeronClusterService();
        AeronClusterConfig config = newConfig();

        service.activate(config);

        AeronClusterRuntimeRegistry.Snapshot snapshot = AeronClusterRuntimeRegistry.snapshot();
        assertTrue(service.isEnabled());
        assertEquals(2, snapshot.nodeId);
        assertEquals("http://validator-0:8090", snapshot.selfUrl);
        assertEquals(Arrays.asList("http://validator-1:8090", "http://validator-2:8090"), snapshot.peerUrls);
        assertFalse(snapshot.observeElections);
        assertTrue(snapshot.logClusterStateDetails);
        assertEquals("osgi-config-admin", AeronClusterTuningSourceRegistry.getSource());
    }

    @Test
    public void testApplyConfigToSystemPropertiesPublishesHostnamesAndTuningSettings() throws Exception {
        AeronClusterService service = new AeronClusterService();
        AeronClusterConfig config = newConfig();
        service.activate(config);

        invokeApplyConfig(service, "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"));

        assertEquals("2", System.getProperty("aeron.cluster.nodeId"));
        assertEquals("https://beacon.example", System.getProperty("ethereum.beacon.api.url"));
        assertEquals("staging", System.getProperty("oak.cluster.environment"));
        assertEquals("15", System.getProperty("oak.cluster.session.timeout.minutes"));
        assertEquals("2000", System.getProperty("oak.cluster.media.driver.timeout.ms"));
        assertEquals("1024", System.getProperty("aeron.socket.so_sndbuf"));
        assertEquals("2048", System.getProperty("aeron.socket.so_rcvbuf"));
        assertEquals("4096", System.getProperty("oak.cluster.publication.term.buffer.length.bytes"));
        assertEquals("8192", System.getProperty("oak.cluster.term.length.bytes"));
        assertEquals("30000", System.getProperty("oak.cluster.heartbeat.maxAgeMs"));
        assertEquals("60000", System.getProperty("oak.cluster.reachability.cacheMs"));
        assertEquals("7000", System.getProperty("oak.cluster.reachability.connectTimeoutMs"));
        assertEquals("8000", System.getProperty("oak.cluster.reachability.readTimeoutMs"));
        assertEquals("9", System.getProperty("oak.cluster.reconnect.maxAttempts"));
        assertEquals("http", System.getProperty("oak.health.peerProbeMode"));
        assertEquals("true", System.getProperty("aeron.delete.dirs.on.startup"));
        assertEquals("validator-0,validator-1,validator-2", System.getProperty("aeron.cluster.hostnames"));
    }

    @Test
    public void testApplyConfigToSystemPropertiesDoesNotOverrideExplicitHostnames() throws Exception {
        AeronClusterService service = new AeronClusterService();
        service.activate(newConfig());
        System.setProperty("aeron.cluster.hostnames", "preconfigured-hosts");

        invokeApplyConfig(service, "http://validator-0:8090",
            Arrays.asList("http://validator-1:8090", "http://validator-2:8090"));

        assertEquals("preconfigured-hosts", System.getProperty("aeron.cluster.hostnames"));
    }

    @Test
    public void testDeactivateShutsDownLauncherAndClearsRuntimeSnapshot() throws Exception {
        AeronClusterService service = new AeronClusterService();
        AeronClusterLauncher launcher = mock(AeronClusterLauncher.class);

        service.activate(newConfig());
        setField(service, "startupResult",
            new AeronClusterStartupResult(null, launcher, Collections.singletonList("validator-0"), 0));

        service.deactivate();

        verify(launcher).shutdown();
        AeronClusterRuntimeRegistry.Snapshot snapshot = AeronClusterRuntimeRegistry.snapshot();
        assertTrue(snapshot.enabled);
        assertEquals(0, snapshot.nodeId);
        assertNull(snapshot.selfUrl);
        assertTrue(snapshot.peerUrls.isEmpty());
        assertTrue(snapshot.observeElections);
        assertFalse(snapshot.logClusterStateDetails);
    }

    private static AeronClusterConfig newConfig() {
        AeronClusterConfig config = mock(AeronClusterConfig.class);
        when(config.enabled()).thenReturn(true);
        when(config.nodeId()).thenReturn(2);
        when(config.selfUrl()).thenReturn("http://validator-0:8090");
        when(config.peerUrls()).thenReturn(new String[] {"http://validator-1:8090", "http://validator-2:8090"});
        when(config.observeElections()).thenReturn(false);
        when(config.logClusterStateDetails()).thenReturn(true);
        when(config.beaconApiUrl()).thenReturn("https://beacon.example");
        when(config.clusterEnvironment()).thenReturn("staging");
        when(config.sessionTimeoutMinutes()).thenReturn(15);
        when(config.mediaDriverTimeoutMs()).thenReturn(2000);
        when(config.socketSendBufferBytes()).thenReturn(1024);
        when(config.socketReceiveBufferBytes()).thenReturn(2048);
        when(config.publicationTermBufferLengthBytes()).thenReturn(4096);
        when(config.clusterTermLengthBytes()).thenReturn(8192);
        when(config.heartbeatMaxAgeMs()).thenReturn(30000L);
        when(config.reachabilityCacheMs()).thenReturn(60000L);
        when(config.reachabilityConnectTimeoutMs()).thenReturn(7000);
        when(config.reachabilityReadTimeoutMs()).thenReturn(8000);
        when(config.reconnectMaxAttempts()).thenReturn(9);
        when(config.peerProbeMode()).thenReturn("http");
        when(config.deleteAeronDirsOnStartup()).thenReturn(true);
        return config;
    }

    private static void invokeApplyConfig(AeronClusterService service, String selfUrl, java.util.List<String> peerUrls)
        throws Exception {
        Method method = AeronClusterService.class.getDeclaredMethod(
            "applyConfigToSystemProperties", String.class, java.util.List.class);
        method.setAccessible(true);
        method.invoke(service, selfUrl, peerUrls);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = AeronClusterService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private static void resetTuningSource() throws Exception {
        Field field = AeronClusterTuningSourceRegistry.class.getDeclaredField("SOURCE");
        field.setAccessible(true);
        AtomicReference<String> source = (AtomicReference<String>) field.get(null);
        source.set("system-properties");
    }
}
