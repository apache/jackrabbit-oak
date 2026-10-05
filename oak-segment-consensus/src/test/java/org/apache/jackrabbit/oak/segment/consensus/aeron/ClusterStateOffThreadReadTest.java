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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.aeron.cluster.service.Cluster;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@code Cluster} is owned by the clustered service thread; other threads read role, log position and
 * cluster time from copies the service callbacks publish.
 */
public class ClusterStateOffThreadReadTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void offThreadReadersNeverCallClusterAndSeePublishedState() throws Exception {
        List<String> clusterCalls = new CopyOnWriteArrayList<>();
        Thread serviceThread = Thread.currentThread();
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(0);
        when(cluster.role()).thenAnswer(invocation -> record(clusterCalls, serviceThread, "role", Cluster.Role.LEADER));
        when(cluster.logPosition()).thenAnswer(invocation -> record(clusterCalls, serviceThread, "logPosition", 4_096L));
        when(cluster.time()).thenAnswer(invocation -> record(clusterCalls, serviceThread, "time", 77L));

        System.setProperty("oak.health.peerProbeMode", "none");
        EthereumWallet wallet = mock(EthereumWallet.class);
        when(wallet.getWalletAddress()).thenReturn("0xabc");
        AeronConsensusEngine engine = new AeronConsensusEngine(mock(FileStore.class), mock(NodeStore.class),
            "http://self:18090", Arrays.asList("http://127.0.0.1:18091", "http://127.0.0.1:18092"), wallet,
            tempFolder.newFolder("engine").getAbsolutePath(), null, new SnapshotService(),
            new AeronBackgroundCoordinator());
        setField(engine, "cluster", cluster);
        setField(engine, "genesisVerified", true);

        // Service thread: the callbacks publish what they learn
        engine.onRoleChange(Cluster.Role.LEADER);
        engine.onNewLeadershipTermEvent(3L, 4_096L, 77L, 0L, 0, 1, TimeUnit.MILLISECONDS, 0);
        clusterCalls.clear();

        ExecutorService httpThread = Executors.newSingleThreadExecutor(r -> new Thread(r, "http-reader"));
        try {
            httpThread.submit(() -> {
                assertTrue(engine.isLeader());
                assertEquals(ValidatorRole.LEADER, engine.getCurrentRole());
                assertEquals("http://self:18090", engine.getCurrentLeader());
                assertEquals("http://self:18090", engine.getCurrentLeaderHint());
                assertEquals(0, engine.getLeaderMemberId());
                assertEquals(0L, engine.getReplicationLag());
                assertTrue(engine.isClusterHealthy());
                Map<String, Object> state = engine.getNativeClusterState();
                assertEquals("LEADER", state.get("role"));
                assertEquals(4_096L, state.get("logPosition"));
                assertEquals(77L, state.get("clusterTime"));
                Map<String, Object> lag = engine.getReplicationLagStatus();
                assertEquals(4_096L, lag.get("myLogPosition"));
                return null;
            }).get(10, TimeUnit.SECONDS);
        } finally {
            httpThread.shutdownNow();
        }

        assertEquals("off-thread readers called Cluster", Arrays.asList(), clusterCalls);
    }

    @Test
    public void clusterStateSurfacesTheAppVersionOfTheLatestTermEvent() throws Exception {
        Cluster cluster = mock(Cluster.class);
        when(cluster.memberId()).thenReturn(0);
        System.setProperty("oak.health.peerProbeMode", "none");
        AeronConsensusEngine engine = new AeronConsensusEngine(mock(FileStore.class), mock(NodeStore.class),
            "http://self:18090", Arrays.asList("http://127.0.0.1:18091"), mock(EthereumWallet.class),
            tempFolder.newFolder("engine").getAbsolutePath(), null, new SnapshotService(),
            new AeronBackgroundCoordinator());
        setField(engine, "cluster", cluster);
        engine.onRoleChange(Cluster.Role.FOLLOWER);

        engine.onNewLeadershipTermEvent(5L, 0L, 0L, 0L, 1, 1, TimeUnit.MILLISECONDS,
            org.agrona.SemanticVersion.compose(2, 3, 4));

        assertEquals("2.3.4", engine.getNativeClusterState().get("appVersion"));
    }

    @After
    public void clearProbeMode() {
        System.clearProperty("oak.health.peerProbeMode");
    }

    private static <T> T record(List<String> calls, Thread serviceThread, String method, T value) {
        if (Thread.currentThread() != serviceThread) {
            calls.add(method + "@" + Thread.currentThread().getName());
        }
        return value;
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
