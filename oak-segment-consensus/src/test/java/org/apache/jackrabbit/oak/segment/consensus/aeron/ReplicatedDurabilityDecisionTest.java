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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.Cluster;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Durability is decided by every member from the same SEGMENT_PERSISTED log entries.
 */
public class ReplicatedDurabilityDecisionTest {

    private static final List<String> TWO_PEERS = Arrays.asList("http://peer-1:8080", "http://peer-2:8080");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final AeronIngressControlPayloadBuilder payloads = new AeronIngressControlPayloadBuilder();

    @Test
    public void threeMembersReachIdenticalDecisionsAtTheSameLogEntry() throws Exception {
        List<AeronEncodedMessage> log = Arrays.asList(
            persisted("p1", 0, true, "h-p1-0", null),
            persisted("p2", 1, false, null, "disk full"),
            persisted("p1", 0, true, "h-p1-0b", null),   // duplicate ack
            persisted("p1", 2, true, "h-p1-2", null),    // quorum for p1 (2 of 3)
            persisted("p2", 2, false, null, "io"),       // p2 can no longer reach 2 of 3
            persisted("p1", 1, true, "h-p1-1", null),    // late ack after decision
            persisted("p3", 7, true, "h", null),         // not a member
            persisted("p3", 0, true, "h-p3-0", null)
        );

        List<String> member0 = apply(newEngine("m0"), log);
        List<String> member1 = apply(newEngine("m1"), log);
        List<String> member2 = apply(newEngine("m2"), log);

        assertEquals(Arrays.asList("3:p1:DURABLE:h-p1-0", "4:p2:FAILED:disk full"), member0);
        assertEquals(member0, member1);
        assertEquals(member0, member2);
    }

    @Test
    public void replayIntoFreshInstanceGivesTheSameDecisions() throws Exception {
        List<AeronEncodedMessage> log = Arrays.asList(
            persisted("p1", 1, true, "a", null),
            persisted("p1", 2, true, "b", null),
            persisted("p2", 0, true, "c", null),
            persisted("p1", 0, true, "d", null),
            persisted("p2", 1, true, "e", null)
        );

        List<String> live = apply(newEngine("live"), log);
        List<String> replayed = apply(newEngine("replay"), log);

        assertEquals(Arrays.asList("1:p1:DURABLE:a", "4:p2:DURABLE:c"), live);
        assertEquals(live, replayed);
    }

    @Test
    public void quorumOfSegmentPersistedDecidesWithoutAnyAckMessage() throws Exception {
        AeronConsensusEngine leader = newEngine("leader");
        AeronCluster ingress = installLeaderIngress(leader);
        List<String> decisions = new ArrayList<>();
        leader.setDurabilityStatusCallback(recorder(decisions, new int[] {0}));

        dispatch(leader, persisted("p-lost-ack", 0, true, "h1", null));
        dispatch(leader, persisted("p-lost-ack", 1, true, "h1", null));

        assertEquals(Arrays.asList("0:p-lost-ack:DURABLE:h1"), decisions);
        verify(ingress, after(300L).never()).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void legacyAckAndQueueEntriesInAnOldLogAreAcceptedAndIgnored() throws Exception {
        AeronConsensusEngine engine = newEngine("legacy");
        List<String> decisions = new ArrayList<>();
        engine.setDurabilityStatusCallback(recorder(decisions, new int[] {0}));

        assertTrue(dispatch(engine, AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_QUEUE_SEGMENT,
            "{\"proposalId\":\"p\",\"totalMembers\":3,\"requiredAcks\":2}")));
        assertTrue(dispatch(engine, AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_ACK_SEGMENT_PERSISTED,
            "{\"proposalId\":\"p\",\"success\":true,\"totalMembers\":3,\"requiredAcks\":2}")));

        assertTrue("an ACK entry alone decides nothing", decisions.isEmpty());
    }

    @Test
    public void memberDoesNotResendItsAckOnceTheLogHoldsIt() throws Exception {
        AeronConsensusEngine engine = newEngine("member0");
        AeronCluster ingress = installLeaderIngress(engine);

        dispatch(engine, persisted("p-own", 0, true, "h", null));
        assertTrue(engine.sendSegmentPersisted("p-own", "h", true, null));

        dispatch(engine, persisted("p-decided", 1, true, "h", null));
        dispatch(engine, persisted("p-decided", 2, true, "h", null));
        assertTrue(engine.sendSegmentPersisted("p-decided", "h", true, null));

        verify(ingress, after(300L).never()).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    @Test
    public void decisionIsForgottenOnceItsRetentionWindowOfLogTimeHasPassed() throws Exception {
        AeronConsensusEngine engine = newEngine("member0");
        AeronCluster ingress = installLeaderIngress(engine);
        long decidedAt = 1_000L;

        dispatch(engine, persisted("p-old", 1, true, "h", null), decidedAt);
        dispatch(engine, persisted("p-old", 2, true, "h", null), decidedAt);
        dispatch(engine, persisted("p-next", 1, true, "h", null), decidedAt + DurabilityTally.RETENTION_MS - 1);
        assertTrue(engine.sendSegmentPersisted("p-old", "h", true, null));
        verify(ingress, after(300L).never()).offer(any(MutableDirectBuffer.class), eq(0), anyInt());

        dispatch(engine, persisted("p-next", 2, true, "h", null), decidedAt + DurabilityTally.RETENTION_MS);
        assertTrue(engine.sendSegmentPersisted("p-old", "h", true, null));
        verify(ingress, timeout(1_000L)).offer(any(MutableDirectBuffer.class), eq(0), anyInt());
    }

    private AeronEncodedMessage persisted(String proposalId, int memberId, boolean success, String head, String error) {
        return payloads.buildSegmentPersisted(proposalId, memberId, success, head, error);
    }

    private List<String> apply(AeronConsensusEngine engine, List<AeronEncodedMessage> log) throws Exception {
        List<String> decisions = new ArrayList<>();
        int[] position = {0};
        engine.setDurabilityStatusCallback(recorder(decisions, position));
        for (AeronEncodedMessage entry : log) {
            dispatch(engine, entry);
            position[0]++;
        }
        return decisions;
    }

    private static AeronConsensusEngine.DurabilityStatusCallback recorder(List<String> decisions, int[] position) {
        return new AeronConsensusEngine.DurabilityStatusCallback() {
            @Override
            public void onDurable(String proposalId, String durableHead) {
                decisions.add(position[0] + ":" + proposalId + ":DURABLE:" + durableHead);
            }

            @Override
            public void onFailure(String proposalId, String error) {
                decisions.add(position[0] + ":" + proposalId + ":FAILED:" + error);
            }
        };
    }

    private static boolean dispatch(AeronConsensusEngine engine, AeronEncodedMessage entry) throws Exception {
        return dispatch(engine, entry, 1L);
    }

    private static boolean dispatch(AeronConsensusEngine engine, AeronEncodedMessage entry, long clusterTime)
        throws Exception {
        MessageDispatcher dispatcher = (MessageDispatcher) field(engine, "messageDispatcher");
        return dispatcher.dispatch(clusterTime, entry.buffer, 0, entry.totalLength);
    }

    private AeronConsensusEngine newEngine(String name) throws Exception {
        File dir = tempFolder.newFolder(name);
        EthereumWallet wallet = mock(EthereumWallet.class);
        when(wallet.getWalletAddress()).thenReturn("0xabc");
        return new AeronConsensusEngine(mock(FileStore.class), mock(NodeStore.class), "http://self:8080",
            TWO_PEERS, wallet, dir.getAbsolutePath(), null, new SnapshotService(), new AeronBackgroundCoordinator());
    }

    private AeronCluster installLeaderIngress(AeronConsensusEngine engine) throws Exception {
        Cluster cluster = mock(Cluster.class);
        when(cluster.role()).thenReturn(Cluster.Role.LEADER);
        when(cluster.memberId()).thenReturn(0);
        setField(engine, "cluster", cluster);
        setField(engine, "idleStrategy", mock(IdleStrategy.class));

        AeronCluster client = mock(AeronCluster.class);
        when(client.isClosed()).thenReturn(false);
        when(client.sendKeepAlive()).thenReturn(true);
        when(client.offer(any(MutableDirectBuffer.class), eq(0), anyInt())).thenReturn(1L);
        engine.setAeronDirectoryName(tempFolder.getRoot().getAbsolutePath() + "/aeron-test");
        AeronInternalClusterClientConnector connector = mock(AeronInternalClusterClientConnector.class);
        when(connector.connectOnce(any(), any(), any(), any()))
            .thenReturn(AeronInternalClusterClientConnector.ConnectAttemptResult.success(client));
        setField(engine, "internalClusterClientConnector", connector);
        AeronInternalIngressClientManager manager =
            (AeronInternalIngressClientManager) field(engine, "internalIngressClientManager");
        assertTrue(manager.ensureAvailable("test bind", 1500L));
        return client;
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
