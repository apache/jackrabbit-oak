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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.aeron.AeronConsensusEngine;
import org.apache.jackrabbit.oak.segment.consensus.gc.EntityGCAccount;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.leader.ValidatorRole;
import org.apache.jackrabbit.oak.segment.consensus.queue.DurabilityState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalStatus;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.segment.http.server.model.ValidatorRegistration;
import org.apache.jackrabbit.oak.segment.http.server.model.WriteMetadata;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExplorerApiV1HandlerTest {

    private static final String WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String LOCAL_CLUSTER_ID = "local-localhost-8090";
    private static final String REMOTE_CLUSTER_ID = "remote-validator-2-8090";

    @Test
    public void testHandleSummaryIncludesClusterQueueAndIdentityDetails() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(new MemoryNodeStore());
        context.validatorWalletAddress = WALLET;
        context.clusterWalletAddress = "0x9999999999999999999999999999999999999999";
        context.registeredClients.put("c1", new ClientRegistration("c1", "http://client-1:4502", WALLET));
        context.registeredValidators.put("v1", new ValidatorRegistration("validator-1", "http://validator-1:8090"));

        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.getClusterSize()).thenReturn(5);
        when(engine.getReachableValidatorCount()).thenReturn(4);
        when(engine.getCurrentRole()).thenReturn(ValidatorRole.FOLLOWER);
        when(engine.isLeader()).thenReturn(false);
        when(engine.getCurrentLeader()).thenReturn("http://validator-1:8090");
        when(engine.getCurrentTerm()).thenReturn(9);
        when(engine.getCurrentEpoch()).thenReturn(17);
        when(engine.getCurrentEthereumEpoch()).thenReturn(27);
        context.aeronConsensusEngine = engine;

        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        Map<String, Object> queueStats = new LinkedHashMap<>();
        queueStats.put("verifiedCount", 11L);
        queueStats.put("totalFinalizedCount", 8L);
        queueStats.put("batchQueueSize", 3L);
        queueStats.put("pendingCount", 2L);
        queueStats.put("mempoolPendingCount", 1L);
        queueStats.put("rejectedCount", 1L);
        queueStats.put("backpressurePendingCount", 2L);
        queueStats.put("backpressurePendingRawCount", 4L);
        queueStats.put("backpressureMaxPending", 6L);
        queueStats.put("backpressureActive", true);
        queueStats.put("totalProposalsSent", 14L);
        queueStats.put("currentEpoch", 18L);
        queueStats.put("finalizedEpoch", 15L);
        queueStats.put("epochsUntilFinality", 2L);
        when(queueManager.getQueueStats()).thenReturn(queueStats);
        context.proposalQueueManager = queueManager;

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleSummary(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"explorer.v1\""));
        assertTrue(json.contains("\"consensusType\":\"aeron-cluster\""));
        assertTrue(json.contains("\"role\":\"FOLLOWER\""));
        assertTrue(json.contains("\"clusterState\":\"HEALTHY\""));
        assertTrue(json.contains("\"routingDebt\":6"));
        assertTrue(json.contains("\"validatorWalletAddress\":\"" + WALLET + "\""));
        assertTrue(json.contains("\"registeredClients\":1"));
        assertTrue(json.contains("\"registeredValidators\":1"));
    }

    @Test
    public void testHandleSummaryFallsBackToStandaloneDefaults() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(newContext(new MemoryNodeStore()));
        handler.handleSummary(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"consensusType\":\"none\""));
        assertTrue(json.contains("\"role\":\"STANDALONE\""));
        assertTrue(json.contains("\"clusterState\":\"HEALTHY\""));
        assertTrue(json.contains("\"queue\":{}"));
    }

    @Test
    public void testHandleProposalByIdRejectsMissingProposalId() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(newContext(new MemoryNodeStore()));
        handler.handleProposalById(response, "");

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("proposalId is required"));
    }

    @Test
    public void testHandleProposalByIdReturnsStatusPayload() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(new MemoryNodeStore());
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        ProposalStatus status = new ProposalStatus(
            "proposal-1",
            ProposalState.VERIFIED,
            "0xtx",
            111L,
            22L,
            null,
            DurabilityState.ACKED,
            222L,
            null,
            "head-1"
        );
        when(queueManager.getProposalStatus("proposal-1")).thenReturn(status);
        context.proposalQueueManager = queueManager;

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleProposalById(response, "proposal-1");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"proposalId\":\"proposal-1\""));
        assertTrue(json.contains("\"state\":\"VERIFIED\""));
        assertTrue(json.contains("\"durabilityState\":\"ACKED\""));
        assertTrue(json.contains("\"durableHead\":\"head-1\""));
    }

    @Test
    public void testHandleWalletByAddressReturnsWalletMetadataAndGcAccount() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedWallet(nodeStore, WALLET);

        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(nodeStore);
        context.gcAccountManager = new GCAccountManager();
        EntityGCAccount account = context.gcAccountManager.getAccount(WALLET.toLowerCase());
        account.totalDebt = new BigDecimal("4.25");
        account.executedDebt = new BigDecimal("1.50");
        account.writesBlocked = false;

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleWalletByAddress(response, WALLET);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"wallet\":\"" + WALLET + "\""));
        assertTrue(json.contains("\"walletPath\":\"" + WalletPathUtil.getShardRoot(WALLET) + "\""));
        assertTrue(json.contains("\"contentCount\":2"));
        assertTrue(json.contains("\"totalWrites\":7"));
        assertTrue(json.contains("\"name\":\"doc-1\""));
        assertTrue(json.contains("\"contentType\":\"fragment\""));
        assertTrue(json.contains("\"totalDebt\":\"4.25\""));
        assertTrue(json.contains("\"pendingDebt\":\"2.75\""));
        assertTrue(json.contains("\"authority\":{\"wallet\":\"" + WALLET + "\""));
    }

    @Test
    public void testHandleWalletByAddressReturnsNotFoundForMissingWallet() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(newContext(new MemoryNodeStore()));
        handler.handleWalletByAddress(response, WALLET);

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("Wallet not found"));
    }

    @Test
    public void testHandleReleaseFlowRequiresProposalQueue() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(newContext(new MemoryNodeStore()));
        handler.handleReleaseFlow(response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("Proposal queue not available"));
    }

    @Test
    public void testHandleReleaseFlowReturnsFlowPayload() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(new MemoryNodeStore());
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        Map<String, Object> flow = new LinkedHashMap<>();
        flow.put("releaseMode", "adaptive-active");
        flow.put("releaseStages", new LinkedHashMap<String, Object>());
        when(queueManager.getProposalReleaseFlowStats()).thenReturn(flow);
        context.proposalQueueManager = queueManager;

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleReleaseFlow(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"explorer.v1\""));
        assertTrue(json.contains("\"releaseFlow\":{\"releaseMode\":\"adaptive-active\",\"releaseStages\":{}}"));
    }

    @Test
    public void testHandleContentNavReturnsClusterAwareSections() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(new MemoryNodeStore());
        context.shardingRuntimeConfig = ShardingRuntimeConfig.fromSpecs(true, "00-7f", "80-ff=http://validator-2:8090");

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleContentNav(response);

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"contractVersion\":\"explorer.content.v1\""));
        assertTrue(json.contains("\"clusterId\":\"" + LOCAL_CLUSTER_ID + "\""));
        assertTrue(json.contains("\"clusterId\":\"" + REMOTE_CLUSTER_ID + "\""));
        assertTrue(json.contains("\"browseRoot\":\"/oak-chain\""));
        assertTrue(json.contains("\"strategy\":\"event-invalidated\""));
        assertTrue(json.contains("\"ttlMs\":86400000"));
    }

    @Test
    public void testHandleContentTreeFiltersPrefixesByClusterScope() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedTreeNode(nodeStore, "/oak-chain/12/aa/local-doc");
        seedTreeNode(nodeStore, "/oak-chain/90/bb/remote-doc");

        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(nodeStore);
        context.shardingRuntimeConfig = ShardingRuntimeConfig.fromSpecs(true, "00-7f", "80-ff=http://validator-2:8090");

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleContentTree(response, LOCAL_CLUSTER_ID, "/oak-chain");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"clusterId\":\"" + LOCAL_CLUSTER_ID + "\""));
        assertTrue(json.contains("\"name\":\"12\""));
        assertFalse(json.contains("\"name\":\"90\""));
    }

    @Test
    public void testHandleContentTreePagesVisibleChildren() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        for (int i = 0; i < 5; i++) {
            seedTreeNode(nodeStore, "/oak-chain/12/aa/doc-" + i);
        }
        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(newContext(nodeStore));
        Set<String> seen = new HashSet<>();

        StringWriter first = new StringWriter();
        handler.handleContentTree(responseWithBody(first), LOCAL_CLUSTER_ID, "/oak-chain/12/aa", 0, 2);
        assertTrue(first.toString().contains("\"childrenPage\":{\"offset\":0,\"limit\":2,\"returned\":2,\"nextOffset\":2}"));
        seen.addAll(childNames(first.toString()));

        StringWriter second = new StringWriter();
        handler.handleContentTree(responseWithBody(second), LOCAL_CLUSTER_ID, "/oak-chain/12/aa", 2, 2);
        assertTrue(second.toString().contains("\"childrenPage\":{\"offset\":2,\"limit\":2,\"returned\":2,\"nextOffset\":4}"));
        seen.addAll(childNames(second.toString()));

        StringWriter last = new StringWriter();
        handler.handleContentTree(responseWithBody(last), LOCAL_CLUSTER_ID, "/oak-chain/12/aa", 4, 2);
        assertTrue(last.toString().contains("\"childrenPage\":{\"offset\":4,\"limit\":2,\"returned\":1,\"nextOffset\":null}"));
        seen.addAll(childNames(last.toString()));
        assertEquals(5, seen.size());

        StringWriter clamped = new StringWriter();
        handler.handleContentTree(responseWithBody(clamped), LOCAL_CLUSTER_ID, "/oak-chain/12/aa", -3, 100000);
        assertTrue(clamped.toString().contains("\"childrenPage\":{\"offset\":0,\"limit\":"
            + ExplorerApiV1Handler.MAX_TREE_PAGE_SIZE + ",\"returned\":5,\"nextOffset\":null}"));
    }

    private static Set<String> childNames(String json) {
        Set<String> names = new HashSet<>();
        Matcher matcher = Pattern.compile("\"name\":\"(doc-\\d)\"").matcher(json);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }

    @Test
    public void testHandleContentProvenanceIncludesWriteAndWalletAuthority() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedWallet(nodeStore, WALLET);

        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        ServerContext context = newContext(nodeStore);
        context.shardingRuntimeConfig = ShardingRuntimeConfig.fromSpecs(true, "00-7f", "80-ff=http://validator-2:8090");
        context.recentWriteMetadata.put(
            WalletPathUtil.getShardRoot(WALLET),
            new WriteMetadata("record-1", "consensus", "http://validator-1:8090", 444L, "wallet write")
        );

        ExplorerApiV1Handler handler = new ExplorerApiV1Handler(context);
        handler.handleContentProvenance(response, LOCAL_CLUSTER_ID, WalletPathUtil.getShardRoot(WALLET));

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"matchPath\":\"" + WalletPathUtil.getShardRoot(WALLET) + "\""));
        assertTrue(json.contains("\"recordId\":\"record-1\""));
        assertTrue(json.contains("\"wallet\":\"" + WALLET + "\""));
        assertTrue(json.contains("\"ownership\":\"local\""));
    }

    private static ServerContext newContext(MemoryNodeStore nodeStore) {
        return new ServerContext(
            mock(FileStore.class),
            nodeStore,
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
    }

    private static HttpServletResponse responseWithBody(StringWriter body) throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }

    private static void seedWallet(MemoryNodeStore nodeStore, String wallet) throws Exception {
        String[] levels = WalletPathUtil.getShardLevels(wallet);
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder walletNode = root.child("oak-chain")
            .child(levels[0])
            .child(levels[1])
            .child(levels[2])
            .child(wallet);
        walletNode.setProperty("contentCount", 2L);
        walletNode.setProperty("totalWrites", 7L);
        walletNode.setProperty("walletCreated", 100L);
        walletNode.setProperty("lastWrite", 200L);
        walletNode.setProperty("nodeType", "wallet-root");
        NodeBuilder doc = walletNode.child("content").child("doc-1");
        doc.setProperty("contentType", "fragment");
        doc.setProperty("timestamp", 300L);
        doc.setProperty("message", "hello");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void seedTreeNode(MemoryNodeStore nodeStore, String path) throws Exception {
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder current = root;
        for (String segment : path.split("/")) {
            if (segment == null || segment.isEmpty()) {
                continue;
            }
            current = current.child(segment);
        }
        current.setProperty("jcr:primaryType", "nt:unstructured");
        current.setProperty("message", "seed");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
