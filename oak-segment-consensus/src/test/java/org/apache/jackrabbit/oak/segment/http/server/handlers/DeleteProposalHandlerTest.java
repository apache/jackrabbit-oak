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
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Test;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.Sign;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class DeleteProposalHandlerTest {

    private static final String VALID_WALLET = "0x1234567890abcdef1234567890abcdef12345678";
    private static final String OTHER_WALLET = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd";
    private static final String VALID_SIGNATURE = "0xabcdef12";
    private static final String TEST_PRIVATE_KEY =
        "4c0883a6910395bda8e1ab1b5f9f1cc0aa1f4b3f8718abf3483c796f9649b7fd";
    private static final String PRIORITY_TX_HASH = "0xabcdef123456789f";
    private static final String VALID_CHAIN_PROPOSAL_ID =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    private static final int LARGE_DELETE_BRANCH_COUNT = 100;
    private static final int LARGE_DELETE_LEAF_COUNT = 100;

    @After
    public void tearDown() {
        System.clearProperty("oak.blockchain.mode");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();
    }

    @Test
    public void testHandleDeleteProposalRejectsUnregisteredWallet() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(WalletPathUtil.getShardRoot(VALID_WALLET) + "/content/doc-1");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("not registered"));
    }

    @Test
    public void testHandleDeleteProposalRedirectsForeignShardBeforeLocalProcessing() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        context.setShardingRuntimeConfig(ShardingRuntimeConfig.fromSpecs(
            true,
            "80-ff",
            "10-1f=http://cluster-a:8090"
        ));
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(WalletPathUtil.getShardRoot(VALID_WALLET) + "/content/doc-1");
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        verify(response).setHeader("Location", "http://cluster-a:8090/v1/propose-delete");
        assertTrue(body.toString().contains("\"code\":\"wrong_shard\""));
        assertTrue(body.toString().contains("\"l1Prefix\":\"12\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleDeleteProposalRedirectsFollowerToCurrentLeader() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        when(context.aeronConsensusEngine.isLeader()).thenReturn(false);
        when(context.aeronConsensusEngine.getCurrentLeader()).thenReturn("http://leader-2:8094");
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(WalletPathUtil.getShardRoot(VALID_WALLET) + "/content/doc-1");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        verify(response).setHeader("Location", "http://leader-2:8094/v1/propose-delete");
        assertTrue(body.toString().contains("\"code\":\"wrong_leader\""));
        assertTrue(body.toString().contains("\"currentLeader\":\"http://leader-2:8094\""));
        assertTrue(body.toString().contains("\"redirectUrl\":\"http://leader-2:8094/v1/propose-delete\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleDeleteProposalRejectsClientIdWalletMismatch() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        context.registeredClients.put("client-1", new ClientRegistration("client-1", "http://author-1:4502", OTHER_WALLET));
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(WalletPathUtil.getShardRoot(VALID_WALLET) + "/content/doc-1");
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        when(request.getHeader("X-Client-Id")).thenReturn("client-1");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("Wallet mismatch"));
    }

    @Test
    public void testHandleDeleteProposalRejectsPathOwnershipViolation() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        registerClient(context, VALID_WALLET, "client-1");
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn("/oak-chain/wrong/path");
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("Path ownership violation"));
    }

    @Test
    public void testHandleDeleteProposalAllowsWalletRootDeletion() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        seedContent(nodeStore, VALID_WALLET);
        String contentPath = WalletPathUtil.getShardRoot(VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertTrue(body.toString().contains("\"type\":\"DELETE\""));
    }

    @Test
    public void testHandleDeleteProposalRecoversWalletRegistrationFromWalletContent() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertTrue(context.registeredClients.containsKey(VALID_WALLET));
    }

    @Test
    public void testHandleDeleteProposalRejectsMissingEthereumTxHash() throws Exception {
        ServerContext context = readyContext(new MemoryNodeStore());
        registerClient(context, VALID_WALLET, "client-1");
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(WalletPathUtil.getShardRoot(VALID_WALLET) + "/content/doc-1");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing ethereumTxHash parameter"));
    }

    @Test
    public void testHandleDeleteProposalRejectsMissingProposalIdInMockMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(null);
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing proposalId parameter"));
    }

    @Test
    public void testHandleDeleteProposalRejectsMissingProposalIdInChainBackedMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(null);
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing proposalId parameter"));
    }

    @Test
    public void testHandleDeleteProposalRejectsWhenFullVerificationUnavailable() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        withForcedSignatureVerifierUnavailable("simulated verifier outage", () -> {
            handler.handleDeleteProposal(request, response);
        });

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("Full Ethereum signature verification unavailable"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleDeleteProposalQueuesChainBackedDeleteWithClientProposalId() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        String proposalId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Credentials credentials = Credentials.create(TEST_PRIVATE_KEY);
        String wallet = credentials.getAddress();
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, wallet);
        String signature = personalSign(contentPath, credentials);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, wallet, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(wallet);
        when(request.getParameter("signature")).thenReturn(signature);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        when(request.getParameter("proposalId")).thenReturn(proposalId);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            eq(proposalId),
            eq(PRIORITY_TX_HASH),
            eq(wallet),
            eq(contentPath),
            eq(signature)
        );
        assertTrue(body.toString().contains("\"proposalId\":\"" + proposalId + "\""));
        assertTrue(body.toString().contains("\"proposalIdSource\":\"client\""));
    }

    @Test
    public void testHandleDeleteProposalRejectsChainBackedDeleteNotSignedOverContentPath() throws Exception {
        System.setProperty("oak.blockchain.mode", "sepolia");
        org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig.reset();

        Credentials credentials = Credentials.create(TEST_PRIVATE_KEY);
        String wallet = credentials.getAddress();
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, wallet);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, wallet, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(wallet);
        when(request.getParameter("signature")).thenReturn(personalSign("", credentials));
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        assertTrue(body.toString().contains("Signature verification failed"));
        assertEquals(1L, context.apiRejectedRequests.get());
        verifyNoInteractions(context.proposalQueueManager);
        assertEquals(0, BigDecimal.ZERO.compareTo(context.gcAccountManager.getAccount(wallet).totalDebt));
    }

    @Test
    public void testHandleDeleteProposalQueuesDeleteAndTracksGcDebt() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertEquals("0.10", context.gcAccountManager.getAccount(VALID_WALLET).totalDebt.toString());
        String json = body.toString();
        assertTrue(json.contains("\"status\":\"accepted\""));
        assertTrue(json.contains("\"type\":\"DELETE\""));
        assertFalse(json.contains("\"tier\""));
        assertTrue(json.contains("\"gcDebtIncurred\":\"0.10\""));
        assertTrue(json.contains("\"totalDebt\":\"0.10\""));
        assertTrue(json.contains("\"estimatedDeleteSizeMb\":1"));
        assertTrue(json.contains("\"estimatedNodeCount\":1"));
        assertTrue(json.contains("\"estimatedDescendantCount\":0"));
        assertTrue(json.contains("\"estimatedPropertyCount\":2"));
        assertTrue(json.contains("\"estimationTruncated\":false"));
    }

    @Test
    public void testHandleDeleteProposalEstimatesLargeDeleteSubtreeWithoutLegacyUnderCount() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedLargeDeleteSubtree(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertEquals(0, context.gcAccountManager.getAccount(VALID_WALLET).totalDebt.compareTo(new BigDecimal("1.10")));

        String json = body.toString();
        assertTrue(json.contains("\"estimatedDeleteSizeMb\":11"));
        assertTrue(json.contains("\"estimatedNodeCount\":10101"));
        assertTrue(json.contains("\"estimatedDescendantCount\":10100"));
        assertTrue(json.contains("\"estimatedPropertyCount\":10101"));
        assertTrue(json.contains("\"estimationTruncated\":false"));
        assertTrue(json.contains("\"gcDebtIncurred\":\"1.10\""));
    }

    @Test
    public void testHandleDeleteProposalIgnoresLegacyPaymentTierWhenProvided() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        when(request.getParameter("paymentTier")).thenReturn("priority");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertFalse(body.toString().contains("\"tier\""));
    }

    @Test
    public void testHandleDeleteProposalIgnoresInvalidLegacyPaymentTier() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        String contentPath = seedContent(nodeStore, VALID_WALLET);
        ServerContext context = readyContext(nodeStore);
        registerClient(context, VALID_WALLET, "client-1");
        context.gcAccountManager = new GCAccountManager();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        DeleteProposalHandler handler = new DeleteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("contentPath")).thenReturn(contentPath);
        when(request.getParameter("ethereumTxHash")).thenReturn(PRIORITY_TX_HASH);
        when(request.getParameter("paymentTier")).thenReturn("banana");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleDeleteProposal(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        verify(context.proposalQueueManager).queueDeleteProposal(
            anyString(),
            eq(PRIORITY_TX_HASH),
            eq(VALID_WALLET),
            eq(contentPath),
            eq(VALID_SIGNATURE)
        );
        assertFalse(body.toString().contains("\"tier\""));
    }

    private static ServerContext readyContext(MemoryNodeStore nodeStore) {
        ServerContext context = new ServerContext(
            mock(FileStore.class),
            nodeStore,
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.isClusterHealthy()).thenReturn(true);
        when(engine.isLeader()).thenReturn(true);
        when(engine.getCurrentLeader()).thenReturn("http://localhost:8090");
        context.aeronConsensusEngine = engine;
        return context;
    }

    private static void registerClient(ServerContext context, String wallet, String clientId) {
        ClientRegistration registration = new ClientRegistration(clientId, "http://author-1:4502", wallet);
        context.registeredClients.put(wallet, registration);
        context.registeredClients.put(clientId, registration);
    }

    private static String seedContent(MemoryNodeStore nodeStore, String wallet) throws Exception {
        String path = WalletPathUtil.getShardRoot(wallet) + "/content/doc-1";
        String[] parts = path.substring(1).split("/");
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder current = root;
        for (String part : parts) {
            current = current.child(part);
        }
        current.setProperty("contentType", "fragment");
        current.setProperty("message", "hello");
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return path;
    }

    private static String seedLargeDeleteSubtree(MemoryNodeStore nodeStore, String wallet) throws Exception {
        String path = WalletPathUtil.getShardRoot(wallet) + "/content/wallet-root";
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder subtreeRoot = root;
        for (String part : path.substring(1).split("/")) {
            subtreeRoot = subtreeRoot.child(part);
        }
        subtreeRoot.setProperty("contentType", "wallet");

        for (int branch = 0; branch < LARGE_DELETE_BRANCH_COUNT; branch++) {
            NodeBuilder branchBuilder = subtreeRoot.child("branch-" + branch);
            branchBuilder.setProperty("branchIndex", branch);
            for (int leaf = 0; leaf < LARGE_DELETE_LEAF_COUNT; leaf++) {
                NodeBuilder leafBuilder = branchBuilder.child("leaf-" + leaf);
                leafBuilder.setProperty("leafIndex", leaf);
            }
        }

        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return path;
    }

    private static String personalSign(String message, Credentials credentials) {
        Sign.SignatureData signatureData = Sign.signPrefixedMessage(
            message.getBytes(StandardCharsets.UTF_8),
            credentials.getEcKeyPair()
        );
        StringBuilder builder = new StringBuilder("0x");
        for (byte[] part : new byte[][] {signatureData.getR(), signatureData.getS(), signatureData.getV()}) {
            for (byte b : part) {
                builder.append(String.format("%02x", b & 0xff));
            }
        }
        return builder.toString();
    }

    private static HttpServletRequest request() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getContentType()).thenReturn(null);
        when(request.getParameter("proposalId")).thenReturn(VALID_CHAIN_PROPOSAL_ID);
        return request;
    }

    private static HttpServletResponse responseWithBody(StringWriter body) throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }

    private static void withForcedSignatureVerifierUnavailable(String reason, ThrowingRunnable runnable)
            throws Exception {
        boolean originalAvailable = readVerifierAvailability();
        String originalReason = readVerifierReason();
        setVerifierAvailability(false);
        setVerifierReason(reason);
        try {
            runnable.run();
        } finally {
            setVerifierAvailability(originalAvailable);
            setVerifierReason(originalReason);
        }
    }

    private static boolean readVerifierAvailability() throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        return field.getBoolean(null);
    }

    private static void setVerifierAvailability(boolean available) throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        field.setBoolean(null, available);
    }

    private static String readVerifierReason() throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("availabilityReason");
        field.setAccessible(true);
        return (String) field.get(null);
    }

    private static void setVerifierReason(String reason) throws Exception {
        Field field = org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier.class
            .getDeclaredField("availabilityReason");
        field.setAccessible(true);
        field.set(null, reason);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
