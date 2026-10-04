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
import org.apache.jackrabbit.oak.segment.consensus.config.BlockchainConfig;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalQueueManagerOptimized;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumSignatureVerifier;
import org.apache.jackrabbit.oak.segment.consensus.sharding.ShardingRuntimeConfig;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.server.ServerContext;
import org.apache.jackrabbit.oak.segment.http.server.binary.CidMappingService;
import org.apache.jackrabbit.oak.segment.http.server.model.ClientRegistration;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.Sign;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WriteProposalHandlerTest {

    private static final String VALID_WALLET = "0x1111111111111111111111111111111111111111";
    private static final String VALID_SIGNATURE = "0xabcdef12";
    private static final String VALID_TX_HASH = "0xabcdef1234567890";
    private static final String TEST_PRIVATE_KEY = "4c0883a6910395bda8e1ab1b5f9f1cc0aa1f4b3f8718abf3483c796f9649b7fd";
    private static final String VALID_CHAIN_PROPOSAL_ID =
        "0x1111111111111111111111111111111111111111111111111111111111111111";

    @After
    public void tearDown() {
        System.clearProperty("oak.blockchain.mode");
        System.clearProperty("oak.blockchain.rpcUrl");
        System.clearProperty("oak.blockchain.contractAddress");
        System.clearProperty("oak.proposal.validator.binary.upload.enabled");
        BlockchainConfig.reset();
    }

    @Test
    public void testHandleProposeWriteRejectsWhenEngineNotConfigured() throws Exception {
        ServerContext context = newContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request(), response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Aeron consensus engine not configured\""));
    }

    @Test
    public void testHandleProposeWriteRejectsWhenClusterUnhealthy() throws Exception {
        ServerContext context = newContext();
        AeronConsensusEngine engine = baseEngine();
        when(engine.isClusterHealthy()).thenReturn(false);
        when(engine.getUnhealthyReason()).thenReturn("no_leader");
        context.aeronConsensusEngine = engine;
        WriteProposalHandler handler = new WriteProposalHandler(context);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request(), response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"error\":\"Cluster unhealthy: no_leader. Please retry in a few seconds.\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsMissingWalletAddress() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request(), response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing wallet address. Please provide a valid Ethereum address"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRedirectsForeignShardBeforeLocalProcessing() throws Exception {
        ServerContext context = readyContext();
        context.setShardingRuntimeConfig(ShardingRuntimeConfig.fromSpecs(
            true,
            "80-ff",
            "10-1f=http://cluster-a:8090"
        ));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        verify(response).setHeader("Location", "http://cluster-a:8090/v1/propose-write");
        assertTrue(body.toString().contains("\"code\":\"wrong_shard\""));
        assertTrue(body.toString().contains("\"l1Prefix\":\"11\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRedirectsFollowerToCurrentLeader() throws Exception {
        ServerContext context = readyContext();
        when(context.aeronConsensusEngine.isLeader()).thenReturn(false);
        when(context.aeronConsensusEngine.getCurrentLeader()).thenReturn("http://leader-2:8094");
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
        verify(response).setHeader("Location", "http://leader-2:8094/v1/propose-write");
        assertTrue(body.toString().contains("\"code\":\"wrong_leader\""));
        assertTrue(body.toString().contains("\"currentLeader\":\"http://leader-2:8094\""));
        assertTrue(body.toString().contains("\"redirectUrl\":\"http://leader-2:8094/v1/propose-write\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsFollowerWhenLeaderUnknown() throws Exception {
        ServerContext context = readyContext();
        when(context.aeronConsensusEngine.isLeader()).thenReturn(false);
        when(context.aeronConsensusEngine.getCurrentLeader()).thenReturn(null);
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        verify(response).setHeader("Retry-After", "1");
        assertTrue(body.toString().contains("\"code\":\"leader_unknown\""));
        assertTrue(body.toString().contains("Current leader is not yet known"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsNonEnterpriseClientIpfsCid() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("ipfsCid")).thenReturn("bafy-test");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        String json = body.toString();
        assertTrue(json.contains("\"code\":\"client_ipfs_cid_requires_enterprise_registration\""));
        assertTrue(json.contains("Client-side ipfsCid is restricted to registered enterprise clients."));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsMissingSignature() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing signature. All writes require a signature"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsInvalidSignaturePrefix() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn("abcdef");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid signature format: must start with '0x'"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsSignatureWithoutHexPayload() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn("0x");
        when(request.getParameter("message")).thenReturn("test");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid signature: too short"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsSignatureWithNonHexCharacters() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn("0xzz11");
        when(request.getParameter("message")).thenReturn("test");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid signature format: must be valid hexadecimal"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsCryptographicallyInvalidSignatureInChainBackedMode() throws Exception {
        Assume.assumeTrue("Requires full Ethereum signature verification", EthereumSignatureVerifier.isFullVerificationAvailable());

        withChainBackedMode();
        SignedRequest signedRequest = signedRequest("signed message");
        ServerContext context = readyContext();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        registerClient(context, signedRequest.walletAddress, "client-1");
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(signedRequest.walletAddress);
        when(request.getParameter("signature")).thenReturn(signedRequest.signature);
        when(request.getParameter("message")).thenReturn("tampered message");
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        assertTrue(body.toString().contains("Signature verification failed"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsWhenFullVerificationUnavailable() throws Exception {
        withChainBackedMode();
        SignedRequest signedRequest = signedRequest("signed message");
        ServerContext context = readyContext();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        registerClient(context, signedRequest.walletAddress, "client-1");
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(signedRequest.walletAddress);
        when(request.getParameter("signature")).thenReturn(signedRequest.signature);
        when(request.getParameter("message")).thenReturn(signedRequest.message);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        withForcedSignatureVerifierUnavailable("simulated verifier outage", () -> {
            handler.handleProposeWrite(request, response);
        });

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("Full Ethereum signature verification unavailable"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsMissingEthereumTxHash() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing ethereumTxHash parameter."));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsTransactionHashWithNonHexCharacters() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn("0xabcxyz12");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid ethereumTxHash format: must be valid hexadecimal"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsTransactionHashWithoutHexPrefix() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn("abcdef12");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid ethereumTxHash format: must start with '0x'"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsTooShortTransactionHash() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn("0xabc");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid ethereumTxHash: too short"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsInvalidProposalIdFormat() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("proposalId")).thenReturn("bad-proposal-id");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid proposalId format. Expected 0x-prefixed 32-byte hex."));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsMissingProposalIdInMockMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();

        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(null);
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing proposalId parameter"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsMissingProposalIdInSepoliaMode() throws Exception {
        withChainBackedMode();
        SignedRequest signedRequest = signedRequest("chain-backed message");
        ServerContext context = readyContext();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        registerClient(context, signedRequest.walletAddress, "client-1");
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(null);
        when(request.getParameter("walletAddress")).thenReturn(signedRequest.walletAddress);
        when(request.getParameter("signature")).thenReturn(signedRequest.signature);
        when(request.getParameter("message")).thenReturn(signedRequest.message);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Missing proposalId parameter"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsUuidProposalIdInSepoliaMode() throws Exception {
        withChainBackedMode();
        SignedRequest signedRequest = signedRequest("chain-backed uuid message");
        ServerContext context = readyContext();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        registerClient(context, signedRequest.walletAddress, "client-1");
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(signedRequest.walletAddress);
        when(request.getParameter("signature")).thenReturn(signedRequest.signature);
        when(request.getParameter("message")).thenReturn(signedRequest.message);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("proposalId")).thenReturn("123e4567-e89b-12d3-a456-426614174000");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Invalid proposalId format. Expected 0x-prefixed 32-byte hex."));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsQueueUnavailableInMockMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();

        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(VALID_CHAIN_PROPOSAL_ID);
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("message")).thenReturn("hello");
        when(request.getParameter("contentType")).thenReturn("page");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("Proposal queue unavailable. Clients must use queued verification."));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteImmediateIngressFailureReturnsServerError() throws Exception {
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("proposalId")).thenReturn(VALID_CHAIN_PROPOSAL_ID);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("Proposal queue unavailable. Clients must use queued verification."));
    }

    @Test
    public void testHandleProposeWriteIgnoresLegacyPaymentTierWhenQueueConfigured() throws Exception {
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            nullable(String.class), nullable(String.class), anyString(), nullable(String.class)
        )).thenReturn(new QueuedProposal(
            "proposal-1",
            VALID_TX_HASH,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 300_000L,
            ProposalState.PENDING
        ));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("paymentTier")).thenReturn("gold");
        when(request.getParameter("message")).thenReturn("hello");
        when(request.getParameter("contentType")).thenReturn("page");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        assertTrue(body.toString().contains("\"status\":\"accepted\""));
        assertEquals(1L, context.apiAcceptedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsInvalidOrganizationName() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("organization")).thenReturn("bad org!");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Organization name must be alphanumeric, hyphens, underscores only"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsUnregisteredWalletOutsideMockMode() throws Exception {
        System.setProperty("oak.blockchain.mode", "mainnet");
        BlockchainConfig.reset();

        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("not registered. Please register via /v1/register-client"));
    }

    @Test
    public void testHandleProposeWriteRejectsWalletMismatchForExplicitClientId() throws Exception {
        ServerContext context = readyContext();
        context.registeredClients.put("client-1", new ClientRegistration("client-1", "http://author", "0x2222222222222222222222222222222222222222"));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("clientId")).thenReturn("client-1");
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("Path enforcement violation"));
    }

    @Test
    public void testHandleProposeWriteRejectsAmbiguousBinarySources() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("ipfsCid")).thenReturn("bafy-test");
        when(request.getParameter("binaryData")).thenReturn("AQID");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_BAD_REQUEST);
        assertTrue(body.toString().contains("Ambiguous binary source: provide either ipfsCid or validator-hosted binary payload"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteRejectsUnknownEnterpriseCid() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        Path storageDir = Files.createTempDirectory("write-proposal-cids");
        try {
            ServerContext context = readyContext();
            context.registeredClients.put(
                "enterprise-1",
                new ClientRegistration(
                    "enterprise-1",
                    "http://author-1:4502",
                    VALID_WALLET.toLowerCase(),
                    ClientRegistration.CLIENT_TYPE_ENTERPRISE
                )
            );
            try (CidMappingService cidMappingService = new CidMappingService(storageDir)) {
                context.cidMappingService = cidMappingService;
                WriteProposalHandler handler = new WriteProposalHandler(context);
                HttpServletRequest request = request();
                when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
                when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
                when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
                when(request.getParameter("ipfsCid")).thenReturn("QmUnknownCid");
                StringWriter body = new StringWriter();
                HttpServletResponse response = responseWithBody(body);

                handler.handleProposeWrite(request, response);

                verify(response).setStatus(422);
                assertTrue(body.toString().contains("\"code\":\"unknown_ipfs_cid\""));
                assertEquals(1L, context.apiRejectedRequests.get());
            }
        } finally {
            Files.deleteIfExists(storageDir.resolve("cid-mappings.properties"));
            Files.deleteIfExists(storageDir);
        }
    }

    @Test
    public void testHandleProposeWriteRejectsEnterpriseCidWhenCidServiceUnavailable() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        context.registeredClients.put(
            "enterprise-1",
            new ClientRegistration(
                "enterprise-1",
                "http://author-1:4502",
                VALID_WALLET.toLowerCase(),
                ClientRegistration.CLIENT_TYPE_ENTERPRISE
            )
        );
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("ipfsCid")).thenReturn("QmKnownCid");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("CID provenance service unavailable"));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteAcceptsKnownEnterpriseCidAndQueuesProposal() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        context.registeredClients.put(
            "enterprise-1",
            new ClientRegistration(
                "enterprise-1",
                "http://author-1:4502",
                VALID_WALLET.toLowerCase(),
                ClientRegistration.CLIENT_TYPE_ENTERPRISE
            )
        );
        context.cidMappingService = mock(CidMappingService.class);
        when(context.cidMappingService.getOakBlobId("QmKnownCid")).thenReturn(Optional.of("blob-1"));
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            nullable(String.class), nullable(String.class), anyString(), nullable(String.class)
        )).thenReturn(new QueuedProposal(
            "proposal-1",
            VALID_TX_HASH,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 300_000L,
            ProposalState.PENDING
        ));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("ipfsCid")).thenReturn("QmKnownCid");
        when(request.getParameter("message")).thenReturn("hello");
        when(request.getParameter("contentType")).thenReturn("page");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        assertTrue(body.toString().contains("\"status\":\"accepted\""));
        assertEquals(1L, context.apiIpfsPolicyAcceptedEnterpriseCid.get());
        assertEquals(1L, context.apiAcceptedRequests.get());
    }

    @Test
    public void testConcurrentWritesInSameMillisecondGetDistinctStoragePaths() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        WriteProposalHandler handler = new WriteProposalHandler(context, () -> 1791134070821L);

        proposeWrite(handler, "0x3F2A9C1E" + "0".repeat(56));
        proposeWrite(handler, "0X7b00aa11" + "1".repeat(56));

        ArgumentCaptor<String> paths = ArgumentCaptor.forClass(String.class);
        verify(queueManager, times(2)).queueProposal(
            anyString(), anyString(), anyString(), paths.capture(), anyString(), anyString(), anyString(),
            nullable(String.class), nullable(String.class), anyString(), nullable(String.class)
        );
        String first = paths.getAllValues().get(0);
        String second = paths.getAllValues().get(1);
        assertTrue(first, first.endsWith("/acme/content/page-1791134070821-3f2a9c1e"));
        assertTrue(second, second.endsWith("/acme/content/page-1791134070821-7b00aa11"));
    }

    private static void proposeWrite(WriteProposalHandler handler, String proposalId) throws Exception {
        HttpServletRequest request = request();
        when(request.getParameter("proposalId")).thenReturn(proposalId);
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("organization")).thenReturn("acme");
        when(request.getParameter("message")).thenReturn("hello");
        when(request.getParameter("contentType")).thenReturn("page");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
    }

    @Test
    public void testHandleProposeWriteReturnsQueueOverloadedWhenAdmissionRejected() throws Exception {
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class),
            org.mockito.ArgumentMatchers.nullable(String.class),
            anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class)
        )).thenThrow(new java.util.concurrent.RejectedExecutionException("queue_overloaded"));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("message")).thenReturn("hello");
        when(request.getParameter("contentType")).thenReturn("page");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("\"code\":\"queue_overloaded\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteAcceptsValidatorHostedBinaryWhenUploadEnabled() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class),
            org.mockito.ArgumentMatchers.nullable(String.class),
            anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class)
        )).thenReturn(new QueuedProposal(
            "proposal-1",
            VALID_TX_HASH,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 300_000L,
            ProposalState.PENDING
        ));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("binaryData")).thenReturn("AQID");
        when(request.getParameter("mimeType")).thenReturn("application/octet-stream");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        assertTrue(body.toString().contains("\"status\":\"accepted\""));
    }

    @Test
    public void testHandleProposeWriteAllowsValidatorHostedBinaryWhenCapabilityEnabled() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        context.proposalQueueManager = queueManager;
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class),
            org.mockito.ArgumentMatchers.nullable(String.class),
            anyString(),
            org.mockito.ArgumentMatchers.nullable(String.class)
        )).thenReturn(new QueuedProposal(
            "proposal-1",
            VALID_TX_HASH,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 300_000L,
            ProposalState.PENDING
        ));
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("binaryData")).thenReturn("AQID");
        when(request.getParameter("mimeType")).thenReturn("application/octet-stream");
        when(request.getParameter("contentType")).thenReturn("page");
        when(request.getParameter("message")).thenReturn("hello");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        assertTrue(body.toString().contains("\"status\":\"accepted\""));
    }

    @Test
    public void testHandleProposeWriteAcceptsMultipartBinaryUpload() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        System.setProperty("oak.proposal.validator.binary.upload.enabled", "true");
        BlockchainConfig.reset();

        ServerContext context = readyContext();
        ProposalQueueManagerOptimized queueManager = mock(ProposalQueueManagerOptimized.class);
        BlobStore blobStore = mock(BlobStore.class);
        context.proposalQueueManager = queueManager;
        context.blobStore = blobStore;
        context.registeredClients.put("client-1", new ClientRegistration("client-1", "http://author", VALID_WALLET.toLowerCase()));
        when(blobStore.writeBlob(any())).thenReturn("blob-1");
        when(queueManager.queueProposal(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), anyString(),
            nullable(String.class), nullable(String.class), anyString(), nullable(String.class)
        )).thenReturn(new QueuedProposal(
            "proposal-1",
            VALID_TX_HASH,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 300_000L,
            ProposalState.PENDING
        ));

        HttpServletRequest request = request();
        when(request.getContentType()).thenReturn("multipart/form-data; boundary=test");
        Collection<Part> parts = multipartParts();
        when(request.getParts()).thenReturn(parts);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        WriteProposalHandler handler = new WriteProposalHandler(context);
        handler.handleProposeWrite(request, response);

        verify(blobStore).writeBlob(any());
        verify(response).setStatus(HttpServletResponse.SC_ACCEPTED);
        assertTrue(body.toString().contains("\"status\":\"accepted\""));
    }

    @Test
    public void testHandleProposeWriteRejectsValidatorHostedBinaryWhenCapabilityDisabled() throws Exception {
        System.setProperty("oak.blockchain.mode", "mock");
        System.setProperty("oak.proposal.validator.binary.upload.enabled", "false");
        BlockchainConfig.reset();
        ServerContext context = readyContext();
        context.proposalQueueManager = mock(ProposalQueueManagerOptimized.class);
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        when(request.getParameter("signature")).thenReturn(VALID_SIGNATURE);
        when(request.getParameter("ethereumTxHash")).thenReturn(VALID_TX_HASH);
        when(request.getParameter("binaryData")).thenReturn("AQID");
        when(request.getParameter("mimeType")).thenReturn("application/octet-stream");
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(HttpServletResponse.SC_FORBIDDEN);
        assertTrue(body.toString().contains("\"code\":\"validator_binary_upload_disabled\""));
        assertEquals(1L, context.apiRejectedRequests.get());
    }

    @Test
    public void testHandleProposeWriteBlocksWritesWhenGcDebtExceeded() throws Exception {
        ServerContext context = readyContext();
        GCAccountManager accountManager = new GCAccountManager();
        accountManager.addDebt(VALID_WALLET.toLowerCase(), "/oak-chain/demo", 1200L);
        accountManager.convertAllPendingToExecuted();
        context.gcAccountManager = accountManager;
        WriteProposalHandler handler = new WriteProposalHandler(context);
        HttpServletRequest request = request();
        when(request.getParameter("walletAddress")).thenReturn(VALID_WALLET);
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        handler.handleProposeWrite(request, response);

        verify(response).setStatus(402);
        assertTrue(body.toString().contains("\"code\":\"write_blocked_gc_debt\""));
        assertTrue(body.toString().contains("\"paymentUrl\":\"/v1/gc/account/" + VALID_WALLET.toLowerCase() + "/pay\""));
    }

    private static ServerContext readyContext() {
        ServerContext context = newContext();
        context.aeronConsensusEngine = baseEngine();
        return context;
    }

    private static ServerContext newContext() {
        return new ServerContext(
            mock(FileStore.class),
            mock(NodeStore.class),
            Paths.get("/tmp/store"),
            "http://localhost:8090"
        );
    }

    private static AeronConsensusEngine baseEngine() {
        AeronConsensusEngine engine = mock(AeronConsensusEngine.class);
        when(engine.isClusterHealthy()).thenReturn(true);
        when(engine.isLeader()).thenReturn(true);
        when(engine.getCurrentLeader()).thenReturn("http://localhost:8090");
        return engine;
    }

    private static void registerClient(ServerContext context, String wallet, String clientId) {
        ClientRegistration registration = new ClientRegistration(clientId, "http://author", wallet.toLowerCase());
        context.registeredClients.put(wallet.toLowerCase(), registration);
        context.registeredClients.put(clientId, registration);
    }

    private static void withChainBackedMode() {
        System.setProperty("oak.blockchain.mode", "sepolia");
        System.setProperty("oak.blockchain.rpcUrl", "https://rpc.example.invalid");
        System.setProperty("oak.blockchain.contractAddress", "0x1111111111111111111111111111111111111112");
        BlockchainConfig.reset();
    }

    private static SignedRequest signedRequest(String message) {
        Credentials credentials = Credentials.create(TEST_PRIVATE_KEY);
        Sign.SignatureData signatureData = Sign.signPrefixedMessage(
            message.getBytes(StandardCharsets.UTF_8),
            credentials.getEcKeyPair()
        );
        return new SignedRequest(credentials.getAddress(), signatureHex(signatureData), message);
    }

    private static String signatureHex(Sign.SignatureData signatureData) {
        byte[] bytes = new byte[65];
        System.arraycopy(signatureData.getR(), 0, bytes, 0, 32);
        System.arraycopy(signatureData.getS(), 0, bytes, 32, 32);
        bytes[64] = signatureData.getV()[0];
        StringBuilder builder = new StringBuilder("0x");
        for (byte b : bytes) {
            builder.append(String.format("%02x", b & 0xff));
        }
        return builder.toString();
    }

    private static HttpServletRequest request() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getContentType()).thenReturn(null);
        when(request.getParameter("proposalId")).thenReturn(VALID_CHAIN_PROPOSAL_ID);
        return request;
    }

    private static Collection<Part> multipartParts() throws Exception {
        return Arrays.asList(
            fieldPart("proposalId", VALID_CHAIN_PROPOSAL_ID),
            fieldPart("walletAddress", VALID_WALLET),
            fieldPart("signature", VALID_SIGNATURE),
            fieldPart("ethereumTxHash", VALID_TX_HASH),
            fieldPart("contentType", "page"),
            fieldPart("message", "hello"),
            filePart("binary", "asset.bin", "application/octet-stream", new byte[] {1, 2, 3})
        );
    }

    private static Part fieldPart(String name, String value) throws Exception {
        Part part = mock(Part.class);
        when(part.getName()).thenReturn(name);
        when(part.getSubmittedFileName()).thenReturn(null);
        when(part.getInputStream()).thenReturn(new ByteArrayInputStream(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return part;
    }

    private static Part filePart(String name, String filename, String contentType, byte[] bytes) throws Exception {
        Part part = mock(Part.class);
        when(part.getName()).thenReturn(name);
        when(part.getSubmittedFileName()).thenReturn(filename);
        when(part.getContentType()).thenReturn(contentType);
        when(part.getInputStream()).thenReturn(new ByteArrayInputStream(bytes));
        return part;
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
        Field field = EthereumSignatureVerifier.class.getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        return field.getBoolean(null);
    }

    private static void setVerifierAvailability(boolean available) throws Exception {
        Field field = EthereumSignatureVerifier.class.getDeclaredField("bouncyCastleAvailable");
        field.setAccessible(true);
        field.setBoolean(null, available);
    }

    private static String readVerifierReason() throws Exception {
        Field field = EthereumSignatureVerifier.class.getDeclaredField("availabilityReason");
        field.setAccessible(true);
        return (String) field.get(null);
    }

    private static void setVerifierReason(String reason) throws Exception {
        Field field = EthereumSignatureVerifier.class.getDeclaredField("availabilityReason");
        field.setAccessible(true);
        field.set(null, reason);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class SignedRequest {
        private final String walletAddress;
        private final String signature;
        private final String message;

        private SignedRequest(String walletAddress, String signature, String message) {
            this.walletAddress = walletAddress;
            this.signature = signature;
            this.message = message;
        }
    }
}
