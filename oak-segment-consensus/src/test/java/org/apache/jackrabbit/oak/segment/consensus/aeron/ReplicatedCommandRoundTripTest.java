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

import org.apache.jackrabbit.oak.segment.consensus.queue.ProposalState;
import org.apache.jackrabbit.oak.segment.consensus.queue.QueuedProposal;
import org.apache.jackrabbit.oak.segment.consensus.service.MutationAuditMetadata;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Every string field of every replicated command must survive
 * encoder -> Aeron payload -> {@link MessageDispatcher} unchanged.
 */
public class ReplicatedCommandRoundTripTest {

    private static final List<String> SAMPLES = Arrays.asList(
        "{\"t\":\"x\"}",
        "say \"hi\"",
        "back\\slash \\\\ and trailing \\",
        "line\nbreak\rcarriage\ttab",
        "ctl \u0000 \u0001 \b \f \u001f end",
        "a } b {",
        "{ unbalanced",
        "literal \\u0041 not an escape",
        "na\u00efve caf\u00e9 \u65e5\u672c",
        "emoji \uD83D\uDE00\uD83D\uDE80",
        "\"\\\"\\\\\"",
        ""
    );

    private final AeronIngressWritePayloadBuilder writes = new AeronIngressWritePayloadBuilder();
    private final AeronIngressControlPayloadBuilder controls = new AeronIngressControlPayloadBuilder();

    @Test
    public void singleWriteRoundTripsEveryStringField() {
        for (String s : SAMPLES) {
            MutationAuditMetadata audit = MutationAuditMetadata.write(
                "tx" + s, "corr" + s, "pid" + s, "eth" + s, 1L, 2L, 3L);
            AeronEncodedMessage encoded = writes.buildWriteProposalWithBinary(
                "0xw" + s, "/p" + s, "ct" + s, "msg" + s, "sig" + s, 7,
                "blob" + s, "mime" + s, "cid" + s, audit);

            List<String> calls = new ArrayList<>();
            assertTrue(dispatch(dispatcher(calls), encoded));
            assertEquals(Arrays.asList(String.join("|",
                "0xw" + s, "/p" + s, "ct" + s, "msg" + s, "sig" + s, "null",
                "blob" + s, "mime" + s, "cid" + s,
                "WRITE", "tx" + s, "corr" + s, "pid" + s, "eth" + s, "1", "2", "3")), calls);
        }
    }

    @Test
    public void deleteRoundTripsEveryStringField() {
        for (String s : SAMPLES) {
            MutationAuditMetadata audit = MutationAuditMetadata.delete(
                "tx" + s, "corr" + s, "pid" + s, "eth" + s, 1L, 2L, 3L);
            AeronEncodedMessage encoded = writes.buildDeleteProposal("0xw" + s, "/p" + s, "sig" + s, 7, audit);

            List<String> calls = new ArrayList<>();
            assertTrue(dispatch(dispatcher(calls), encoded));
            assertEquals(Arrays.asList(String.join("|",
                "delete", "0xw" + s, "/p" + s, "sig" + s,
                "DELETE", "tx" + s, "corr" + s, "pid" + s, "eth" + s, "1", "2", "3")), calls);
        }
    }

    @Test
    public void writeBatchRoundTripsEveryStringFieldOfEveryProposal() {
        List<QueuedProposal> batch = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        int i = 0;
        for (String s : SAMPLES) {
            String n = (i++) + s;
            QueuedProposal p = new QueuedProposal("pid" + n, "eth" + n, null, 1L, 2L, ProposalState.PENDING);
            p.setWalletAddress("0xw" + n);
            p.setPath("/p" + n);
            p.setContentType("ct" + n);
            p.setMessage("msg" + n);
            p.setSignature("sig" + n);
            p.setIntentToken("intent" + n);
            p.setBlobId("blob" + n);
            p.setMimeType("mime" + n);
            p.setIpfsCid("cid" + n);
            p.setTransactionId("tx" + n);
            p.setCorrelationId("corr" + n);
            p.setConfirmedBlock(1L);
            p.setObservedEpoch(2L);
            p.setFinalizedEpoch(3L);
            batch.add(p);
            expected.add(String.join("|",
                "0xw" + n, "/p" + n, "ct" + n, "msg" + n, "sig" + n, "intent" + n,
                "blob" + n, "mime" + n, "cid" + n,
                "WRITE", "tx" + n, "corr" + n, "pid" + n, "eth" + n, "1", "2", "3"));
        }

        List<String> calls = new ArrayList<>();
        assertTrue(dispatch(dispatcher(calls), writes.buildWriteBatch(batch, 7)));
        assertEquals(expected, calls);
    }

    @Test
    public void writeBatchWithUnbalancedBracesInMessagesKeepsProposalsApart() {
        String[] messages = {"a } b {", "}}}", "{{{", "] , [", "x\"}, {\"walletAddress\":\"0xevil"};
        List<QueuedProposal> batch = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < messages.length; i++) {
            QueuedProposal p = new QueuedProposal("pid" + i, null, null, 1L, 2L, ProposalState.PENDING);
            p.setWalletAddress("0xw" + i);
            p.setPath("/p" + i);
            p.setContentType("page");
            p.setMessage(messages[i]);
            p.setSignature("sig" + i);
            batch.add(p);
            expected.add(String.join("|", "0xw" + i, "/p" + i, "page", messages[i], "sig" + i, "null",
                "null", "null", "null", "WRITE", "null", "null", "pid" + i, "null", "null", "null", "null"));
        }

        List<String> calls = new ArrayList<>();
        assertTrue(dispatch(dispatcher(calls), writes.buildWriteBatch(batch, 7)));
        assertEquals(expected, calls);
    }

    @Test
    public void writeBatchRoutesDeleteItemsToApplyDeleteInSubmissionOrder() {
        QueuedProposal writeA = batchItem("pid-a", "/a", QueuedProposal.ProposalType.WRITE);
        QueuedProposal deleteP = batchItem("pid-del-p", "/p", QueuedProposal.ProposalType.DELETE);
        QueuedProposal writeP = batchItem("pid-p", "/p", QueuedProposal.ProposalType.WRITE);

        List<String> calls = new ArrayList<>();
        assertTrue(dispatch(dispatcher(calls), writes.buildWriteBatch(Arrays.asList(writeA, deleteP, writeP), 7)));
        assertEquals(Arrays.asList(
            "0xw|/a|page|msg-pid-a|sig-pid-a|null|null|null|null|WRITE|null|null|pid-a|null|null|null|null",
            "delete|0xw|/p|sig-pid-del-p|DELETE|null|null|pid-del-p|null|null|null|null",
            "0xw|/p|page|msg-pid-p|sig-pid-p|null|null|null|null|WRITE|null|null|pid-p|null|null|null|null"), calls);
    }

    @Test
    public void gcCommandsRoundTripEveryStringField() {
        for (String s : SAMPLES) {
            List<String> calls = new ArrayList<>();
            MessageDispatcher d = dispatcher(calls);
            assertTrue(dispatch(d, controls.buildGcProposal("pid" + s, "0xw" + s, "rev" + s, 42L, "usdc" + s)));
            assertTrue(dispatch(d, controls.buildGcVote("pid" + s, 2, true, "why" + s)));
            assertTrue(dispatch(d, controls.buildGcVote("pid" + s, 3, false, "no" + s)));
            assertTrue(dispatch(d, controls.buildGcExecute("pid" + s, 1)));
            assertEquals(Arrays.asList(
                String.join("|", "gcProposal", "pid" + s, "0xw" + s, "rev" + s, "42", "usdc" + s),
                String.join("|", "gcVote", "pid" + s, "2", "true", "why" + s),
                String.join("|", "gcVote", "pid" + s, "3", "false", "no" + s),
                String.join("|", "gcExecute", "pid" + s, "1")), calls);
        }
    }

    @Test
    public void durabilityCommandsRoundTripEveryStringField() {
        for (String s : SAMPLES) {
            List<String> calls = new ArrayList<>();
            MessageDispatcher d = dispatcher(calls);
            assertTrue(dispatch(d, controls.buildQueueSegment("pid" + s, 3, 2)));
            assertTrue(dispatch(d, controls.buildSegmentPersisted("pid" + s, 1, false, "head" + s, "err" + s)));
            assertTrue(dispatch(d, controls.buildAckSegmentPersisted("pid" + s, true, "head" + s, "err" + s, 3, 2)));
            assertEquals(Arrays.asList(
                String.join("|", "queue", "pid" + s, "3", "2"),
                String.join("|", "persisted", "pid" + s, "1", "head" + s, "false", "err" + s),
                String.join("|", "ack", "pid" + s, "true", "head" + s, "err" + s, "3", "2")), calls);
        }
    }

    @Test
    public void transactionCommandsRoundTripEveryStringField() {
        for (String s : SAMPLES) {
            List<String> calls = new ArrayList<>();
            MessageDispatcher d = dispatcher(calls);
            assertTrue(dispatch(d, controls.buildStartTransaction("tx" + s, "corr" + s, 5000L, "0xw" + s, 7)));
            assertTrue(dispatch(d, controls.buildCommitTransaction("tx" + s, "corr" + s, 7)));
            assertTrue(dispatch(d, controls.buildAbortTransaction("tx" + s, "corr" + s, "why" + s, 7)));
            assertEquals(Arrays.asList(
                String.join("|", "start", "tx" + s, "corr" + s, "5000", "0xw" + s),
                String.join("|", "commit", "tx" + s, "corr" + s),
                String.join("|", "abort", "tx" + s, "corr" + s, "why" + s)), calls);
        }
    }

    @Test
    public void payloadLargerThanSixteenBitBlockLengthRoundTrips() {
        StringBuilder big = new StringBuilder();
        while (big.length() < 70_000) {
            big.append("chunk \"").append(big.length()).append("\" \uD83D\uDE00 ");
        }
        String message = big.toString();
        AeronEncodedMessage encoded = writes.buildWriteProposal("0xw", "/p", "page", message, "sig", 7, null, "pid");
        assertTrue(encoded.totalLength > 0xFFFF);

        List<String> calls = new ArrayList<>();
        assertTrue(dispatch(dispatcher(calls), encoded));
        assertEquals(Arrays.asList(String.join("|", "0xw", "/p", "page", message, "sig", "null",
            "null", "null", "null", "WRITE", "null", "null", "pid", "null", "null", "null", "null")), calls);
    }

    @Test
    public void encoderEscapesEveryControlCharacter() {
        StringBuilder all = new StringBuilder();
        for (char c = 0; c < 0x20; c++) {
            all.append(c);
        }
        String json = controls.buildGcVote("p", 1, true, all.toString()).json;
        for (int i = 0; i < json.length(); i++) {
            assertTrue("raw control char at " + i + " in " + json, json.charAt(i) >= 0x20);
        }
    }

    @Test
    public void numericAndBooleanFieldsReadFromTopLevelOnly() {
        List<String> calls = new ArrayList<>();
        MessageDispatcher d = dispatcher(calls);
        d.setTermProvider(() -> 9L);
        assertTrue(dispatch(d, SimpleMessageHeader.TEMPLATE_ID_WRITE_PROPOSAL,
            "{\"nested\":{\"term\":1,\"walletAddress\":\"0xnested\"},"
                + "\"walletAddress\" : \"0xw\", \"path\" : \"/p\", \"term\" : 9}"));
        assertTrue(dispatch(d, SimpleMessageHeader.TEMPLATE_ID_GC_VOTE,
            "{\"meta\":{\"approve\":false,\"validatorId\":7},"
                + "\"proposalId\" : \"pid\", \"validatorId\" : 2, \"approve\" : true}"));
        assertEquals(Arrays.asList(
            "0xw|/p|null|null|null|null|null|null|null|WRITE|null|null|null|null|null|null|null",
            "gcVote|pid|2|true|null"), calls);
    }

    static QueuedProposal batchItem(String proposalId, String path, QueuedProposal.ProposalType type) {
        QueuedProposal p = new QueuedProposal(proposalId, null, null, 1L, 2L, ProposalState.PENDING);
        p.setType(type);
        p.setWalletAddress("0xw");
        p.setPath(path);
        p.setContentType("page");
        p.setMessage("msg-" + proposalId);
        p.setSignature("sig-" + proposalId);
        return p;
    }

    private static MessageDispatcher dispatcher(List<String> calls) {
        MessageDispatcher dispatcher = new MessageDispatcher(new MessageDispatcher.WriteCallback() {
            @Override
            public void applyWrite(String walletAddress, String path, String contentType, String message,
                                   String signature, String intentToken, String blobId, String mimeType,
                                   String ipfsCid, MutationAuditMetadata audit) {
                calls.add(String.join("|", walletAddress, path, contentType, message, signature,
                    String.valueOf(intentToken), String.valueOf(blobId), String.valueOf(mimeType),
                    String.valueOf(ipfsCid), audit(audit)));
            }

            @Override
            public void applyDelete(String walletAddress, String path, String signature, MutationAuditMetadata audit) {
                calls.add(String.join("|", "delete", walletAddress, path, signature, audit(audit)));
            }
        });
        dispatcher.setTermProvider(() -> 7L);
        dispatcher.setGCCallback(new MessageDispatcher.GCCallback() {
            @Override
            public void applyGCProposal(String proposalId, String proposerWallet, String targetRevision,
                                        long estimatedReclaimableSizeMB, String estimatedCostUSDC) {
                calls.add(String.join("|", "gcProposal", proposalId, proposerWallet, targetRevision,
                    String.valueOf(estimatedReclaimableSizeMB), estimatedCostUSDC));
            }

            @Override
            public void applyGCVote(String proposalId, int validatorId, boolean approve, String reason) {
                calls.add(String.join("|", "gcVote", proposalId, String.valueOf(validatorId),
                    String.valueOf(approve), String.valueOf(reason)));
            }

            @Override
            public void applyGCExecute(String proposalId, int executorId) {
                calls.add(String.join("|", "gcExecute", proposalId, String.valueOf(executorId)));
            }
        });
        dispatcher.setDurabilityCallback(new MessageDispatcher.DurabilityCallback() {
            @Override
            public void onQueueSegment(String proposalId, int totalMembers, int requiredAcks) {
                calls.add(String.join("|", "queue", proposalId, String.valueOf(totalMembers),
                    String.valueOf(requiredAcks)));
            }

            @Override
            public void onSegmentPersisted(String proposalId, int memberId, String durableHead, boolean success,
                                           String error) {
                calls.add(String.join("|", "persisted", proposalId, String.valueOf(memberId), durableHead,
                    String.valueOf(success), error));
            }

            @Override
            public void onAckSegmentPersisted(String proposalId, boolean success, String durableHead, String error,
                                              int totalMembers, int requiredAcks) {
                calls.add(String.join("|", "ack", proposalId, String.valueOf(success), durableHead, error,
                    String.valueOf(totalMembers), String.valueOf(requiredAcks)));
            }
        });
        dispatcher.setTransactionCallback(new MessageDispatcher.TransactionCallback() {
            @Override
            public void onStartTransaction(String transactionId, String correlationId, long timeoutMs,
                                           String initiatorWallet) {
                calls.add(String.join("|", "start", transactionId, correlationId, String.valueOf(timeoutMs),
                    initiatorWallet));
            }

            @Override
            public void onCommitTransaction(String transactionId, String correlationId) {
                calls.add(String.join("|", "commit", transactionId, correlationId));
            }

            @Override
            public void onAbortTransaction(String transactionId, String correlationId, String reason) {
                calls.add(String.join("|", "abort", transactionId, correlationId, reason));
            }
        });
        return dispatcher;
    }

    private static String audit(MutationAuditMetadata a) {
        Function<Object, String> v = String::valueOf;
        return String.join("|", a.getOperation().name(), v.apply(a.getTransactionId()),
            v.apply(a.getCorrelationId()), v.apply(a.getProposalId()), v.apply(a.getEthereumTxHash()),
            v.apply(a.getConfirmedBlockNumber()), v.apply(a.getEthereumObservedEpoch()),
            v.apply(a.getEthereumFinalizedEpoch()));
    }

    private static boolean dispatch(MessageDispatcher dispatcher, AeronEncodedMessage encoded) {
        return dispatcher.dispatch(1L, encoded.buffer, 0, encoded.totalLength);
    }

    private static boolean dispatch(MessageDispatcher dispatcher, int templateId, String json) {
        return dispatch(dispatcher, AeronIngressPayloadSupport.encode(templateId, json));
    }
}
