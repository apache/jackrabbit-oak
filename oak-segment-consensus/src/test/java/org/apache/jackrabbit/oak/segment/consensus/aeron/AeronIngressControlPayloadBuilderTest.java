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

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AeronIngressControlPayloadBuilderTest {

    private final AeronIngressControlPayloadBuilder builder = new AeronIngressControlPayloadBuilder();

    @Test
    public void startTransactionDefaultsTimeoutAndEscapesOptionalFields() {
        AeronEncodedMessage encoded = builder.buildStartTransaction("tx\"1", "corr\n1", 0L, "0xabc", 9);

        assertEquals(SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION, encoded.templateId);
        assertTrue(encoded.json.contains("\"transactionId\":\"tx\\\"1\""));
        assertTrue(encoded.json.contains("\"correlationId\":\"corr\\n1\""));
        assertTrue(encoded.json.contains("\"initiatorWallet\":\"0xabc\""));
        assertTrue(encoded.json.contains("\"timeoutMs\":30000"));
        assertTrue(encoded.json.contains("\"term\":9"));
        assertPayloadMatches(encoded);
    }

    @Test
    public void commitTransactionOmitsMissingCorrelationId() {
        AeronEncodedMessage encoded = builder.buildCommitTransaction("tx-2", null, 4);

        assertEquals(SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION, encoded.templateId);
        assertFalse(encoded.json.contains("\"correlationId\""));
        assertTrue(encoded.json.contains("\"term\":4"));
        assertPayloadMatches(encoded);
    }

    @Test
    public void transactionCommandsOmitUnknownTerm() {
        assertFalse(builder.buildStartTransaction("tx-4", null, 0L, null, null).json.contains("\"term\""));
        assertFalse(builder.buildCommitTransaction("tx-4", null, null).json.contains("\"term\""));
        assertFalse(builder.buildAbortTransaction("tx-4", null, null, null).json.contains("\"term\""));
    }

    @Test
    public void abortTransactionIncludesReasonWhenPresent() {
        AeronEncodedMessage encoded = builder.buildAbortTransaction("tx-3", "corr-3", "quota exceeded", 7);

        assertEquals(SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION, encoded.templateId);
        assertTrue(encoded.json.contains("\"reason\":\"quota exceeded\""));
        assertTrue(encoded.json.contains("\"term\":7"));
        assertPayloadMatches(encoded);
    }

    @Test
    public void durabilityMessagesEncodeOptionalFields() {
        AeronEncodedMessage persisted = builder.buildSegmentPersisted("proposal-1", 4, false, "head-1", "disk full");
        AeronEncodedMessage withoutError = builder.buildSegmentPersisted("proposal-1", 5, true, "head-2", null);

        assertEquals(SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED, persisted.templateId);
        assertTrue(persisted.json.contains("\"memberId\":4"));
        assertTrue(persisted.json.contains("\"error\":\"disk full\""));
        assertTrue(withoutError.json.contains("\"durableHead\":\"head-2\""));
        assertFalse(withoutError.json.contains("\"error\""));
        assertPayloadMatches(persisted);
        assertPayloadMatches(withoutError);
    }

    @Test
    public void gcMessagesDefaultHeadAndReasonFields() {
        AeronEncodedMessage proposal = builder.buildGcProposal("gc-1", "0xwallet", null, 128L, null);
        AeronEncodedMessage vote = builder.buildGcVote("gc-1", 5, true, null);
        AeronEncodedMessage execute = builder.buildGcExecute("gc-1", 6);

        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL, proposal.templateId);
        assertTrue(proposal.json.contains("\"targetRevision\":\"HEAD\""));
        assertTrue(proposal.json.contains("\"estimatedCostUSDC\":\"0\""));
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_VOTE, vote.templateId);
        assertTrue(vote.json.contains("\"reason\":\"\""));
        assertEquals(SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE, execute.templateId);
        assertTrue(execute.json.contains("\"executorId\":6"));
        assertPayloadMatches(proposal);
        assertPayloadMatches(vote);
        assertPayloadMatches(execute);
    }

    @Test
    public void escapeJsonNormalizesNullAndControlCharacters() {
        assertEquals("", builder.escapeJson(null));
        assertEquals("\\\"quote\\\"\\\\slash\\n", builder.escapeJson("\"quote\"\\slash\n"));
    }

    private static void assertPayloadMatches(AeronEncodedMessage encoded) {
        SimpleMessageHeader.HeaderInfo header = SimpleMessageHeader.decode(encoded.buffer, 0);
        byte[] payload = new byte[encoded.totalLength - SimpleMessageHeader.ENCODED_LENGTH];
        encoded.buffer.getBytes(SimpleMessageHeader.ENCODED_LENGTH, payload);
        assertEquals(encoded.templateId, header.templateId);
        assertEquals(payload.length, header.blockLength);
        assertEquals(encoded.json, new String(payload, StandardCharsets.UTF_8));
    }
}
