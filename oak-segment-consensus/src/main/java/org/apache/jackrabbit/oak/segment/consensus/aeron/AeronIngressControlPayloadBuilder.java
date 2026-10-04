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

final class AeronIngressControlPayloadBuilder {

    AeronEncodedMessage buildStartTransaction(String transactionId,
                                              String correlationId,
                                              long timeoutMs,
                                              String initiatorWallet,
                                              Integer term) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"transactionId\":\"").append(escapeJson(transactionId)).append("\"");
        if (correlationId != null && !correlationId.isEmpty()) {
            json.append(",\"correlationId\":\"").append(escapeJson(correlationId)).append("\"");
        }
        if (initiatorWallet != null && !initiatorWallet.isEmpty()) {
            json.append(",\"initiatorWallet\":\"").append(escapeJson(initiatorWallet)).append("\"");
        }
        json.append(",\"timeoutMs\":").append(timeoutMs > 0 ? timeoutMs : 30000L);
        appendTerm(json, term);
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_START_TRANSACTION, json.toString());
    }

    AeronEncodedMessage buildCommitTransaction(String transactionId,
                                               String correlationId,
                                               Integer term) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"transactionId\":\"").append(escapeJson(transactionId)).append("\"");
        if (correlationId != null && !correlationId.isEmpty()) {
            json.append(",\"correlationId\":\"").append(escapeJson(correlationId)).append("\"");
        }
        appendTerm(json, term);
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_COMMIT_TRANSACTION, json.toString());
    }

    AeronEncodedMessage buildAbortTransaction(String transactionId,
                                              String correlationId,
                                              String reason,
                                              Integer term) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"transactionId\":\"").append(escapeJson(transactionId)).append("\"");
        if (correlationId != null && !correlationId.isEmpty()) {
            json.append(",\"correlationId\":\"").append(escapeJson(correlationId)).append("\"");
        }
        if (reason != null && !reason.isEmpty()) {
            json.append(",\"reason\":\"").append(escapeJson(reason)).append("\"");
        }
        appendTerm(json, term);
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_ABORT_TRANSACTION, json.toString());
    }

    private static void appendTerm(StringBuilder json, Integer term) {
        if (term != null) {
            json.append(",\"term\":").append(term.intValue());
        }
    }

    AeronEncodedMessage buildQueueSegment(String proposalId, int totalMembers, int requiredAcks) {
        String json = "{\"proposalId\":\"" + escapeJson(proposalId) + "\"," +
            "\"totalMembers\":" + totalMembers + "," +
            "\"requiredAcks\":" + requiredAcks + "}";
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_QUEUE_SEGMENT, json);
    }

    AeronEncodedMessage buildSegmentPersisted(String proposalId,
                                              int memberId,
                                              boolean success,
                                              String durableHead,
                                              String error) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"proposalId\":\"").append(escapeJson(proposalId)).append("\",");
        json.append("\"memberId\":").append(memberId).append(",");
        json.append("\"success\":").append(success);
        if (durableHead != null && !durableHead.isEmpty()) {
            json.append(",\"durableHead\":\"").append(escapeJson(durableHead)).append("\"");
        }
        if (error != null && !error.isEmpty()) {
            json.append(",\"error\":\"").append(escapeJson(error)).append("\"");
        }
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_SEGMENT_PERSISTED, json.toString());
    }

    AeronEncodedMessage buildAckSegmentPersisted(String proposalId,
                                                 boolean success,
                                                 String durableHead,
                                                 String error,
                                                 int totalMembers,
                                                 int requiredAcks) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"proposalId\":\"").append(escapeJson(proposalId)).append("\",");
        json.append("\"success\":").append(success).append(",");
        json.append("\"totalMembers\":").append(totalMembers).append(",");
        json.append("\"requiredAcks\":").append(requiredAcks);
        if (durableHead != null && !durableHead.isEmpty()) {
            json.append(",\"durableHead\":\"").append(escapeJson(durableHead)).append("\"");
        }
        if (error != null && !error.isEmpty()) {
            json.append(",\"error\":\"").append(escapeJson(error)).append("\"");
        }
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_ACK_SEGMENT_PERSISTED, json.toString());
    }

    AeronEncodedMessage buildGcProposal(String proposalId,
                                        String proposerWallet,
                                        String targetRevision,
                                        long estimatedReclaimableSizeMB,
                                        String estimatedCostUSDC) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"proposalId\":\"").append(escapeJson(proposalId)).append("\",");
        json.append("\"proposerWallet\":\"").append(escapeJson(proposerWallet)).append("\",");
        json.append("\"targetRevision\":\"")
            .append(escapeJson(targetRevision != null ? targetRevision : "HEAD"))
            .append("\",");
        json.append("\"estimatedReclaimableSizeMB\":").append(estimatedReclaimableSizeMB).append(",");
        json.append("\"estimatedCostUSDC\":\"")
            .append(escapeJson(estimatedCostUSDC != null ? estimatedCostUSDC : "0"))
            .append("\"");
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_GC_PROPOSAL, json.toString());
    }

    AeronEncodedMessage buildGcVote(String proposalId,
                                    int validatorId,
                                    boolean approve,
                                    String reason) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"proposalId\":\"").append(escapeJson(proposalId)).append("\",");
        json.append("\"validatorId\":").append(validatorId).append(",");
        json.append("\"approve\":").append(approve).append(",");
        json.append("\"reason\":\"").append(escapeJson(reason != null ? reason : "")).append("\"");
        json.append("}");
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_GC_VOTE, json.toString());
    }

    AeronEncodedMessage buildGcExecute(String proposalId, int executorId) {
        String json = "{\"proposalId\":\"" + escapeJson(proposalId) + "\"," +
            "\"executorId\":" + executorId + "}";
        return AeronIngressPayloadSupport.encode(SimpleMessageHeader.TEMPLATE_ID_GC_EXECUTE, json);
    }

    String escapeJson(String value) {
        return AeronIngressPayloadSupport.escapeJson(value);
    }
}
