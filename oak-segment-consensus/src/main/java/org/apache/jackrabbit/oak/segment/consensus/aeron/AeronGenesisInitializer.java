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

import org.apache.jackrabbit.oak.segment.consensus.genesis.CanonicalGenesisContent;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AeronGenesisInitializer {

    private static final Logger log = LoggerFactory.getLogger(AeronGenesisInitializer.class);

    static final String GENESIS_ADDRESS = CanonicalGenesisContent.GENESIS_ADDRESS;

    private final FileStore fileStore;
    private final NodeStore nodeStore;
    private final CanonicalGenesisContent canonicalGenesisContent;

    AeronGenesisInitializer(FileStore fileStore, NodeStore nodeStore, BlobStore blobStore) {
        this.fileStore = fileStore;
        this.nodeStore = nodeStore;
        this.canonicalGenesisContent = new CanonicalGenesisContent(nodeStore, blobStore);
    }

    void initializeGenesisContent(String proposalJson) {
        initializeGenesisContent(proposalJson, null);
    }

    void initializeGenesisContent(String proposalJson, AppliedLogPosition logPosition) {
        initializeGenesisContent(GenesisProposal.fromJson(proposalJson), logPosition);
    }

    void initializeGenesisContent(GenesisProposal proposal, AppliedLogPosition logPosition) {
        log.info("Creating deterministic genesis from replicated proposal: validator={}, timestamp={}",
            proposal.getGenesisValidatorUrl(), proposal.getTimestamp());

        try {
            if (canonicalGenesisContent.exists()) {
                log.info("Canonical genesis already exists - skipping duplicate genesis application");
                return;
            }

            NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            canonicalGenesisContent.populate(rootBuilder, proposal.getTimestamp(), proposal.getGenesisValidatorUrl());
            if (logPosition != null) {
                logPosition.writeTo(rootBuilder);
            }
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            String newHead = fileStore.getHead().getRecordId().toString10();
            log.info("Genesis committed deterministically - validator={}, timestamp={}, head={}",
                proposal.getGenesisValidatorUrl(), proposal.getTimestamp(), newHead);
        } catch (Exception e) {
            log.error("Exception during genesis creation", e);
        }
    }

    static final class GenesisProposal {
        private final long timestamp;
        private final String genesisValidatorUrl;

        private GenesisProposal(long timestamp, String genesisValidatorUrl) {
            this.timestamp = timestamp;
            this.genesisValidatorUrl = genesisValidatorUrl;
        }

        static GenesisProposal create(long timestamp, String genesisValidatorUrl) {
            long normalizedTimestamp = timestamp > 0 ? timestamp : 0L;
            String normalizedValidator = genesisValidatorUrl != null && !genesisValidatorUrl.trim().isEmpty()
                ? genesisValidatorUrl.trim()
                : CanonicalGenesisContent.DEFAULT_GENESIS_VALIDATOR_URL;
            return new GenesisProposal(normalizedTimestamp, normalizedValidator);
        }

        static GenesisProposal fromJson(String json) {
            String source = json != null ? json.trim() : "";
            long timestamp = extractLongField(source, "timestamp");
            String genesisValidator = extractJsonField(source, "genesisValidator");
            return create(timestamp, genesisValidator);
        }

        long getTimestamp() {
            return timestamp;
        }

        String getGenesisValidatorUrl() {
            return genesisValidatorUrl;
        }

        String toJson() {
            return "{\"command\":\"CREATE_GENESIS\",\"timestamp\":" + timestamp
                + ",\"genesisValidator\":\"" + escapeJson(genesisValidatorUrl) + "\"}";
        }

        private static long extractLongField(String json, String field) {
            String marker = "\"" + field + "\":";
            int start = json.indexOf(marker);
            if (start < 0) {
                return 0L;
            }
            int valueStart = start + marker.length();
            int valueEnd = valueStart;
            while (valueEnd < json.length() && Character.isDigit(json.charAt(valueEnd))) {
                valueEnd++;
            }
            if (valueEnd == valueStart) {
                return 0L;
            }
            try {
                return Long.parseLong(json.substring(valueStart, valueEnd));
            } catch (NumberFormatException e) {
                return 0L;
            }
        }

        private static String extractJsonField(String json, String field) {
            String marker = "\"" + field + "\":\"";
            int start = json.indexOf(marker);
            if (start < 0) {
                return null;
            }
            int valueStart = start + marker.length();
            StringBuilder value = new StringBuilder();
            boolean escaping = false;
            for (int i = valueStart; i < json.length(); i++) {
                char ch = json.charAt(i);
                if (escaping) {
                    value.append(ch);
                    escaping = false;
                } else if (ch == '\\') {
                    escaping = true;
                } else if (ch == '"') {
                    return value.toString();
                } else {
                    value.append(ch);
                }
            }
            return null;
        }

        private static String escapeJson(String input) {
            if (input == null) {
                return "";
            }
            return input.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
