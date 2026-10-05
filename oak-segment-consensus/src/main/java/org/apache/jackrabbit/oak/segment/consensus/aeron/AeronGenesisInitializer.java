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
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.segment.consensus.validation.MutationRejectedException;
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

    void initializeGenesisContent(GenesisProposal proposal) {
        initializeGenesisContent(proposal, null);
    }

    /**
     * Applies the first GENESIS entry of the log; later ones verify the existing genesis and change nothing. The
     * applied-log watermark is written in the same Oak merge as genesis. A failure propagates so that this member
     * stops instead of running on without genesis while the others have it.
     */
    void initializeGenesisContent(GenesisProposal proposal, AppliedLogPosition logPosition) {
        log.info("Creating deterministic genesis from replicated proposal: validator={}, timestamp={}",
            proposal.getGenesisValidatorUrl(), proposal.getTimestamp());

        try {
            if (canonicalGenesisContent.exists()) {
                canonicalGenesisContent.verifyExisting();
                log.info("Canonical genesis already exists - skipping duplicate genesis application");
                return;
            }

            NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            canonicalGenesisContent.populate(rootBuilder, proposal.getTimestamp(), proposal.getGenesisValidatorUrl());
            if (logPosition != null) {
                logPosition.writeTo(rootBuilder);
            }
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();
            canonicalGenesisContent.verifyExisting();

            String newHead = fileStore.getHead().getRecordId().toString10();
            log.info("Genesis committed deterministically - validator={}, timestamp={}, head={}",
                proposal.getGenesisValidatorUrl(), proposal.getTimestamp(), newHead);
        } catch (Exception e) {
            log.error("Exception during genesis creation", e);
            throw new IllegalStateException("Canonical genesis application failed", e);
        }
    }

    boolean verifyExistingGenesis() {
        if (!canonicalGenesisContent.exists()) {
            return false;
        }
        canonicalGenesisContent.verifyExisting();
        return true;
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

        static void validateTrigger(String json) {
            try {
                JsopTokenizer reader = new JsopTokenizer(json);
                reader.read('{');
                if (!"command".equals(reader.readString())) {
                    throw new IllegalArgumentException("Expected command");
                }
                reader.read(':');
                if (!"CREATE_GENESIS".equals(reader.readString())) {
                    throw new IllegalArgumentException("Expected CREATE_GENESIS");
                }
                reader.read('}');
                reader.read(JsopReader.END);
            } catch (RuntimeException e) {
                throw new MutationRejectedException("Genesis requires a parameter-free CREATE_GENESIS trigger", e);
            }
        }

        long getTimestamp() {
            return timestamp;
        }

        String getGenesisValidatorUrl() {
            return genesisValidatorUrl;
        }

        String toJson() {
            return "{\"command\":\"CREATE_GENESIS\"}";
        }
    }
}
