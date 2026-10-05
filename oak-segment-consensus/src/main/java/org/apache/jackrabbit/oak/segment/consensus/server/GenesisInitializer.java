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
package org.apache.jackrabbit.oak.segment.consensus.server;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.consensus.genesis.CanonicalGenesisContent;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GenesisInitializer {

    private static final Logger log = LoggerFactory.getLogger(GenesisInitializer.class);

    private final FileStore fileStore;
    private final CanonicalGenesisContent canonicalGenesisContent;

    GenesisInitializer(NodeStore nodeStore, FileStore fileStore, BlobStore blobStore) {
        this.fileStore = fileStore;
        this.canonicalGenesisContent = new CanonicalGenesisContent(nodeStore, blobStore);
    }

    /**
     * Verifies an existing genesis. A missing genesis is left alone: it is created only by applying the GENESIS
     * entry of the consensus log, with Aeron's command timestamp and the applied-log watermark. A merge here would
     * use this node's clock and make the later GENESIS entry a no-op on this node only.
     */
    void initializeGenesisContent() {
        try {
            if (canonicalGenesisContent.exists()) {
                log.info("   ℹ️  Canonical genesis already exists - verifying integrity...");
                canonicalGenesisContent.verifyExisting();
                log.info("   ✅ Canonical genesis integrity verified at {}", CanonicalGenesisContent.getGenesisPath());
                logGenesisHead();
                return;
            }

            log.info("   ⏭️  No genesis yet - it is created from the consensus log");
        } catch (Exception e) {
            log.error("   ❌ FATAL: Failed to verify canonical genesis: {}", e.getMessage(), e);
            throw new RuntimeException("Genesis verification failed - cannot start network", e);
        }
    }

    private void logGenesisHead() {
        RecordId genesisHead = fileStore.getHead().getRecordId();
        log.info("   Genesis HEAD: {}", genesisHead.toString10());
    }
}
