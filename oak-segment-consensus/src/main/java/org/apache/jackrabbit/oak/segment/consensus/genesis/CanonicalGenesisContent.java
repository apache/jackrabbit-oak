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
package org.apache.jackrabbit.oak.segment.consensus.genesis;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.segment.consensus.config.IpfsGatewayUrls;
import org.apache.jackrabbit.oak.segment.consensus.util.WalletPathUtil;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Canonical authoring surface for OakChain genesis content.
 *
 * <p>This class is the single source of truth for:
 * <ul>
 *   <li>the canonical wallet-scoped genesis path</li>
 *   <li>the authored bootstrap content published under genesis</li>
 *   <li>the minimal integrity markers used by startup verification</li>
 * </ul>
 */
public final class CanonicalGenesisContent {

    public static final String GENESIS_ADDRESS = "0x0000000000000000000000000000000000000000";
    public static final String DEFAULT_GENESIS_VALIDATOR_URL = "http://localhost:8090";

    private static final String GENESIS_IMAGE_RESOURCE = "genesis-assets/do-it-live.jpeg";
    private static final String GENESIS_MESSAGE = "DO IT LIVE!";
    private static final String CHAIN_ID = "oak-blockchain-aem";
    private static final String VERSION = "1.0.0";
    private static final String CANONICAL_GENESIS_PATH = WalletPathUtil.getContentPath(GENESIS_ADDRESS) + "/genesis";
    // java.util.Date#toString layout, fixed to UTC: the date is replicated content, so it must not follow the JVM zone.
    private static final DateTimeFormatter GENESIS_DATE =
        DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US).withZone(ZoneId.of("UTC"));

    private final NodeStore nodeStore;
    private final BlobStore blobStore;

    public CanonicalGenesisContent(NodeStore nodeStore, BlobStore blobStore) {
        this.nodeStore = nodeStore;
        this.blobStore = blobStore;
    }

    public static String getGenesisPath() {
        return CANONICAL_GENESIS_PATH;
    }

    public static NodeState getGenesisNode(NodeState root) {
        String[] levels = WalletPathUtil.getShardLevels(GENESIS_ADDRESS);
        return root.getChildNode("oak-chain")
            .getChildNode(levels[0])
            .getChildNode(levels[1])
            .getChildNode(levels[2])
            .getChildNode(GENESIS_ADDRESS)
            .getChildNode("content")
            .getChildNode("genesis");
    }

    public boolean exists() {
        return getGenesisNode(nodeStore.getRoot()).exists();
    }

    public void verifyExisting() {
        NodeState genesis = getGenesisNode(nodeStore.getRoot());
        if (!genesis.exists()) {
            throw new IllegalStateException("Canonical genesis is missing at " + CANONICAL_GENESIS_PATH);
        }
        verifyString(genesis, "message", GENESIS_MESSAGE);
        verifyString(genesis, "chainId", CHAIN_ID);
        if (!genesis.getChildNode("content-contract").exists()) {
            throw new IllegalStateException("Canonical genesis is missing content-contract guidance");
        }
    }

    public void populate(NodeBuilder rootBuilder, long timestamp, String genesisValidatorUrl) throws Exception {
        long normalizedTimestamp = Math.max(0L, timestamp);
        String genesisDate = GENESIS_DATE.format(Instant.ofEpochMilli(normalizedTimestamp));
        String genesisValidator = normalizeGenesisValidatorUrl(genesisValidatorUrl);
        String genesisHost = extractHost(genesisValidator);

        NodeBuilder oakChain = rootBuilder.child("oak-chain");
        oakChain.setProperty("jcr:primaryType", "nt:unstructured");

        String[] levels = WalletPathUtil.getShardLevels(GENESIS_ADDRESS);

        NodeBuilder level1 = oakChain.child(levels[0]);
        level1.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder level2 = level1.child(levels[1]);
        level2.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder level3 = level2.child(levels[2]);
        level3.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder genesisWallet = level3.child(GENESIS_ADDRESS);
        genesisWallet.setProperty("jcr:primaryType", "nt:unstructured");
        genesisWallet.setProperty("wallet", GENESIS_ADDRESS);
        genesisWallet.setProperty("role", "genesis");
        genesisWallet.setProperty("walletCreated", normalizedTimestamp);
        genesisWallet.setProperty("nodeType", "wallet-root");
        genesisWallet.setProperty("description", "Genesis wallet - canonical bootstrap knowledge capsule for the OakChain fabric");
        genesisWallet.setProperty("contentCount", 1L);
        genesisWallet.setProperty("totalWrites", 1L);
        genesisWallet.setProperty("lastWrite", normalizedTimestamp);
        genesisWallet.setProperty("owner", "OakChain Network");
        genesisWallet.setProperty("verified", true);

        NodeBuilder content = genesisWallet.child("content");
        content.setProperty("jcr:primaryType", "nt:unstructured");

        NodeBuilder genesis = content.child("genesis");
        genesis.setProperty("jcr:primaryType", "nt:unstructured");
        genesis.setProperty("jcr:created", normalizedTimestamp);
        genesis.setProperty("jcr:title", "OakChain Network - Self-Documenting Genesis");
        genesis.setProperty("jcr:description",
            "This node is the canonical bootstrap brief for OakChain. Joining validators, CLI clients, "
                + "and operators should read here first.");
        genesis.setProperty("message", GENESIS_MESSAGE);
        genesis.setProperty("tagline", "Billions of enterprise content rides these rails. We're making it decentralized.");
        genesis.setProperty("ethos", "Trust the data, not the operator. Determinism first. Availability without ambiguity.");
        genesis.setProperty("northStar", "Make content verifiable, portable, and durable at global enterprise scale.");
        genesis.setProperty("version", VERSION);
        genesis.setProperty("chainId", CHAIN_ID);
        genesis.setProperty("consensusModel", "aeron-raft");
        genesis.setProperty("genesisTimestamp", normalizedTimestamp);
        genesis.setProperty("genesisDate", genesisDate);
        genesis.setProperty("genesisValidator", genesisValidator);
        genesis.setProperty("canonicalGenesisPath", CANONICAL_GENESIS_PATH);
        genesis.setProperty("walletNamespaceContract", "The wallet namespace boundary is the only enforced content contract today.");
        genesis.setProperty("contentShapePolicy", "Below /content the structure is intentionally evolving; treat it as living protocol surface, not a frozen taxonomy.");
        genesis.setProperty("startHere", "Read protocol, content-contract, architecture, api, and getting-started before writing clients.");
        genesis.setProperty("governedSurface", "/v1/index");

        populateProtocol(genesis, normalizedTimestamp, genesisDate, genesisValidator, genesisHost);
        populateContentContract(genesis);
        populateGettingStarted(genesis, genesisHost);
        populateApi(genesis);
        populateExamples(genesis);
        populateArchitecture(genesis);
        populateEconomics(genesis);
        populateTroubleshooting(genesis);
        populateAbout(genesis);

        String ipfsCid = populateGenesisImage(genesis, normalizedTimestamp);

        NodeBuilder ipfsInfo = genesis.child("ipfs");
        ipfsInfo.setProperty("jcr:primaryType", "nt:unstructured");
        ipfsInfo.setProperty("enabled", blobStore != null);
        ipfsInfo.setProperty("genesisImageCid", ipfsCid != null ? ipfsCid : "N/A (BlobStore fallback)");
        ipfsInfo.setProperty("gateway", IpfsGatewayUrls.gatewayBase());
        ipfsInfo.setProperty("localGateway", IpfsGatewayUrls.localGatewayBase());
        ipfsInfo.setProperty("description", "Binaries stored via IPFS - content-addressed, decentralized, immutable");
    }

    private void populateProtocol(NodeBuilder genesis, long timestamp, String genesisDate, String genesisValidator, String genesisHost) {
        NodeBuilder protocol = genesis.child("protocol");
        protocol.setProperty("jcr:primaryType", "nt:unstructured");
        protocol.setProperty("message", GENESIS_MESSAGE);
        protocol.setProperty("version", VERSION);
        protocol.setProperty("chainId", CHAIN_ID);
        protocol.setProperty("consensusModel", "aeron-raft");
        protocol.setProperty("genesisTimestamp", timestamp);
        protocol.setProperty("genesisDate", genesisDate);
        protocol.setProperty("genesisValidator", genesisValidator);
        protocol.setProperty("genesisHost", genesisHost);
        protocol.setProperty("canonicalGenesisPath", CANONICAL_GENESIS_PATH);
        protocol.setProperty("sourceOfTruth", "This node and /v1/index are the canonical bootstrap references for OakChain.");
        protocol.setProperty("description",
            "Genesis is a code-authored briefing for the network fabric. Update the authoring code, not live instances.");
    }

    private void populateContentContract(NodeBuilder genesis) {
        NodeBuilder contract = genesis.child("content-contract");
        contract.setProperty("jcr:primaryType", "nt:unstructured");
        contract.setProperty("jcr:title", "Wallet Namespace Contract");
        contract.setProperty("enforcedBoundary",
            "Only the wallet namespace /oak-chain/{L1}/{L2}/{L3}/{wallet} is structurally enforced.");
        contract.setProperty("contentRootPattern", "/oak-chain/{L1}/{L2}/{L3}/{wallet}/content");
        contract.setProperty("contentShapeStatus", "Below /content the shape is intentionally open and may evolve.");
        contract.setProperty("guidance", "Treat subtree conventions as living protocol material published by code, not a frozen schema.");
        contract.setProperty("genesisRole", "Genesis is the starting node for cluster joiners, operators, and the future CLI.");
        contract.setProperty("genesisWallet", WalletPathUtil.getShardRoot(GENESIS_ADDRESS));
    }

    private void populateGettingStarted(NodeBuilder genesis, String genesisHost) {
        NodeBuilder gettingStarted = genesis.child("getting-started");
        gettingStarted.setProperty("jcr:primaryType", "nt:unstructured");
        gettingStarted.setProperty("jcr:title", "Getting Started with OakChain");
        gettingStarted.setProperty("jcr:description",
            "The shortest path to understanding the fabric, joining a cluster, and reading canonical chain state.");

        NodeBuilder prereqs = gettingStarted.child("1-prerequisites");
        prereqs.setProperty("jcr:primaryType", "nt:unstructured");
        prereqs.setProperty("title", "Prerequisites");
        prereqs.setProperty("item-1", "A validator URL");
        prereqs.setProperty("item-2", "curl, jq, or another HTTP client");
        prereqs.setProperty("item-3", "An Ethereum wallet for write/payment flows");
        prereqs.setProperty("item-4", "The discipline to trust the live contract surface, not stale examples");

        NodeBuilder discover = gettingStarted.child("2-discover-surface");
        discover.setProperty("jcr:primaryType", "nt:unstructured");
        discover.setProperty("title", "Discover the Live Surface");
        discover.setProperty("curl-health", "curl http://VALIDATOR:8090/health");
        discover.setProperty("curl-manifest", "curl http://VALIDATOR:8090/v1/index | jq .");
        discover.setProperty("curl-consensus", "curl http://VALIDATOR:8090/v1/consensus/status | jq .");
        discover.setProperty("note", "Use /v1/index as route truth. Local HTML routes are diagnostic veneers.");

        NodeBuilder browseGenesis = gettingStarted.child("3-browse-genesis");
        browseGenesis.setProperty("jcr:primaryType", "nt:unstructured");
        browseGenesis.setProperty("title", "Open the Canonical Genesis Node");
        browseGenesis.setProperty("step-1", "GET /v1/explorer/content/nav and pick a clusterId.");
        browseGenesis.setProperty("step-2", "Use the selected clusterId with the node endpoint.");
        browseGenesis.setProperty("step-3",
            "GET /v1/explorer/content/clusters/{clusterId}/node?path=" + CANONICAL_GENESIS_PATH);
        browseGenesis.setProperty("step-4",
            "GET /v1/explorer/content/clusters/{clusterId}/provenance?path=" + CANONICAL_GENESIS_PATH);
        browseGenesis.setProperty("note", "CRX/OC should open here by default; future CLI bootstrap should read this same node.");

        NodeBuilder joinCluster = gettingStarted.child("4-join-cluster");
        joinCluster.setProperty("jcr:primaryType", "nt:unstructured");
        joinCluster.setProperty("title", "Join the Cluster");
        joinCluster.setProperty("env-1", "BOOTSTRAP_PRIMARY_HOST=" + genesisHost);
        joinCluster.setProperty("env-2", "BOOTSTRAP_PRIMARY_PORT=8091");
        joinCluster.setProperty("env-3", "CONSENSUS_ENABLED=true");
        joinCluster.setProperty("env-4", "CONSENSUS_MODE=aeron");
        joinCluster.setProperty("env-5", "CONSENSUS_SELF_URL=http://your-validator:8090");
        joinCluster.setProperty("env-6", "AERON_CLUSTER_NODE_ID=<unique-id>");
        joinCluster.setProperty("note", "A joining validator should sync the same genesis and then follow Aeron cluster authority.");

        NodeBuilder writeFlow = gettingStarted.child("5-write-flow");
        writeFlow.setProperty("jcr:primaryType", "nt:unstructured");
        writeFlow.setProperty("title", "Submit a Write");
        writeFlow.setProperty("step-1", "Obtain a chain-backed proposalId from the authorize/payment contract flow.");
        writeFlow.setProperty("step-2",
            "POST walletAddress, signature, message, contentType, and proposalId to /v1/propose-write.");
        writeFlow.setProperty("step-3", "Track /v1/proposals/{id}/status until finalized.");
        writeFlow.setProperty("step-4", "Inspect queue and adaptive release state via /v1/proposals/queue/stats and /v1/proposals/release-flow.");
        writeFlow.setProperty("note", "The validator allocates the resulting storage path under your wallet namespace.");

        NodeBuilder readWallet = gettingStarted.child("6-read-wallet");
        readWallet.setProperty("jcr:primaryType", "nt:unstructured");
        readWallet.setProperty("title", "Read Wallet-Centric State");
        readWallet.setProperty("curl-wallet-stats",
            "curl --get http://VALIDATOR:8090/v1/wallets/stats --data-urlencode 'wallet=0xYOUR_WALLET' | jq .");
        readWallet.setProperty("curl-wallet-content",
            "curl --get http://VALIDATOR:8090/v1/wallets/content --data-urlencode 'wallet=0xYOUR_WALLET' | jq .");
        readWallet.setProperty("note", "Use the explorer content contract for tree/node/provenance and wallet queries for wallet-centric listings.");
    }

    private void populateApi(NodeBuilder genesis) {
        NodeBuilder api = genesis.child("api");
        api.setProperty("jcr:primaryType", "nt:unstructured");
        api.setProperty("jcr:title", "API Reference");
        api.setProperty("jcr:description", "Live validator contracts that matter for bootstrap, operations, and content.");
        api.setProperty("baseUrl", "http://VALIDATOR:8090");
        api.setProperty("manifest", "/v1/index");

        NodeBuilder discovery = api.child("discovery");
        discovery.setProperty("jcr:primaryType", "nt:unstructured");
        discovery.setProperty("GET_health", "Shallow health");
        discovery.setProperty("GET_health_deep", "Deep dependency health");
        discovery.setProperty("GET_v1_index", "Live validator surface manifest");
        discovery.setProperty("GET_v1_head", "Head status");

        NodeBuilder consensus = api.child("consensus");
        consensus.setProperty("jcr:primaryType", "nt:unstructured");
        consensus.setProperty("GET_v1_consensus_status", "Consensus status and cluster health");
        consensus.setProperty("GET_v1_consensus_leader", "Canonical leader resolution");
        consensus.setProperty("POST_v1_propose_write", "Submit signed write proposal");
        consensus.setProperty("POST_v1_propose_delete", "Submit signed delete proposal");
        consensus.setProperty("GET_v1_proposals_id_status", "Proposal status by id");
        consensus.setProperty("GET_v1_proposals_queue_stats", "Queue, backpressure, and finality counters");
        consensus.setProperty("GET_v1_proposals_release_flow", "Adaptive verified-release state");

        NodeBuilder explorer = api.child("explorer");
        explorer.setProperty("jcr:primaryType", "nt:unstructured");
        explorer.setProperty("GET_v1_explorer_summary", "Explorer summary contract");
        explorer.setProperty("GET_v1_explorer_content_nav", "Cluster-aware content navigation");
        explorer.setProperty("GET_v1_explorer_content_tree", "Cluster-scoped content tree browse");
        explorer.setProperty("GET_v1_explorer_content_node", "Cluster-scoped node detail");
        explorer.setProperty("GET_v1_explorer_content_provenance", "Cluster-scoped provenance and authority facts");

        NodeBuilder wallets = api.child("wallets");
        wallets.setProperty("jcr:primaryType", "nt:unstructured");
        wallets.setProperty("GET_v1_wallets_stats", "Wallet usage and counts");
        wallets.setProperty("GET_v1_wallets_content", "Wallet content query");

        NodeBuilder binary = api.child("binary");
        binary.setProperty("jcr:primaryType", "nt:unstructured");
        binary.setProperty("POST_v1_binary_declare_intent", "Declare binary upload intent");
        binary.setProperty("GET_v1_binary_check_intent_token", "Check binary upload intent");
        binary.setProperty("POST_v1_binary_complete_upload", "Complete binary upload");
        binary.setProperty("note", "Binaries resolve through BlobStore/IPFS while consensus stores the references.");

        NodeBuilder config = api.child("configuration");
        config.setProperty("jcr:primaryType", "nt:unstructured");
        config.setProperty("GET_v1_config_osgi", "Effective OSGi config values");
        config.setProperty("GET_v1_config_osgi_schema", "OSGi config metadata schema");
        config.setProperty("GET_v1_config_osgi_sources", "OSGi config source map");
        config.setProperty("GET_v1_config_osgi_delta", "Current values vs defaults");

        NodeBuilder replication = api.child("replication");
        replication.setProperty("jcr:primaryType", "nt:unstructured");
        replication.setProperty("GET_journal_log", "Journal file for segment sync");
        replication.setProperty("GET_manifest", "Segment manifest");
        replication.setProperty("GET_segments_segmentId", "Fetch specific segment by id");
    }

    private void populateExamples(NodeBuilder genesis) {
        NodeBuilder examples = genesis.child("examples");
        examples.setProperty("jcr:primaryType", "nt:unstructured");
        examples.setProperty("jcr:title", "Working Examples");
        examples.setProperty("jcr:description", "Copy-paste discovery flow for operators, explorers, and future CLI tooling.");

        NodeBuilder curlDiscovery = examples.child("curl-discovery");
        curlDiscovery.setProperty("jcr:primaryType", "nt:unstructured");
        curlDiscovery.setProperty("title", "Validator Discovery");
        curlDiscovery.setProperty("health", "curl http://localhost:8090/health");
        curlDiscovery.setProperty("manifest", "curl http://localhost:8090/v1/index | jq .");
        curlDiscovery.setProperty("consensus", "curl http://localhost:8090/v1/consensus/status | jq .");

        NodeBuilder curlGenesis = examples.child("curl-genesis");
        curlGenesis.setProperty("jcr:primaryType", "nt:unstructured");
        curlGenesis.setProperty("title", "Read Genesis Through Explorer");
        curlGenesis.setProperty("discover-nav", "curl http://localhost:8090/v1/explorer/content/nav | jq .");
        curlGenesis.setProperty("read-node",
            "curl --get http://localhost:8090/v1/explorer/content/clusters/<clusterId>/node "
                + "--data-urlencode 'path=" + CANONICAL_GENESIS_PATH + "' | jq .");
        curlGenesis.setProperty("read-provenance",
            "curl --get http://localhost:8090/v1/explorer/content/clusters/<clusterId>/provenance "
                + "--data-urlencode 'path=" + CANONICAL_GENESIS_PATH + "' | jq .");

        NodeBuilder curlWallet = examples.child("curl-wallet");
        curlWallet.setProperty("jcr:primaryType", "nt:unstructured");
        curlWallet.setProperty("title", "Inspect Wallet State");
        curlWallet.setProperty("wallet-stats",
            "curl --get http://localhost:8090/v1/wallets/stats --data-urlencode 'wallet=0xYOUR_WALLET' | jq .");
        curlWallet.setProperty("wallet-content",
            "curl --get http://localhost:8090/v1/wallets/content --data-urlencode 'wallet=0xYOUR_WALLET' | jq .");

        NodeBuilder readOrder = examples.child("read-order");
        readOrder.setProperty("jcr:primaryType", "nt:unstructured");
        readOrder.setProperty("step-1", "protocol");
        readOrder.setProperty("step-2", "content-contract");
        readOrder.setProperty("step-3", "architecture");
        readOrder.setProperty("step-4", "api");
        readOrder.setProperty("step-5", "getting-started");
    }

    private void populateArchitecture(NodeBuilder genesis) {
        NodeBuilder architecture = genesis.child("architecture");
        architecture.setProperty("jcr:primaryType", "nt:unstructured");
        architecture.setProperty("jcr:title", "System Architecture");
        architecture.setProperty("jcr:description", "How OakChain composes storage, consensus, and contract surfaces.");

        NodeBuilder components = architecture.child("components");
        components.setProperty("jcr:primaryType", "nt:unstructured");
        components.setProperty("oak-segment-store", "Apache Oak TarMK - proven content storage from Adobe AEM");
        components.setProperty("aeron-cluster", "High-performance Raft consensus (io.aeron.cluster)");
        components.setProperty("ethereum", "Settlement, authorization, and external proof surfaces");
        components.setProperty("ipfs", "Content-addressed binary storage");
        components.setProperty("http-api", "Validator-native HTTP contracts");

        NodeBuilder paths = architecture.child("wallet-scoped-paths");
        paths.setProperty("jcr:primaryType", "nt:unstructured");
        paths.setProperty("pattern", "/oak-chain/{shard-level-1}/{shard-level-2}/{shard-level-3}/{wallet}/content/{path}");
        paths.setProperty("example", "/oak-chain/74/2d/35/0x742d35Cc6634C0532925a3b844Bc9e7595f1b3E8/content/my-page");
        paths.setProperty("genesis", CANONICAL_GENESIS_PATH);
        paths.setProperty("sharding", "First 3 bytes of wallet address create the 3-level directory structure");
        paths.setProperty("contract", "The wallet namespace is enforced; subtree structure below /content is intentionally flexible.");

        NodeBuilder consensus = architecture.child("consensus");
        consensus.setProperty("jcr:primaryType", "nt:unstructured");
        consensus.setProperty("algorithm", "Raft (via Aeron Cluster)");
        consensus.setProperty("quorum", "Majority of validators must agree");
        consensus.setProperty("leader-election", "Automatic - leader handles write origination");
        consensus.setProperty("replication", "All accepted writes replicate across validators");
        consensus.setProperty("finality", "Immediate local finality with adaptive verified-release scheduling; Ethereum remains a settlement and compatibility surface.");

        NodeBuilder bootstrapAuthority = architecture.child("bootstrap-authority");
        bootstrapAuthority.setProperty("jcr:primaryType", "nt:unstructured");
        bootstrapAuthority.setProperty("genesis-node", CANONICAL_GENESIS_PATH);
        bootstrapAuthority.setProperty("route-manifest", "/v1/index");
        bootstrapAuthority.setProperty("explorer-entry", "/v1/explorer/content/nav");
    }

    private void populateEconomics(NodeBuilder genesis) {
        NodeBuilder economics = genesis.child("economics");
        economics.setProperty("jcr:primaryType", "nt:unstructured");
        economics.setProperty("jcr:title", "Settlement and Resource Model");
        economics.setProperty("jcr:description", "What OakChain currently enforces around authorization, settlement, and resource accountability.");
        economics.setProperty("philosophy", "The validator should describe enforced economics, not speculative product packaging.");

        NodeBuilder model = economics.child("settlement-model");
        model.setProperty("jcr:primaryType", "nt:unstructured");
        model.setProperty("authority", "The contract/payment flow authorizes writes and anchors settlement facts.");
        model.setProperty("runtime", "The validator verifies chain-backed proposal intent, stages work, and applies deterministic consensus.");
        model.setProperty("positioning", "Oak Segment Consensus publishes runtime economics as settlement and accountability facts, not commercial packaging.");
        model.setProperty("current-truth", "Treat economics here as protocol/accountability guidance, not SKU language.");

        NodeBuilder resourceSignals = economics.child("resource-signals");
        resourceSignals.setProperty("jcr:primaryType", "nt:unstructured");
        resourceSignals.setProperty("fragmentation", "Fragmentation and batching still matter operationally because they shape storage pressure and release behavior.");
        resourceSignals.setProperty("queue", "Queue depth, backpressure, and release flow are the observable runtime signals.");
        resourceSignals.setProperty("routes", "/v1/proposals/queue/stats, /v1/proposals/release-flow, /v1/fragmentation/*");
        resourceSignals.setProperty("note", "Use metrics and settlement evidence to reason about cost, not marketing tier names.");

        NodeBuilder paymentFlow = economics.child("payment-flow");
        paymentFlow.setProperty("jcr:primaryType", "nt:unstructured");
        paymentFlow.setProperty("step-1", "Client obtains or derives a chain-backed proposalId.");
        paymentFlow.setProperty("step-2", "Validator verifies signature and stages the proposal.");
        paymentFlow.setProperty("step-3", "Consensus and release governance determine when work is applied.");
        paymentFlow.setProperty("step-4", "Settlement and authorization facts remain auditable.");
        paymentFlow.setProperty("step-5", "Committed content becomes durable chain state.");
    }

    private void populateTroubleshooting(NodeBuilder genesis) {
        NodeBuilder troubleshooting = genesis.child("troubleshooting");
        troubleshooting.setProperty("jcr:primaryType", "nt:unstructured");
        troubleshooting.setProperty("jcr:title", "Troubleshooting Guide");
        troubleshooting.setProperty("jcr:description", "Common issues and how to close the loop quickly");

        NodeBuilder issues = troubleshooting.child("common-issues");
        issues.setProperty("jcr:primaryType", "nt:unstructured");
        issues.setProperty("issue-signature-invalid", "SIGNATURE_INVALID: Ensure the wallet signature matches the exact payload submitted to the validator.");
        issues.setProperty("issue-not-leader", "NOT_LEADER: Resolve the leader via /v1/consensus/leader and retry against that validator.");
        issues.setProperty("issue-path-forbidden", "PATH_FORBIDDEN: Operate only inside your wallet namespace.");
        issues.setProperty("issue-epoch-stale", "EPOCH_STALE: Refresh your local state and resubmit with current chain facts.");
        issues.setProperty("issue-cluster-id", "Unknown clusterId: refresh /v1/explorer/content/nav before using tree, node, or provenance endpoints.");

        NodeBuilder healthChecks = troubleshooting.child("health-checks");
        healthChecks.setProperty("jcr:primaryType", "nt:unstructured");
        healthChecks.setProperty("check-1", "curl /health - should return a healthy local response");
        healthChecks.setProperty("check-2", "curl /v1/consensus/status - verify cluster role and health");
        healthChecks.setProperty("check-3", "curl /v1/consensus/leader - verify canonical leader resolution");
        healthChecks.setProperty("check-4", "curl /v1/index - verify expected contracts are present");
    }

    private void populateAbout(NodeBuilder genesis) {
        NodeBuilder about = genesis.child("about");
        about.setProperty("jcr:primaryType", "nt:unstructured");
        about.setProperty("jcr:title", "About OakChain");
        about.setProperty("mission", "Decentralizing enterprise content management");
        about.setProperty("foundation", "Built on Apache Oak - the proven content repository behind Adobe AEM");
        about.setProperty("value-proposition", "Billions of dollars of enterprise content already runs on Oak. We're adding decentralization, cryptographic ownership, and blockchain finality.");
        about.setProperty("philosophy", "Bitcoin-tight reliability meets enterprise content management");
        about.setProperty("principles", "Fail Loud, Fail Fast, Never Silently Corrupt");
        about.setProperty("team", "somarc + AI collaborators - distributed intelligence building distributed systems");
        about.setProperty("license", "Apache 2.0");

        NodeBuilder thesis = genesis.child("thesis");
        thesis.setProperty("jcr:primaryType", "nt:unstructured");
        thesis.setProperty("jcr:title", "The Thesis");
        thesis.setProperty("premise-1", "Enterprise content is the most valuable data no one can prove.");
        thesis.setProperty("premise-2", "Audit trails should be data, not policy.");
        thesis.setProperty("premise-3", "Determinism is the only safe way to scale trust.");
        thesis.setProperty("result", "OakChain makes content provable, portable, and economically secure.");
        thesis.setProperty("audience", "Builders who want boring reliability and bold guarantees.");

        NodeBuilder boldBets = genesis.child("bold-bets");
        boldBets.setProperty("jcr:primaryType", "nt:unstructured");
        boldBets.setProperty("bet-1", "Every serious enterprise will demand verifiable content history.");
        boldBets.setProperty("bet-2", "AEM-scale systems can be decentralized without losing performance.");
        boldBets.setProperty("bet-3", "Proof of custody will become the default compliance standard.");
        boldBets.setProperty("bet-4", "Developers will choose APIs over platforms if guarantees are stronger.");
        boldBets.setProperty("bet-5", "Developers will choose systems with stronger guarantees over familiar platforms.");

        NodeBuilder guarantees = genesis.child("guarantees");
        guarantees.setProperty("jcr:primaryType", "nt:unstructured");
        guarantees.setProperty("determinism", "Same inputs, same state on every validator.");
        guarantees.setProperty("auditability", "Every change is traceable by ID, time, and signer.");
        guarantees.setProperty("durability", "Committed content survives validator loss.");
        guarantees.setProperty("portability", "Content is readable without privileged infrastructure.");
        guarantees.setProperty("integrity", "Signatures bind authorship to the data path.");

        NodeBuilder nonGoals = genesis.child("non-goals");
        nonGoals.setProperty("jcr:primaryType", "nt:unstructured");
        nonGoals.setProperty("non-goal-1", "We do not chase maximal throughput at the expense of determinism.");
        nonGoals.setProperty("non-goal-2", "We do not require custodial identity or closed networks.");
        nonGoals.setProperty("non-goal-3", "We do not hide failures; we surface them early and loudly.");

        NodeBuilder oath = genesis.child("operator-oath");
        oath.setProperty("jcr:primaryType", "nt:unstructured");
        oath.setProperty("oath-1", "Run the node as if the audit depends on you.");
        oath.setProperty("oath-2", "Do not change history. Fix the system.");
        oath.setProperty("oath-3", "Measure everything; guess nothing.");
        oath.setProperty("oath-4", "If it fails, document the failure in the chain.");
    }

    private String populateGenesisImage(NodeBuilder genesis, long timestamp) throws Exception {
        NodeBuilder genesisImage = genesis.child("do-it-live.jpeg");
        genesisImage.setProperty("jcr:primaryType", "nt:file");
        genesisImage.setProperty("jcr:created", timestamp);

        NodeBuilder imageContent = genesisImage.child("jcr:content");
        imageContent.setProperty("jcr:primaryType", "nt:resource");
        imageContent.setProperty("jcr:mimeType", "image/jpeg");
        imageContent.setProperty("jcr:lastModified", timestamp);

        byte[] imageBytes = loadGenesisImageBytes();
        if (imageBytes == null) {
            storePlaceholderImage(imageContent, GENESIS_MESSAGE + " (image placeholder)");
            return null;
        }

        imageContent.setProperty("size", (long) imageBytes.length);
        if (blobStore != null) {
            String blobId = blobStore.writeBlob(new ByteArrayInputStream(imageBytes));
            imageContent.setProperty("jcr:data", new BlobStoreBlob(blobStore, blobId));
            imageContent.setProperty("jcr:blobId", blobId);
            if (blobId != null && (blobId.startsWith("Qm") || blobId.startsWith("baf"))) {
                return blobId.split("#")[0];
            }
            return blobId;
        }

        Blob blob = nodeStore.createBlob(new ByteArrayInputStream(imageBytes));
        imageContent.setProperty("jcr:data", blob);
        return null;
    }

    private void storePlaceholderImage(NodeBuilder imageContent, String placeholder) throws Exception {
        byte[] placeholderBytes = placeholder.getBytes(StandardCharsets.UTF_8);
        imageContent.setProperty("jcr:mimeType", "text/plain");
        imageContent.setProperty("size", (long) placeholderBytes.length);
        imageContent.setProperty("jcr:data", nodeStore.createBlob(new ByteArrayInputStream(placeholderBytes)));
    }

    private byte[] loadGenesisImageBytes() throws Exception {
        try (InputStream imageStream = getClass().getClassLoader().getResourceAsStream(GENESIS_IMAGE_RESOURCE)) {
            if (imageStream == null) {
                return null;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int read;
            while ((read = imageStream.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }
            return baos.toByteArray();
        }
    }

    private static void verifyString(NodeState node, String propertyName, String expected) {
        if (!node.hasProperty(propertyName)) {
            throw new IllegalStateException("Canonical genesis is missing property " + propertyName);
        }
        String actual = node.getProperty(propertyName).getValue(Type.STRING);
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Canonical genesis property " + propertyName + " expected "
                + expected + " but found " + actual);
        }
    }

    private static String normalizeGenesisValidatorUrl(String genesisValidatorUrl) {
        if (genesisValidatorUrl == null || genesisValidatorUrl.trim().isEmpty()) {
            return DEFAULT_GENESIS_VALIDATOR_URL;
        }
        return genesisValidatorUrl.trim();
    }

    private static String extractHost(String genesisValidatorUrl) {
        try {
            URI uri = URI.create(genesisValidatorUrl);
            if (uri.getHost() != null && !uri.getHost().isEmpty()) {
                return uri.getHost();
            }
        } catch (IllegalArgumentException ignored) {
            // fall through to simple parsing below
        }

        String sanitized = genesisValidatorUrl.replace("http://", "").replace("https://", "");
        int slashIndex = sanitized.indexOf('/');
        if (slashIndex >= 0) {
            sanitized = sanitized.substring(0, slashIndex);
        }
        int colonIndex = sanitized.indexOf(':');
        return colonIndex >= 0 ? sanitized.substring(0, colonIndex) : sanitized;
    }
}
