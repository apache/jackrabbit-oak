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

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.blob.cloud.ipfs.IPFSDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.consensus.evm.EvmBridge;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.FragmentationTracker;
import org.apache.jackrabbit.oak.segment.consensus.fragmentation.WalletStorageMetrics;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCAccountManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCCostEstimator;
import org.apache.jackrabbit.oak.segment.consensus.gc.GCProposalManager;
import org.apache.jackrabbit.oak.segment.consensus.gc.PeriodicGCJob;
import org.apache.jackrabbit.oak.segment.consensus.security.EthereumWallet;
import org.apache.jackrabbit.oak.segment.consensus.bootstrap.ValidatorBootstrap;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.http.server.SegmentHttpServer;
import org.apache.jackrabbit.oak.segment.http.server.binary.CidMappingService;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;

@Component(service = GlobalStoreServerComponentFactory.class, immediate = true)
public final class DefaultGlobalStoreServerComponentFactory implements GlobalStoreServerComponentFactory {

    public static final DefaultGlobalStoreServerComponentFactory INSTANCE = new DefaultGlobalStoreServerComponentFactory();

    private DefaultGlobalStoreServerComponentFactory() {
        // singleton
    }

    @Override
    public EthereumWallet createEthereumWallet(String keystorePath) throws Exception {
        return new EthereumWallet(keystorePath);
    }

    @Override
    public org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterService createAeronClusterService() {
        return new org.apache.jackrabbit.oak.segment.consensus.aeron.AeronClusterService();
    }

    @Override
    public BlobStore createIpfsBlobStore(String ipfsEndpoint, java.io.File storeDir) throws Exception {
        IPFSDataStore ipfsDataStore = new IPFSDataStore();
        ipfsDataStore.setIpfsApiEndpoint(ipfsEndpoint);
        ipfsDataStore.setMinRecordLength(16 * 1024);
        ipfsDataStore.init(storeDir.getAbsolutePath());
        return new DataStoreBlobStore(ipfsDataStore);
    }

    @Override
    public GCCostEstimator createGCCostEstimator(FileStore fileStore, TarFiles tarFiles, BigDecimal usdcPerMB) {
        return new GCCostEstimator(fileStore, tarFiles, usdcPerMB);
    }

    @Override
    public SegmentHttpServer createHttpServer(java.io.File storeDir, int port, FileStore fileStore, NodeStore nodeStore) {
        return new SegmentHttpServer(storeDir, port, fileStore, nodeStore);
    }

    @Override
    public CidMappingService createCidMappingService(Path storeDir) throws Exception {
        return new CidMappingService(storeDir);
    }

    @Override
    public FragmentationTracker createFragmentationTracker() {
        return new FragmentationTracker();
    }

    @Override
    public WalletStorageMetrics createWalletStorageMetrics(FileStore fileStore) {
        return new WalletStorageMetrics(fileStore);
    }

    @Override
    public GCProposalManager createGCProposalManager(FileStore fileStore,
                                                     GCCostEstimator gcCostEstimator,
                                                     FragmentationTracker fragmentationTracker,
                                                     EvmBridge evmBridge,
                                                     int totalValidators,
                                                     Supplier<Integer> executorIdSupplier,
                                                     Supplier<Boolean> isLeaderSupplier) {
        return new GCProposalManager(fileStore, gcCostEstimator, fragmentationTracker, evmBridge,
            totalValidators, executorIdSupplier, isLeaderSupplier);
    }

    @Override
    public GCAccountManager createGCAccountManager() {
        return new GCAccountManager();
    }

    @Override
    public PeriodicGCJob createPeriodicGCJob(GCAccountManager accountManager) {
        return new PeriodicGCJob(accountManager);
    }

    @Override
    public ValidatorBootstrap createValidatorBootstrap(FileStore fileStore, int standbyPort) {
        return new ValidatorBootstrap(fileStore, standbyPort);
    }

    @Override
    public GenesisInitializer createGenesisInitializer(NodeStore nodeStore, FileStore fileStore, BlobStore blobStore, String selfUrl) {
        return new GenesisInitializer(nodeStore, fileStore, blobStore);
    }

    @Override
    public ConsensusServicesInitializer createConsensusServicesInitializer() {
        return new ConsensusServicesInitializer();
    }
}
