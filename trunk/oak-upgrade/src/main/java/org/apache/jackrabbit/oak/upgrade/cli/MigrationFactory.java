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
package org.apache.jackrabbit.oak.upgrade.cli;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositorySidegrade;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;

public class MigrationFactory {

    protected final MigrationOptions options;

    protected final StoreArguments stores;

    protected final DatastoreArguments datastores;

    protected final Closer closer;

    public MigrationFactory(MigrationOptions options, StoreArguments stores, DatastoreArguments datastores, Closer closer) {
        this.options = options;
        this.stores = stores;
        this.datastores = datastores;
        this.closer = closer;
    }

    public RepositoryUpgrade createUpgrade() throws IOException, RepositoryException, CliArgumentException {
        RepositoryContext src = stores.getSrcStore().create(closer);
        BlobStore srcBlobStore = new DataStoreBlobStore(src.getDataStore());
        NodeStore dstStore = createTarget(closer, srcBlobStore);
        return createUpgrade(src, dstStore);
    }

    public RepositorySidegrade createSidegrade() throws IOException, CliArgumentException {
        BlobStore srcBlobStore = datastores.getSrcBlobStore().create(closer);
        NodeStore srcStore = stores.getSrcStore().create(srcBlobStore, closer);
        NodeStore dstStore = createTarget(closer, srcBlobStore);
        return createSidegrade(srcStore, dstStore);
    }

    protected NodeStore createTarget(Closer closer, BlobStore srcBlobStore) throws IOException {
        BlobStore dstBlobStore = datastores.getDstBlobStore(srcBlobStore).create(closer);
        NodeStore dstStore = stores.getDstStore().create(dstBlobStore, closer);
        return dstStore;
    }

    protected RepositoryUpgrade createUpgrade(RepositoryContext source, NodeStore dstStore) {
        RepositoryUpgrade upgrade = new RepositoryUpgrade(source, dstStore);
        upgrade.setCopyBinariesByReference(datastores.getBlobMigrationCase() == DatastoreArguments.BlobMigrationCase.COPY_REFERENCES);
        upgrade.setCopyVersions(options.getCopyVersions());
        upgrade.setCopyOrphanedVersions(options.getCopyOrphanedVersions());
        if (options.getIncludePaths() != null) {
            upgrade.setIncludes(options.getIncludePaths());
        }
        if (options.getExcludePaths() != null) {
            upgrade.setExcludes(options.getExcludePaths());
        }
        if (options.getMergePaths() != null) {
            upgrade.setMerges(options.getMergePaths());
        }
        upgrade.setFilterLongNames(!stores.getDstType().isSupportLongNames());
        upgrade.setCheckLongNames(!options.isSkipNameCheck() && !stores.getDstType().isSupportLongNames());
        upgrade.setSkipOnError(!options.isFailOnError());
        upgrade.setEarlyShutdown(options.isEarlyShutdown());
        upgrade.setSkipInitialization(options.isSkipInitialization());
        upgrade.setCustomCommitHooks(loacCommitHooks());
        return upgrade;
    }

    private RepositorySidegrade createSidegrade(NodeStore srcStore, NodeStore dstStore) {
        RepositorySidegrade sidegrade = new RepositorySidegrade(srcStore, dstStore);
        sidegrade.setCopyVersions(options.getCopyVersions());
        sidegrade.setCopyOrphanedVersions(options.getCopyOrphanedVersions());
        if (options.getIncludePaths() != null) {
            sidegrade.setIncludes(options.getIncludePaths());
        }
        if (options.getExcludePaths() != null) {
            sidegrade.setExcludes(options.getExcludePaths());
        }
        if (options.getMergePaths() != null) {
            sidegrade.setMerges(options.getMergePaths());
        }
        sidegrade.setFilterLongNames(stores.getSrcType().isSupportLongNames() && !stores.getDstType().isSupportLongNames());
        sidegrade.setVerify(options.isVerify());
        sidegrade.setOnlyVerify(options.isOnlyVerify());
        sidegrade.setSkipCheckpoints(options.isSkipCheckpoints());
        sidegrade.setForceCheckpoints(options.isForceCheckpoints());
        sidegrade.setMigrateDocumentMetadata(options.isAddSecondaryMetadata());
        sidegrade.setCustomCommitHooks(loacCommitHooks());
        return sidegrade;
    }

    private List<CommitHook> loacCommitHooks() {
        ServiceLoader<CommitHook> loader = ServiceLoader.load(CommitHook.class);
        Iterator<CommitHook> iterator = loader.iterator();
        ImmutableList.Builder<CommitHook> builder = ImmutableList.<CommitHook> builder().addAll(iterator);
        return builder.build();
    }

}
