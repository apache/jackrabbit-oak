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
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.commons.pio.Closer;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.RepositorySidegrade;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;
import org.apache.jackrabbit.oak.upgrade.cli.parser.DatastoreArguments;
import org.apache.jackrabbit.oak.upgrade.cli.parser.MigrationOptions;
import org.apache.jackrabbit.oak.upgrade.cli.parser.StoreArguments;

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
        BlobStore srcBlobStore = new ToJackrabbitDataStoreDelegatingBlobStore(src.getDataStore());
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
        upgrade.setCustomCommitHooks(loadCommitHooks());
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
        sidegrade.setCustomCommitHooks(loadCommitHooks());
        return sidegrade;
    }

    private List<CommitHook> loadCommitHooks() {
        ServiceLoader<CommitHook> loader = ServiceLoader.load(CommitHook.class);
        return Collections.unmodifiableList(ListUtils.toList(loader.iterator()));
    }

    /**
     * Wraps An Oak BlobStore around a Jackrabbit Datastore
     */
    private static class ToJackrabbitDataStoreDelegatingBlobStore implements BlobStore {

        private org.apache.jackrabbit.core.data.DataStore delegate;

        public ToJackrabbitDataStoreDelegatingBlobStore(
                org.apache.jackrabbit.core.data.DataStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public String writeBlob(InputStream inputStream) throws IOException {
            try {
                org.apache.jackrabbit.core.data.DataRecord record = delegate.addRecord(inputStream);
                return record.getIdentifier().toString();
            } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
                throw new IOException("Failed to write blob", ex);
            }
        }

        @Override
        public String writeBlob(InputStream inputStream, BlobOptions options) throws IOException {
            try {
                org.apache.jackrabbit.core.data.DataRecord record = delegate.addRecord(inputStream);
                return record.getIdentifier().toString();
            } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
                throw new IOException("Failed to write blob", ex);
            }
        }

        @Override
        public int readBlob(String blobId, long pos, byte[] buff, int off, int length)
                throws IOException {

            try (InputStream is = getInputStream(blobId)) {

                if (pos > 0) {
                    long skipped = is.skip(pos);
                    if (skipped < pos) {
                        return -1;
                    }
                }

                return is.read(buff, off, length);
            }
        }

        @Override
        public long getBlobLength(String blobId) throws IOException {
            try {
                org.apache.jackrabbit.core.data.DataRecord record = delegate.getRecord(new org.apache.jackrabbit.core.data.DataIdentifier(blobId));
                return record.getLength();
            } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
                throw new IOException("Failed to get blob length", ex);
            }
        }

        @Override
        public InputStream getInputStream(String blobId) throws IOException {
            try {
                org.apache.jackrabbit.core.data.DataRecord record = delegate.getRecord(new org.apache.jackrabbit.core.data.DataIdentifier(blobId));
                return record.getStream();
            } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
                throw new IOException("Failed to get input stream", ex);
            }
        }

        @Override
        public String getBlobId(String reference) {
            // Usually same as blobId for Jackrabbit datastore
            return reference;
        }

        @Override
        public String getReference(String blobId) {
            // Jackrabbit DataStore doesn't distinguish strongly here
            return blobId;
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

}
