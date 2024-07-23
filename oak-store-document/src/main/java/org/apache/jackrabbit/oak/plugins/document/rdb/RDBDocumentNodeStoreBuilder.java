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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static java.util.Set.of;
import static org.apache.jackrabbit.guava.common.base.Suppliers.memoize;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * A builder for a {@link DocumentNodeStore} backed by a relational database.
 */
public class RDBDocumentNodeStoreBuilder
        extends DocumentNodeStoreBuilder<RDBDocumentNodeStoreBuilder> {

    private static final Logger log = LoggerFactory.getLogger(RDBDocumentNodeStoreBuilder.class);

    /**
     * @return a new {@link RDBDocumentNodeStoreBuilder}.
     */
    public static RDBDocumentNodeStoreBuilder newRDBDocumentNodeStoreBuilder() {
        return new RDBDocumentNodeStoreBuilder();
    }

    /**
     * Sets a {@link DataSource} to use for the RDB document and blob
     * stores.
     *
     * @return this
     */
    public RDBDocumentNodeStoreBuilder setRDBConnection(DataSource ds) {
        setRDBConnection(ds, new RDBOptions());
        return thisBuilder();
    }

    /**
     * Sets a {@link DataSource} to use for the RDB document and blob
     * stores, including {@link RDBOptions}.
     *
     * @return this
     */
    public RDBDocumentNodeStoreBuilder setRDBConnection(DataSource ds, RDBOptions options) {
        setRDBConnection(ds, ds, options);
        return thisBuilder();
    }

    /**
     * Sets a {@link DataSource}s to use for the RDB document and blob
     * stores.
     *
     * @return this
     */
    public RDBDocumentNodeStoreBuilder setRDBConnection(DataSource documentStoreDataSource, DataSource blobStoreDataSource) {
        setRDBConnection(documentStoreDataSource, blobStoreDataSource, new RDBOptions());
        return thisBuilder();
    }

    /**
     * Sets a {@link DataSource}s to use for the RDB document and blob
     * stores, including {@link RDBOptions}.
     *
     * @return this
     */
    public RDBDocumentNodeStoreBuilder setRDBConnection(DataSource documentStoreDataSource, DataSource blobStoreDataSource, RDBOptions options) {
        this.documentStoreSupplier = memoize(() -> new RDBDocumentStore(documentStoreDataSource, this, options));
        if (this.blobStoreSupplier == null) {
            this.blobStoreSupplier = memoize(() -> new RDBBlobStore(blobStoreDataSource, this, options));
        }
        return thisBuilder();
    }

    public VersionGCSupport createVersionGCSupport() {
        DocumentStore store = getDocumentStore();
        if (store instanceof RDBDocumentStore) {
            return new RDBVersionGCSupport((RDBDocumentStore) store);
        } else {
            return super.createVersionGCSupport();
        }
    }

    public Iterable<ReferencedBlob> createReferencedBlobs(DocumentNodeStore ns) {
        final DocumentStore store = getDocumentStore();
        if (store instanceof RDBDocumentStore) {
            return () -> new RDBBlobReferenceIterator(ns, (RDBDocumentStore) store);
        } else {
            return super.createReferencedBlobs(ns);
        }
    }

    public MissingLastRevSeeker createMissingLastRevSeeker() {
        final DocumentStore store = getDocumentStore();
        if (store instanceof RDBDocumentStore) {
            return new RDBMissingLastRevSeeker((RDBDocumentStore) store, getClock());
        } else {
            return super.createMissingLastRevSeeker();
        }
    }

    @Override
    public boolean isFullGCEnabled() {
        // fullGC is non supported for RDB
        return false;
    }

    @Override
    public RDBDocumentNodeStoreBuilder setFullGCEnabled(boolean b) {
        // fullGC is non supported for RDB
        return thisBuilder();
    }

    @Override
    public Set<String> getFullGCIncludePaths() {
        return of();
    }

    @Override
    public RDBDocumentNodeStoreBuilder setFullGCIncludePaths(@NotNull String[] includePaths) {
        // fullGC is not supported for RDB
        return thisBuilder();
    }

    @Override
    public Set<String> getFullGCExcludePaths() {
        return of();
    }

    @Override
    public RDBDocumentNodeStoreBuilder setFullGCExcludePaths(@NotNull String[] excludePaths) {
        // fullGC is not supported for RDB
        return thisBuilder();
    }

    @Override
    public RDBDocumentNodeStoreBuilder setFullGCMode(int v) {
        // fullGC modes are not supported for RDB
        log.warn("FullGC modes are not supported for RDB");
        return thisBuilder();
    }
  
    @Override
    public int getFullGCMode() {
        // fullGC modes are not supported for RDB
        return 0;
    }
  
    @Override
    public RDBDocumentNodeStoreBuilder setDocStoreFullGCFeature(@Nullable Feature docStoreFullGC) {
        return thisBuilder();
    }

    @Override
    @Nullable
    public Feature getDocStoreFullGCFeature() {
        return null;
    }

    @Override
    public boolean isEmbeddedVerificationEnabled() {
        return false;
    }

    @Override
    public RDBDocumentNodeStoreBuilder setEmbeddedVerificationEnabled(boolean b) {
        // embeddedVerification is non supported for RDB since fullGC is not.
        return thisBuilder();
    }

    @Override
    public RDBDocumentNodeStoreBuilder setDocStoreEmbeddedVerificationFeature(@Nullable Feature getDocStoreEmbeddedVerification) {
        return thisBuilder();
    }

    @Override
    @Nullable
    public Feature getDocStoreEmbeddedVerificationFeature() {
        return null;
    }
}
