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

import static com.google.common.base.Suppliers.memoize;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;

/**
 * A builder for a {@link DocumentNodeStore} backed by a relational database.
 */
public class RDBDocumentNodeStoreBuilder
        extends DocumentNodeStoreBuilder<RDBDocumentNodeStoreBuilder> {

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
}
