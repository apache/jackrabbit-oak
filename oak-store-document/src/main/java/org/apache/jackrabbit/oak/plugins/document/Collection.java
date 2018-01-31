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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

/**
 * The collection types.
 *
 * @param <T> the document type
 */
public abstract class Collection<T extends Document> {

    /**
     * The 'nodes' collection. It contains all the node data, with one document
     * per node, and the path as the primary key. Each document possibly
     * contains multiple revisions.
     * <p>
     * Key: the path, value: the node data (possibly multiple revisions)
     * <p>
     * Old revisions are removed after some time, either by the process that
     * removed or updated the node, lazily when reading, or in a background
     * process.
     */
    public static final Collection<NodeDocument> NODES =
            new Collection<NodeDocument>("nodes") {
                @Override
                @Nonnull
                public NodeDocument newDocument(DocumentStore store) {
                    return new NodeDocument(store);
                }
            };

    /**
     * The 'clusterNodes' collection contains the list of currently running
     * cluster nodes. The key is the clusterNodeId (0, 1, 2,...).
     */
    public static final Collection<ClusterNodeInfoDocument> CLUSTER_NODES =
            new Collection<ClusterNodeInfoDocument>("clusterNodes") {
                @Override
                @Nonnull
                public ClusterNodeInfoDocument newDocument(DocumentStore store) {
                    return new ClusterNodeInfoDocument();
                }
            };

    /**
     * The 'settings' collection contains setting/state data required for DocumentNodeStore
     */
    public static final Collection<Document> SETTINGS =
            new Collection<Document>("settings") {
                @Override
                @Nonnull
                public Document newDocument(DocumentStore store) {
                    return new Document();
                }
            };

    /**
     * The 'journal' collection contains documents with consolidated
     * diffs for changes performed by a cluster node between two background
     * updates.
     */
    public static final Collection<JournalEntry> JOURNAL =
            new Collection<JournalEntry>("journal") {
        @Nonnull
        @Override
        public JournalEntry newDocument(DocumentStore store) {
            return new JournalEntry(store);
        }
    };

    /**
     * The 'blobs' collection contains data from the blob store. The method
     * {@link #newDocument(DocumentStore)} always throws an
     * {@link UnsupportedOperationException} because blobs are not stored as
     * {@link Document}s.
     */
    public static final Collection<Document> BLOBS =
            new Collection<Document>("blobs") {
        @Nonnull
        @Override
        public Document newDocument(DocumentStore store) {
            throw new UnsupportedOperationException();
        }
    };


    private final String name;

    public Collection(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * @param store the document store.
     * @return a new document for this collection.
     */
    @Nonnull
    public abstract T newDocument(DocumentStore store);
}
