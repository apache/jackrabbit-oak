/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.index.indexer.document;

import com.google.common.collect.FluentIterable;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Path;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;

public class NodeStateEntryTraverser implements Iterable<NodeStateEntry>, Closeable {
    private final Closer closer = Closer.create();
    private final RevisionVector rootRevision;
    private final DocumentNodeStore documentNodeStore;
    private final MongoDocumentStore documentStore;
    /**
     * Traverse only those node states which have been modified on or after lower limit
     * and before the upper limit of this range.
     */
    private final TraversingRange traversingRange;

    private Consumer<String> progressReporter = id -> {};

    private final String id;

    public NodeStateEntryTraverser(String id, DocumentNodeStore documentNodeStore,
                                   MongoDocumentStore documentStore) {
        this(id, documentNodeStore.getHeadRevision(), documentNodeStore, documentStore,
                new TraversingRange(new LastModifiedRange(0, Long.MAX_VALUE), null));
    }

    public NodeStateEntryTraverser(String id, RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
                                   MongoDocumentStore documentStore, TraversingRange traversingRange) {
        this.id = id;
        this.rootRevision = rootRevision;
        this.documentNodeStore = documentNodeStore;
        this.documentStore = documentStore;
        this.traversingRange = traversingRange;
    }

    public String getId() {
        return id;
    }

    @NotNull
    @Override
    public Iterator<NodeStateEntry> iterator() {
        return getIncludedDocs().iterator();
    }

    public NodeStateEntryTraverser withProgressCallback(Consumer<String> progressReporter) {
        this.progressReporter = progressReporter;
        return this;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @SuppressWarnings("Guava")
    private Iterable<NodeStateEntry> getIncludedDocs() {
        return FluentIterable.from(getDocsFilteredByPath())
                .filter(doc -> includeDoc(doc))
                .transformAndConcat(doc -> getEntries(doc));
    }

    private boolean includeDoc(NodeDocument doc) {
        return !doc.isSplitDocument();
    }

    /**
     * Returns the modification range corresponding to node states which are traversed by this.
     * @return {@link LastModifiedRange}
     */
    public TraversingRange getDocumentTraversalRange() {
        return traversingRange;
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private Iterable<NodeStateEntry> getEntries(NodeDocument doc) {
        Path path = doc.getPath();

        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);

        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return emptyList();
        }

        return transform(
                concat(singleton(nodeState),
                    nodeState.getAllBundledNodesStates()),
                dns -> {
                    NodeStateEntry.NodeStateEntryBuilder builder =  new NodeStateEntry.NodeStateEntryBuilder(dns, dns.getPath().toString());
                    if (doc.getModified() != null) {
                        builder.withLastModified(doc.getModified());
                    }
                    builder.withID(doc.getId());
                    return builder.build();
                }
        );
    }

    private Iterable<NodeDocument> getDocsFilteredByPath() {
        CloseableIterable<NodeDocument> docs = findAllDocuments();
        closer.register(docs);
        return docs;
    }

    private CloseableIterable<NodeDocument> findAllDocuments() {
        return new MongoDocumentTraverser(documentStore)
                .getAllDocuments(Collection.NODES, traversingRange, this::reportProgress);
    }

    private boolean reportProgress(String id) {
        progressReporter.accept(id);
        // always returning true here and do the path predicate and hidden node filter when iterating.
        return true;
    }
}
