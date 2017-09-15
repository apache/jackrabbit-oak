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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.FluentIterable;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.NotNull;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public class NodeStateEntryTraverser implements Iterable<NodeStateEntry>, Closeable {
    private final Closer closer = Closer.create();
    private final RevisionVector rootRevision;
    private final DocumentNodeStore documentNodeStore;
    private final MongoDocumentStore documentStore;

    private Consumer<String> progressReporter = id -> {};
    private Predicate<String> pathPredicate = path -> true;

    public NodeStateEntryTraverser(DocumentNodeStore documentNodeStore,
                                   MongoDocumentStore documentStore) {
        this(documentNodeStore.getHeadRevision(), documentNodeStore, documentStore);
    }

    public NodeStateEntryTraverser(RevisionVector rootRevision, DocumentNodeStore documentNodeStore,
                                   MongoDocumentStore documentStore) {
        this.rootRevision = rootRevision;
        this.documentNodeStore = documentNodeStore;
        this.documentStore = documentStore;
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

    public NodeStateEntryTraverser withPathPredicate(Predicate<String> pathPredicate) {
        this.pathPredicate = pathPredicate;
        return this;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }

    @SuppressWarnings("Guava")
    private Iterable<NodeStateEntry> getIncludedDocs() {
        return FluentIterable.from(getDocsFilteredByPath())
                .filter(doc -> !doc.isSplitDocument())
                .transformAndConcat(doc -> getEntries(doc));
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private Iterable<NodeStateEntry> getEntries(NodeDocument doc) {
        String path = doc.getPath();

        DocumentNodeState nodeState = documentNodeStore.getNode(path, rootRevision);

        //At DocumentNodeState api level the nodeState can be null
        if (nodeState == null || !nodeState.exists()) {
            return emptyList();
        }

        return transform(
                concat(singleton(nodeState),
                    nodeState.getAllBundledNodesStates()),
                dns -> new NodeStateEntry(dns, dns.getPath())
        );
    }

    private Iterable<NodeDocument> getDocsFilteredByPath() {
        CloseableIterable<NodeDocument> docs = findAllDocuments();
        closer.register(docs);
        return docs;
    }

    private CloseableIterable<NodeDocument> findAllDocuments() {
        return new MongoDocumentTraverser(documentStore)
                .getAllDocuments(Collection.NODES, id -> includeId(id));
    }

    private boolean includeId(String id) {
        progressReporter.accept(id);
        //Cannot interpret long paths as they are hashed. So let them
        //be included
        if (Utils.isIdFromLongPath(id)){
            return true;
        }

        //Not easy to determine path for previous docs
        //Given there count is pretty low compared to others
        //include them all so that they become part of cache
        if (Utils.isPreviousDocId(id)){
            return true;
        }

        String path = Utils.getPathFromId(id);

        //Exclude hidden nodes from index data
        if (NodeStateUtils.isHiddenPath(path)){
            return false;
        }

        return pathPredicate.test(path);
    }
}
