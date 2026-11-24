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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;

/**
 * A {@link DocumentStore} view for a cluster node with its own document cache.
 * All documents are read from the shared store, except for the root document,
 * which can be frozen. A frozen root document emulates a cluster node that
 * has not yet seen the latest commits of another cluster node, because its
 * cached commit root document was not yet refreshed by a background read.
 * <p>
 * Documents are returned as copies bound to this store, which ensures
 * commit root lookups ({@link NodeDocument#resolveCommitValue(Revision)})
 * go through this view as well.
 */
class StaleCommitRootDocumentStore extends DocumentStoreWrapper {

    private static final String ROOT_ID = Utils.getIdFromPath(PathUtils.ROOT_PATH);

    private volatile NodeDocument frozenRoot;

    StaleCommitRootDocumentStore(DocumentStore store) {
        super(store);
    }

    void freezeCommitRoot() {
        frozenRoot = copyOf(store.find(NODES, ROOT_ID));
    }

    void unfreezeCommitRoot() {
        frozenRoot = null;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return view(collection, key, store.find(collection, key));
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return view(collection, key, store.find(collection, key, maxCacheAge));
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return view(collection, key, store.getIfCached(collection, key));
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey,
                                              String toKey, int limit) {
        return view(collection, store.query(collection, fromKey, toKey, limit));
    }

    @NotNull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey,
                                              String toKey, String indexedProperty,
                                              long startValue, int limit) {
        return view(collection, store.query(collection, fromKey, toKey,
                indexedProperty, startValue, limit));
    }

    @SuppressWarnings("unchecked")
    private <T extends Document> T view(Collection<T> collection, String key, T doc) {
        if (collection != NODES) {
            return doc;
        }
        NodeDocument root = frozenRoot;
        if (root != null && ROOT_ID.equals(key)) {
            return (T) root;
        }
        return (T) copyOf((NodeDocument) doc);
    }

    private <T extends Document> List<T> view(Collection<T> collection, List<T> docs) {
        if (collection != NODES) {
            return docs;
        }
        return docs.stream().map(d -> view(collection, d.getId(), d)).collect(Collectors.toList());
    }

    private NodeDocument copyOf(NodeDocument doc) {
        if (doc == null) {
            return null;
        }
        NodeDocument copy = NODES.newDocument(this);
        doc.deepCopy(copy);
        copy.seal();
        return copy;
    }
}
