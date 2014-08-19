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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.kernel.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * A {@link NodeState} implementation for the {@link DocumentNodeStore}.
 */
class DocumentNodeState extends AbstractNodeState implements CacheValue {

    public static final Children NO_CHILDREN = new Children();

    /**
     * The number of child nodes to fetch initially.
     */
    static final int INITIAL_FETCH_SIZE = 100;

    /**
     * The maximum number of child nodes to fetch in one call. (1600).
     */
    static final int MAX_FETCH_SIZE = INITIAL_FETCH_SIZE << 4;

    final String path;
    final Revision rev;
    final Map<String, PropertyState> properties = Maps.newHashMap();
    Revision lastRevision;
    final boolean hasChildren;

    private final DocumentNodeStore store;

    DocumentNodeState(@Nonnull DocumentNodeStore store, @Nonnull String path,
                      @Nonnull Revision rev) {
        this(store, path, rev, false);
    }

    DocumentNodeState(@Nonnull DocumentNodeStore store, @Nonnull String path,
                      @Nonnull Revision rev, boolean hasChildren) {
        this.store = checkNotNull(store);
        this.path = checkNotNull(path);
        this.rev = checkNotNull(rev);
        this.hasChildren = hasChildren;
    }

    Revision getRevision() {
        return rev;
    }

    //--------------------------< NodeState >-----------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof DocumentNodeState) {
            DocumentNodeState other = (DocumentNodeState) that;
            if (getPath().equals(other.getPath())) {
                if (revisionEquals(other)) {
                    return true;
                }
            }
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            if (modified.getBaseState() == this) {
                return EqualsDiff.equals(this, modified);
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        } else {
            return false;
        }
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public PropertyState getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(String name) {
        return properties.containsKey(name);
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public boolean hasChildNode(String name) {
        if (!hasChildren || !isValidName(name)) {
            return false;
        } else {
            String p = PathUtils.concat(getPath(), name);
            return store.getNode(p, lastRevision) != null;
        }
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        if (!hasChildren) {
            checkValidName(name);
            return EmptyNodeState.MISSING_NODE;
        }
        String p = PathUtils.concat(getPath(), name);
        DocumentNodeState child = store.getNode(p, lastRevision);
        if (child == null) {
            checkValidName(name);
            return EmptyNodeState.MISSING_NODE;
        } else {
            return child;
        }
    }

    @Override
    public long getChildNodeCount(long max) {
        if (!hasChildren) {
            return 0;
        }
        if (max > DocumentNodeStore.NUM_CHILDREN_CACHE_LIMIT) {
            // count all
            return Iterators.size(new ChildNodeEntryIterator());
        }
        Children c = store.getChildren(this, null, (int) max);
        if (c.hasMore) {
            return Long.MAX_VALUE;
        } else {
            // we know the exact value
            return c.children.size();
        }
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (!hasChildren) {
            return Collections.emptyList();
        }
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                return new ChildNodeEntryIterator();
            }
        };
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        if ("/".equals(getPath())) {
            if (rev.isBranch()) {
                // check if this node state is head of a branch
                Branch b = store.getBranches().getBranch(rev);
                if (b == null) {
                    if (store.isDisableBranches()) {
                        if (DocumentNodeStoreBranch.getCurrentBranch() != null) {
                            return new DocumentRootBuilder(this, store);
                        } else {
                            return new MemoryNodeBuilder(this);
                        }
                    } else {
                        throw new IllegalStateException("No branch for revision: " + rev);
                    }
                }
                if (b.isHead(rev)
                        && DocumentNodeStoreBranch.getCurrentBranch() != null) {
                    return new DocumentRootBuilder(this, store);
                } else {
                    return new MemoryNodeBuilder(this);
                }
            } else {
                return new DocumentRootBuilder(this, store);
            }
        } else {
            return new MemoryNodeBuilder(this);
        }
    }

    @Override
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return true;
        } else if (base == EMPTY_NODE || !base.exists()) {
            // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (base instanceof DocumentNodeState) {
            DocumentNodeState mBase = (DocumentNodeState) base;
            if (store == mBase.store) {
                if (getPath().equals(mBase.getPath())) {
                    if (revisionEquals(mBase)) {
                        // no differences
                        return true;
                    } else {
                        // use DocumentNodeStore compare
                        return dispatch(store.diffChildren(this, mBase), mBase, diff);
                    }
                }
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
    }

    void setProperty(String propertyName, String value) {
        if (value == null) {
            properties.remove(propertyName);
        } else {
            properties.put(propertyName,
                    new DocumentPropertyState(store, propertyName, value));
        }
    }

    void setProperty(PropertyState property) {
        properties.put(property.getName(), property);
    }

    String getPropertyAsString(String propertyName) {
        PropertyState prop = properties.get(propertyName);
        if (prop == null) {
            return null;
        }
        JsopBuilder builder = new JsopBuilder();
        new JsonSerializer(builder, store.getBlobSerializer()).serialize(prop);
        return builder.toString();
    }

    Set<String> getPropertyNames() {
        return properties.keySet();
    }

    void copyTo(DocumentNodeState newNode) {
        newNode.properties.putAll(properties);
    }

    boolean hasNoChildren() {
        return !hasChildren;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("path: ").append(path).append('\n');
        buff.append("rev: ").append(rev).append('\n');
        buff.append(properties);
        buff.append('\n');
        return buff.toString();
    }

    /**
     * Create an add node operation for this node.
     */
    UpdateOp asOperation(boolean isNew) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, isNew);
        op.set(Document.ID, id);
        if (Utils.isLongPath(path)) {
            op.set(NodeDocument.PATH, path);
        }
        NodeDocument.setModified(op, rev);
        NodeDocument.setDeleted(op, rev, false);
        for (String p : properties.keySet()) {
            String key = Utils.escapePropertyName(p);
            op.setMapEntry(key, rev, getPropertyAsString(p));
        }
        return op;
    }

    String getPath() {
        return path;
    }

    String getId() {
        return path + "@" + lastRevision;
    }

    void append(JsopWriter json, boolean includeId) {
        if (includeId) {
            json.key(":id").value(getId());
        }
        for (String p : properties.keySet()) {
            json.key(p).encodedValue(getPropertyAsString(p));
        }
    }

    void setLastRevision(Revision lastRevision) {
        this.lastRevision = lastRevision;
    }

    Revision getLastRevision() {
        return lastRevision;
    }

    @Override
    public int getMemory() {
        int size = 180 + path.length() * 2;
        // rough approximation for properties
        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            // name
            size += 48 + entry.getKey().length() * 2;
            PropertyState propState = entry.getValue();
            if (propState.getType() != Type.BINARY
                    && propState.getType() != Type.BINARIES) {
                // assume binaries go into blob store
                for (int i = 0; i < propState.count(); i++) {
                    // size() returns length of string
                    // overhead:
                    // - 8 bytes per reference in values list
                    // - 48 bytes per string
                    size += 56 + propState.size(i) * 2;
                }
            }
        }
        return size;
    }

    //------------------------------< internal >--------------------------------

    /**
     * Returns {@code true} if this state has the same revision as the
     * {@code other} state. This method first compares the read {@link #rev}
     * and then the {@link #lastRevision}.
     *
     * @param other the other state to compare with.
     * @return {@code true} if the revisions are equal, {@code false} otherwise.
     */
    private boolean revisionEquals(DocumentNodeState other) {
        return this.rev.equals(other.rev)
                || this.lastRevision.equals(other.lastRevision);
    }

    private boolean dispatch(@Nonnull String jsonDiff,
                             @Nonnull DocumentNodeState base,
                             @Nonnull NodeStateDiff diff) {
        if (!AbstractNodeState.comparePropertiesAgainstBaseState(this, base, diff)) {
            return false;
        }
        if (jsonDiff.trim().isEmpty()) {
            return true;
        }
        JsopTokenizer t = new JsopTokenizer(jsonDiff);
        boolean continueComparison = true;
        while (continueComparison) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            switch (r) {
                case '+': {
                    String name = t.readString();
                    t.read(':');
                    t.read('{');
                    while (t.read() != '}') {
                        // skip properties
                    }
                    continueComparison = diff.childNodeAdded(name, getChildNode(name));
                    break;
                }
                case '-': {
                    String name = t.readString();
                    continueComparison = diff.childNodeDeleted(name, base.getChildNode(name));
                    break;
                }
                case '^': {
                    String name = t.readString();
                    t.read(':');
                    if (t.matches('{')) {
                        t.read('}');
                        continueComparison = diff.childNodeChanged(name,
                                base.getChildNode(name), getChildNode(name));
                    } else if (t.matches('[')) {
                        // ignore multi valued property
                        while (t.read() != ']') {
                            // skip values
                        }
                    } else {
                        // ignore single valued property
                        t.read();
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("jsonDiff: illegal token '"
                            + t.getToken() + "' at pos: " + t.getLastPos() + ' ' + jsonDiff);
            }
        }
        return continueComparison;
    }

    /**
     * Returns up to {@code limit} child node entries, starting after the given
     * {@code name}.
     *
     * @param name the name of the lower bound child node entry (exclusive) or
     *             {@code null}, if the method should start with the first known
     *             child node.
     * @param limit the maximum number of child node entries to return.
     * @return the child node entries.
     */
    @Nonnull
    private Iterable<ChildNodeEntry> getChildNodeEntries(@Nullable String name,
                                                         int limit) {
        Iterable<DocumentNodeState> children = store.getChildNodes(this, name, limit);
        return Iterables.transform(children, new Function<DocumentNodeState, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(final DocumentNodeState input) {
                return new AbstractChildNodeEntry() {
                    @Nonnull
                    @Override
                    public String getName() {
                        return PathUtils.getName(input.getPath());
                    }

                    @Nonnull
                    @Override
                    public NodeState getNodeState() {
                        return input;
                    }
                };
            }
        });
    }

    /**
     * A list of children for a node.
     */
    public static class Children implements CacheValue {

        /**
         * Ascending sorted list of names of child nodes.
         */
        final ArrayList<String> children = new ArrayList<String>();
        boolean hasMore;

        @Override
        public int getMemory() {
            int size = 114;
            for (String c : children) {
                size += c.length() * 2 + 56;
            }
            return size;
        }

        @Override
        public String toString() {
            return children.toString();
        }
    }

    private class ChildNodeEntryIterator implements Iterator<ChildNodeEntry> {

        private String previousName;
        private Iterator<ChildNodeEntry> current;
        private int fetchSize = INITIAL_FETCH_SIZE;
        private int currentRemaining = fetchSize;

        ChildNodeEntryIterator() {
            fetchMore();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                if (current == null) {
                    return false;
                } else if (current.hasNext()) {
                    return true;
                } else if (currentRemaining > 0) {
                    // current returned less than fetchSize
                    return false;
                }
                fetchMore();
            }
        }

        @Override
        public ChildNodeEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ChildNodeEntry entry = current.next();
            previousName = entry.getName();
            currentRemaining--;
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void fetchMore() {
            Iterator<ChildNodeEntry> entries = getChildNodeEntries(
                    previousName, fetchSize).iterator();
            currentRemaining = fetchSize;
            fetchSize = Math.min(fetchSize * 2, MAX_FETCH_SIZE);
            if (entries.hasNext()) {
                current = entries;
            } else {
                current = null;
            }
        }
    }

}
