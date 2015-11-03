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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.jackrabbit.oak.json.JsonSerializer;
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
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

/**
 * A {@link NodeState} implementation for the {@link DocumentNodeStore}.
 */
public class DocumentNodeState extends AbstractNodeState implements CacheValue {

    private static final PerfLogger perfLogger = new PerfLogger(
            LoggerFactory.getLogger(DocumentNodeState.class.getName()
                    + ".perf"));

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
    Revision lastRevision;
    final Revision rootRevision;
    final boolean fromExternalChange;
    final Map<String, PropertyState> properties;
    final boolean hasChildren;

    private final DocumentNodeStore store;

    DocumentNodeState(@Nonnull DocumentNodeStore store,
                      @Nonnull String path,
                      @Nonnull Revision rev) {
        this(store, path, rev, false);
    }

    DocumentNodeState(@Nonnull DocumentNodeStore store, @Nonnull String path,
                      @Nonnull Revision rev, boolean hasChildren) {
        this(store, path, rev, new HashMap<String, PropertyState>(),
                hasChildren, null, null, false);
    }

    private DocumentNodeState(@Nonnull DocumentNodeStore store,
                              @Nonnull String path,
                              @Nonnull Revision rev,
                              @Nonnull Map<String, PropertyState> properties,
                              boolean hasChildren,
                              @Nullable Revision lastRevision,
                              @Nullable Revision rootRevision,
                              boolean fromExternalChange) {
        this.store = checkNotNull(store);
        this.path = checkNotNull(path);
        this.rev = checkNotNull(rev);
        this.lastRevision = lastRevision;
        this.rootRevision = rootRevision != null ? rootRevision : rev;
        this.fromExternalChange = fromExternalChange;
        this.hasChildren = hasChildren;
        this.properties = checkNotNull(properties);
    }

    /**
     * Creates a copy of this {@code DocumentNodeState} with the
     * {@link #rootRevision} set to the given {@code root} revision. This method
     * returns {@code this} instance if the given {@code root} revision is
     * the same as the one in this instance and the {@link #fromExternalChange}
     * flags are equal.
     *
     * @param root the root revision for the copy of this node state.
     * @param externalChange if the {@link #fromExternalChange} flag must be
     *                       set on the returned node state.
     * @return a copy of this node state with the given root revision and
     *          external change flag.
     */
    private DocumentNodeState withRootRevision(@Nonnull Revision root,
                                               boolean externalChange) {
        if (rootRevision.equals(root) && fromExternalChange == externalChange) {
            return this;
        } else {
            return new DocumentNodeState(store, path, rev, properties,
                    hasChildren, lastRevision, root, externalChange);
        }
    }

    /**
     * @return a copy of this {@code DocumentNodeState} with the
     *          {@link #fromExternalChange} flag set to {@code true}.
     */
    @Nonnull
    DocumentNodeState fromExternalChange() {
        return new DocumentNodeState(store, path, rev, properties, hasChildren,
                lastRevision, rootRevision, true);
    }

    /**
     * @return {@code true} if this node state was created as a result of an
     *          external change; {@code false} otherwise.
     */
    boolean isFromExternalChange() {
        return fromExternalChange;
    }

    @Nonnull
    Revision getRevision() {
        return rev;
    }

    /**
     * Returns the root revision for this node state. This is the read revision
     * passed from the parent node state. This revision therefore reflects the
     * revision of the root node state where the traversal down the tree
     * started. The returned revision is only maintained on a best effort basis
     * and may be the same as {@link #getRevision()} if this node state is
     * retrieved directly from the {@code DocumentNodeStore}.
     *
     * @return the revision of the root node state is available, otherwise the
     *          same value as returned by {@link #getRevision()}.
     */
    @Nonnull
    Revision getRootRevision() {
        return rootRevision;
    }

    //--------------------------< NodeState >-----------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof DocumentNodeState) {
            DocumentNodeState other = (DocumentNodeState) that;
            if (!getPath().equals(other.getPath())) {
                // path does not match: not equals
                // (even if the properties are equal)
                return false;
            }
            if (revisionEquals(other)) {
                return true;
            }
            // revision does not match: might still be equals
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            if (modified.getBaseState() == this) {
                return EqualsDiff.equals(this, modified);
            }
        }
        if (that instanceof NodeState) {
            return AbstractNodeState.equals(this, (NodeState) that);
        }
        return false;
    }

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return properties.containsKey(name);
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
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
            return child.withRootRevision(rootRevision, fromExternalChange);
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
                        final long start = perfLogger.start();
                        try {
                            return store.compare(this, mBase, diff);
                        } finally {
                            perfLogger
                                    .end(start,
                                            1,
                                            "compareAgainstBaseState, path={}, rev={}, lastRevision={}, base.path={}, base.rev={}, base.lastRevision={}",
                                            path, rev, lastRevision,
                                            mBase.path, mBase.rev,
                                            mBase.lastRevision);
                        }
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
        } else if (prop instanceof DocumentPropertyState) {
            return ((DocumentPropertyState) prop).getValue();
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
        buff.append("{ path: '").append(path).append("', ");
        buff.append("rev: '").append(rev).append("', ");
        buff.append("properties: '").append(properties.values()).append("' }");
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
        int size = 164 + estimateMemoryUsage(path);
        // rough approximation for properties
        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            // name
            size += estimateMemoryUsage(entry.getKey());
            PropertyState propState = entry.getValue();
            if (propState.getType() != Type.BINARY
                    && propState.getType() != Type.BINARIES) {
                for (int i = 0; i < propState.count(); i++) {
                    // size() returns length of string
                    // overhead:
                    // - 8 bytes per reference in values list
                    // - 48 bytes per string
                    size += 56 + propState.size(i) * 2;
                }
            } else {
                // calculate size based on blobId value
                // referencing the binary in the blob store
                // double the size because the parsed PropertyState
                // will have a similarly sized blobId as well
                size += estimateMemoryUsage(getPropertyAsString(entry.getKey())) * 2;
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
                        return input.withRootRevision(rootRevision, fromExternalChange);
                    }
                };
            }
        });
    }

    public String asString() {
        JsopWriter json = new JsopBuilder();
        json.key("path").value(path);
        json.key("rev").value(rev.toString());
        if (lastRevision != null) {
            json.key("lastRev").value(lastRevision.toString());
        }
        if (hasChildren) {
            json.key("hasChildren").value(hasChildren);
        }
        if (properties.size() > 0) {
            json.key("prop").object();
            for (String k : properties.keySet()) {
                json.key(k).value(getPropertyAsString(k));
            }
            json.endObject();
        }
        return json.toString();
    }
    
    public static DocumentNodeState fromString(DocumentNodeStore store, String s) {
        JsopTokenizer json = new JsopTokenizer(s);
        String path = null;
        Revision rev = null;
        Revision lastRev = null;
        boolean hasChildren = false;
        DocumentNodeState state = null;
        HashMap<String, String> map = new HashMap<String, String>();
        while (true) {
            String k = json.readString();
            json.read(':');
            if ("path".equals(k)) {
                path = json.readString();
            } else if ("rev".equals(k)) {
                rev = Revision.fromString(json.readString());
            } else if ("lastRev".equals(k)) {
                lastRev = Revision.fromString(json.readString());
            } else if ("hasChildren".equals(k)) {
                hasChildren = json.read() == JsopReader.TRUE;
            } else if ("prop".equals(k)) {
                json.read('{');
                while (true) {
                    if (json.matches('}')) {
                        break;
                    }
                    k = json.readString();
                    json.read(':');
                    String v = json.readString();
                    map.put(k, v);
                    json.matches(',');
                }
            }
            if (json.matches(JsopReader.END)) {
                break;
            }
            json.read(',');
        }
        state = new DocumentNodeState(store, path, rev, hasChildren);
        state.setLastRevision(lastRev);
        for (Entry<String, String> e : map.entrySet()) {
            state.setProperty(e.getKey(), e.getValue());
        }
        return state;
    }

    /**
     * A list of children for a node.
     */
    public static class Children implements CacheValue {

        /**
         * Ascending sorted list of names of child nodes.
         */
        final ArrayList<String> children = new ArrayList<String>();
        int cachedMemory;
        boolean hasMore;

        @Override
        public int getMemory() {
            if (cachedMemory == 0) {
                int size = 48;
                if (!children.isEmpty()) {
                    size = 114;
                    for (String c : children) {
                        size += estimateMemoryUsage(c) + 8;
                    }
                }
                cachedMemory = size;
            }
            return cachedMemory;
        }

        @Override
        public String toString() {
            return children.toString();
        }

        public String asString() {
            JsopWriter json = new JsopBuilder();
            if (hasMore) {
                json.key("hasMore").value(true);
            }
            if (children.size() > 0) {
                json.key("children").array();
                for (String c : children) {
                    json.value(c);
                }
                json.endArray();
            }
            return json.toString();            
        }
        
        public static Children fromString(String s) {
            JsopTokenizer json = new JsopTokenizer(s);
            Children children = new Children();
            while (true) {
                if (json.matches(JsopReader.END)) {
                    break;
                }
                String k = json.readString();
                json.read(':');
                if ("hasMore".equals(k)) {
                    children.hasMore = json.read() == JsopReader.TRUE;
                } else if ("children".equals(k)) {
                    json.read('[');
                    while (true) {
                        if (json.matches(']')) {
                            break;
                        }
                        String value = json.readString();
                        children.children.add(value);
                        json.matches(',');
                    }
                }
                if (json.matches(JsopReader.END)) {
                    break;
                }
                json.read(',');
            }
            return children;            
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
