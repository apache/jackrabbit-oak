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

import javax.annotation.CheckForNull;
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
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;

/**
 * A {@link NodeState} implementation for the {@link DocumentNodeStore}.
 */
public class DocumentNodeState extends AbstractDocumentNodeState implements CacheValue {

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
    final RevisionVector readRevision;
    RevisionVector lastRevision;
    final RevisionVector rootRevision;
    final boolean fromExternalChange;
    final Map<String, PropertyState> properties;
    final boolean hasChildren;

    private final DocumentNodeStore store;

    private AbstractDocumentNodeState cachedSecondaryState;

    DocumentNodeState(@Nonnull DocumentNodeStore store,
                      @Nonnull String path,
                      @Nonnull RevisionVector readRevision) {
        this(store, path, readRevision, false);
    }

    DocumentNodeState(@Nonnull DocumentNodeStore store, @Nonnull String path,
                      @Nonnull RevisionVector readRevision, boolean hasChildren) {
        this(store, path, readRevision, new HashMap<String, PropertyState>(),
                hasChildren, null, null, false);
    }

    private DocumentNodeState(@Nonnull DocumentNodeStore store,
                              @Nonnull String path,
                              @Nonnull RevisionVector readRevision,
                              @Nonnull Map<String, PropertyState> properties,
                              boolean hasChildren,
                              @Nullable RevisionVector lastRevision,
                              @Nullable RevisionVector rootRevision,
                              boolean fromExternalChange) {
        this.store = checkNotNull(store);
        this.path = checkNotNull(path);
        this.readRevision = checkNotNull(readRevision);
        this.lastRevision = lastRevision;
        this.rootRevision = rootRevision != null ? rootRevision : readRevision;
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
    @Override
    public DocumentNodeState withRootRevision(@Nonnull RevisionVector root,
                                               boolean externalChange) {
        if (rootRevision.equals(root) && fromExternalChange == externalChange) {
            return this;
        } else {
            return new DocumentNodeState(store, path, readRevision, properties,
                    hasChildren, lastRevision, root, externalChange);
        }
    }

    /**
     * @return a copy of this {@code DocumentNodeState} with the
     *          {@link #fromExternalChange} flag set to {@code true}.
     */
    @Nonnull
    DocumentNodeState fromExternalChange() {
        return new DocumentNodeState(store, path, readRevision, properties, hasChildren,
                lastRevision, rootRevision, true);
    }

    /**
     * @return {@code true} if this node state was created as a result of an
     *          external change; {@code false} otherwise.
     */
    @Override
    public boolean isFromExternalChange() {
        return fromExternalChange;
    }

    //--------------------------< AbstractDocumentNodeState >-----------------------------------

    @Override
    @Nonnull
    public RevisionVector getRevision() {
        return readRevision;
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
    public RevisionVector getRootRevision() {
        return rootRevision;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public RevisionVector getLastRevision() {
        return lastRevision;
    }

    //--------------------------< NodeState >-----------------------------------


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
            return getChildNodeDoc(name) != null;
        }
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        if (!hasChildren) {
            checkValidName(name);
            return EmptyNodeState.MISSING_NODE;
        }
        AbstractDocumentNodeState child = getChildNodeDoc(name);
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

    @Override
    public long getPropertyCount() {
        return properties.size();
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (!hasChildren) {
            return Collections.emptyList();
        }

        AbstractDocumentNodeState secondaryState = getSecondaryNodeState();
        if (secondaryState != null){
            return secondaryState.getChildNodeEntries();
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
            if (readRevision.isBranch()) {
                // check if this node state is head of a branch
                Branch b = store.getBranches().getBranch(readRevision);
                if (b == null) {
                    if (store.isDisableBranches()) {
                        if (DocumentNodeStoreBranch.getCurrentBranch() != null) {
                            return new DocumentRootBuilder(this, store);
                        } else {
                            return new MemoryNodeBuilder(this);
                        }
                    } else {
                        throw new IllegalStateException("No branch for revision: " + readRevision);
                    }
                }
                if (b.isHead(readRevision.getBranchRevision())
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

    @Override
    public boolean hasNoChildren() {
        return !hasChildren;
    }

    @Override
    protected NodeStateDiffer getNodeStateDiffer() {
        return store;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("{ path: '").append(path).append("', ");
        buff.append("readRevision: '").append(readRevision).append("', ");
        buff.append("properties: '").append(properties.values()).append("' }");
        return buff.toString();
    }

    /**
     * Create an add operation for this node at the given revision.
     *
     * @param revision the revision this node is created.
     */
    UpdateOp asOperation(@Nonnull Revision revision) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, true);
        op.set(Document.ID, id);
        if (Utils.isLongPath(path)) {
            op.set(NodeDocument.PATH, path);
        }
        NodeDocument.setModified(op, revision);
        NodeDocument.setDeleted(op, revision, false);
        for (String p : properties.keySet()) {
            String key = Utils.escapePropertyName(p);
            op.setMapEntry(key, revision, getPropertyAsString(p));
        }
        return op;
    }

    String getId() {
        return path + "@" + lastRevision;
    }

    void setLastRevision(RevisionVector lastRevision) {
        this.lastRevision = lastRevision;
    }

    @Override
    public int getMemory() {
        int size = 40 // shallow
                + readRevision.getMemory()
                + (lastRevision != null ? lastRevision.getMemory() : 0)
                + rootRevision.getMemory()
                + estimateMemoryUsage(path);
        // rough approximation for properties
        for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
            // name
            size += estimateMemoryUsage(entry.getKey());
            PropertyState propState = entry.getValue();
            if (propState.getType() != Type.BINARY
                    && propState.getType() != Type.BINARIES) {
                for (int i = 0; i < propState.count(); i++) {
                    // size() returns length of string
                    // shallow memory:
                    // - 8 bytes per reference in values list
                    // - 48 bytes per string
                    // double useage per property because of parsed PropertyState
                    size += (56 + propState.size(i) * 2) * 2;
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

    @CheckForNull
    private AbstractDocumentNodeState getChildNodeDoc(String childNodeName){
        AbstractDocumentNodeState secondaryState = getSecondaryNodeState();
        if (secondaryState != null){
            NodeState result = secondaryState.getChildNode(childNodeName);
            //If given child node exist then cast it and return
            //else return null
            if (result.exists()){
                return (AbstractDocumentNodeState) result;
            }
            return null;
        }
        return store.getNode(PathUtils.concat(getPath(), childNodeName), lastRevision);
    }

    @CheckForNull
    private AbstractDocumentNodeState getSecondaryNodeState(){
        if (cachedSecondaryState == null){
            cachedSecondaryState = store.getSecondaryNodeState(getPath(), rootRevision, lastRevision);
        }
        return cachedSecondaryState;
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
        Iterable<? extends AbstractDocumentNodeState> children = store.getChildNodes(this, name, limit);
        return Iterables.transform(children, new Function<AbstractDocumentNodeState, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(final AbstractDocumentNodeState input) {
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
        json.key("rev").value(readRevision.toString());
        if (lastRevision != null) {
            json.key("lastRev").value(lastRevision.toString());
        }
        if (hasChildren) {
            json.key("hasChildren").value(true);
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
        RevisionVector rev = null;
        RevisionVector lastRev = null;
        boolean hasChildren = false;
        DocumentNodeState state = null;
        HashMap<String, String> map = new HashMap<String, String>();
        while (true) {
            String k = json.readString();
            json.read(':');
            if ("path".equals(k)) {
                path = json.readString();
            } else if ("rev".equals(k)) {
                rev = RevisionVector.fromString(json.readString());
            } else if ("lastRev".equals(k)) {
                lastRev = RevisionVector.fromString(json.readString());
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
