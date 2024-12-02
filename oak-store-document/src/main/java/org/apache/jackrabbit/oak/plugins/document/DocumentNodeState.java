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
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.TreeTraverser;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlorUtils;
import org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor;
import org.apache.jackrabbit.oak.plugins.document.bundlor.Matcher;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;

/**
 * A {@link NodeState} implementation for the {@link DocumentNodeStore}.
 */
public class DocumentNodeState extends AbstractDocumentNodeState implements CacheValue {

    private static final Logger log = LoggerFactory.getLogger(DocumentNodeState.class);

    public static final Children NO_CHILDREN = new Children();

    /**
     * The number of child nodes to fetch initially.
     */
    static final int INITIAL_FETCH_SIZE = 100;

    /**
     * The maximum number of child nodes to fetch in one call. (1600).
     */
    static final int MAX_FETCH_SIZE = INITIAL_FETCH_SIZE << 4;

    private final Path path;
    private final RevisionVector lastRevision;
    private final RevisionVector rootRevision;
    private final boolean fromExternalChange;
    private final Map<String, PropertyState> properties;
    private final boolean hasChildren;

    private final DocumentNodeStore store;
    private final BundlingContext bundlingContext;

    private AbstractDocumentNodeState cachedSecondaryState;
    private int memory;

    DocumentNodeState(@NotNull DocumentNodeStore store,
                      @NotNull Path path,
                      @NotNull RevisionVector rootRevision) {
        this(store, path, rootRevision, Collections.<PropertyState>emptyList(), false, null);
    }

    DocumentNodeState(@NotNull DocumentNodeStore store, @NotNull Path path,
                      @NotNull RevisionVector rootRevision,
                      Iterable<? extends PropertyState> properties,
                      boolean hasChildren,
                      @Nullable RevisionVector lastRevision) {
        this(store, path, rootRevision, asMap(properties),
                hasChildren, 0, lastRevision, false);
    }

    public DocumentNodeState(@NotNull DocumentNodeStore store,
                             @NotNull Path path,
                             @NotNull RevisionVector rootRevision,
                             @NotNull Map<String, PropertyState> properties,
                             boolean hasChildren,
                             int memory,
                             @Nullable RevisionVector lastRevision,
                             boolean fromExternalChange) {
        this(store, path, lastRevision, rootRevision,
                fromExternalChange, createBundlingContext(requireNonNull(properties), hasChildren), memory);
    }

    protected DocumentNodeState(@NotNull DocumentNodeStore store,
                                @NotNull Path path,
                                @Nullable RevisionVector lastRevision,
                                @NotNull RevisionVector rootRevision,
                                boolean fromExternalChange,
                                BundlingContext bundlingContext,
                                int memory) {
        this.store = requireNonNull(store);
        this.path = requireNonNull(path);
        this.rootRevision = requireNonNull(rootRevision);
        this.lastRevision = lastRevision;
        this.fromExternalChange = fromExternalChange;
        this.properties = bundlingContext.getProperties();
        this.bundlingContext = bundlingContext;
        this.hasChildren = bundlingContext.hasChildren();
        this.memory = memory;
    }

    static DocumentNodeState newMissingNode(@NotNull DocumentNodeStore store,
                                            @NotNull Path path,
                                            @NotNull RevisionVector rootRevision) {
        return new DocumentNodeState(store, path, rootRevision) {
            @Override
            public boolean exists() {
                return false;
            }
        };
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
    public DocumentNodeState withRootRevision(@NotNull RevisionVector root,
                                               boolean externalChange) {
        if (rootRevision.equals(root) && fromExternalChange == externalChange) {
            return this;
        } else {
            return new DocumentNodeState(store, path, lastRevision, root, externalChange, bundlingContext, memory);
        }
    }

    /**
     * @return a copy of this {@code DocumentNodeState} with the
     *          {@link #fromExternalChange} flag set to {@code true}.
     */
    @NotNull
    public DocumentNodeState fromExternalChange() {
        return new DocumentNodeState(store, path, lastRevision, rootRevision, true, bundlingContext, memory);
    }

    /**
     * Returns this state as a branch root state connected to the given
     * {@code branch}.
     *
     * @param branch the branch instance.
     * @return a {@link DocumentBranchRootNodeState} connected to the given
     *      {@code branch}.
     * @throws IllegalStateException if this is not a root node state or does
     *      not represent a branch state.
     */
    @NotNull
    DocumentNodeState asBranchRootState(@NotNull DocumentNodeStoreBranch branch) {
        Validate.checkState(path.isRoot());
        Validate.checkState(getRootRevision().isBranch());
        return new DocumentBranchRootNodeState(store, branch, path, rootRevision, lastRevision, bundlingContext, memory);
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

    /**
     * Returns the root revision for this node state. This is the root revision
     * passed from the parent node state. This revision therefore reflects the
     * revision of the root node state where the traversal down the tree
     * started.
     *
     * @return the revision of the root node state.
     */
    @NotNull
    public RevisionVector getRootRevision() {
        return rootRevision;
    }

    @Override
    public Path getPath() {
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
    public PropertyState getProperty(@NotNull String name) {
        return properties.get(name);
    }

    @Override
    public boolean hasProperty(@NotNull String name) {
        return properties.containsKey(name);
    }

    @NotNull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        //Filter out the meta properties related to bundling from
        //generic listing of props
        if (bundlingContext.isBundled()){
            return Iterables.filter(properties.values(), BundlorUtils.NOT_BUNDLOR_PROPS::test);
        }
        return properties.values();
    }

    @Override
    public boolean hasChildNode(@NotNull String name) {
        if (!hasChildren || !isValidName(name)) {
            return false;
        } else {
            return getChildNodeDoc(name) != null;
        }
    }

    @NotNull
    @Override
    public NodeState getChildNode(@NotNull String name) {
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

    /**
     /**
     * Returns the child node for the given name from the cache. This method
     * returns {@code null} if the cache does not have an entry for the child
     * node.
     * <p>
     * Please note, the returned node state may also represent a node that does
     * not exist. In which case {@link #exists()} of the returned node state
     * will return {@code false}.
     *
     * @param name the name of the child node.
     * @return the node state or {@code null} if the cache does not have an
     *          entry for the child node.
     */
    @Nullable
    public DocumentNodeState getChildIfCached(String name) {
        return store.getNodeIfCached(new Path(getPath(), name), lastRevision);
    }

    @Override
    public long getChildNodeCount(long max) {
        if (!hasChildren) {
            return 0;
        }

        int bundledChildCount = bundlingContext.getBundledChildNodeNames().size();
        if (bundlingContext.hasOnlyBundledChildren()){
            return bundledChildCount;
        }

        String name = "";
        long count = 0;
        int fetchSize = INITIAL_FETCH_SIZE;
        long remaining = Math.max(max, 1); // fetch at least once
        Children c = NO_CHILDREN;
        while (remaining > 0) {
            c = store.getChildren(this, name, fetchSize);
            count += c.children.size();
            remaining -= c.children.size();
            if (!c.hasMore) {
                break;
            }
            name = c.children.get(c.children.size() - 1);
            fetchSize = Math.min(fetchSize << 1, MAX_FETCH_SIZE);
        }
        if (!c.hasMore) {
            // we know the exact value
            return count + bundledChildCount;
        } else {
            // there are more than max
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long getPropertyCount() {
        if (bundlingContext.isBundled()){
            return Iterables.size(getProperties());
        }
        return properties.size();
    }

    @NotNull
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
                if (bundlingContext.isBundled()) {
                    //If all the children are bundled
                    if (bundlingContext.hasOnlyBundledChildren()){
                        return getBundledChildren();
                    }
                    return Iterators.concat(getBundledChildren(), new ChildNodeEntryIterator());
                }

                return new ChildNodeEntryIterator();
            }
        };
    }

    @NotNull
    @Override
    public NodeBuilder builder() {
        if (getPath().isRoot()) {
            if (getRootRevision().isBranch()) {
                throw new IllegalStateException("Cannot create builder from branched DocumentNodeState");
            } else {
                return new DocumentRootBuilder(this, store, store.createBranch(this));
            }
        } else {
            return new MemoryNodeBuilder(this);
        }
    }

    public Set<String> getBundledChildNodeNames(){
        return bundlingContext.getBundledChildNodeNames();
    }

    public boolean hasOnlyBundledChildren(){
        if (bundlingContext.isBundled()){
            return bundlingContext.hasOnlyBundledChildren();
        }
        return false;
    }

    String getPropertyAsString(String propertyName) {
        return asString(properties.get(propertyName));
    }

    private String asString(PropertyState prop) {
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
        buff.append("rootRevision: '").append(rootRevision).append("', ");
        buff.append("lastRevision: '").append(lastRevision).append("', ");
        buff.append("properties: '").append(properties.values()).append("' }");
        return buff.toString();
    }

    /**
     * Create an add operation for this node at the given revision.
     *
     * @param revision the revision this node is created.
     */
    UpdateOp asOperation(@NotNull Revision revision) {
        String id = Utils.getIdFromPath(path);
        UpdateOp op = new UpdateOp(id, true);
        if (Utils.isIdFromLongPath(id)) {
            op.set(NodeDocument.PATH, path.toString());
        }
        NodeDocument.setModified(op, revision);
        NodeDocument.setDeleted(op, revision, false);
        for (String p : properties.keySet()) {
            String key = Utils.escapePropertyName(p);
            op.setMapEntry(key, revision, getPropertyAsString(p));
        }
        return op;
    }

    @Override
    public int getMemory() {
        long size = memory;
        if (size == 0) {
            size = 40 // shallow
                    + (lastRevision != null ? lastRevision.getMemory() : 0)
                    + rootRevision.getMemory()
                    + path.getMemory();
            // rough approximation for properties
            for (Map.Entry<String, PropertyState> entry : bundlingContext.getAllProperties().entrySet()) {
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
                        // double usage per property because of parsed PropertyState
                        size += (56 + propState.size(i) * 2) * 2;
                    }
                } else {
                    // calculate size based on blobId value
                    // referencing the binary in the blob store
                    // double the size because the parsed PropertyState
                    // will have a similarly sized blobId as well
                    size += (long)estimateMemoryUsage(asString(entry.getValue())) * 2;
                }
            }
            if (size > Integer.MAX_VALUE) {
                log.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
                size = Integer.MAX_VALUE;
            }
            memory = (int) size;
        }
        return (int) size;
    }

    public Iterable<DocumentNodeState> getAllBundledNodesStates() {
        return new TreeTraverser<DocumentNodeState>(){
            @Override
            public Iterable<DocumentNodeState> children(DocumentNodeState root) {
                return Iterables.transform(() -> root.getBundledChildren(), ce -> (DocumentNodeState)ce.getNodeState());
            }
        }.preOrderTraversal(this)
         .filter(dns -> !dns.getPath().equals(this.getPath()) ); //Exclude this
    }

    /**
     * Returns all properties, including bundled, as Json serialized value.
     *
     * @return all properties, including bundled.
     */
    public Map<String, String> getAllBundledProperties() {
        Map<String, String> allProps = new HashMap<>();
        for (Map.Entry<String, PropertyState> e : bundlingContext.getAllProperties().entrySet()) {
            allProps.put(e.getKey(), asString(e.getValue()));
        }
        return allProps;
    }

    //------------------------------< internal >--------------------------------

    @Nullable
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

        Matcher child = bundlingContext.matcher.next(childNodeName);
        if (child.isMatch()){
            if (bundlingContext.hasChildNode(child.getMatchedPath())){
                return createBundledState(childNodeName, child);
            } else {
                return null;
            }
        } else if (bundlingContext.hasOnlyBundledChildren()) {
            return null;
        }

        return store.getNode(new Path(getPath(), childNodeName), lastRevision);
    }

    @Nullable
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
     *             the empty {@code String}, if the method should start with the
     *             first known child node.
     * @param limit the maximum number of child node entries to return.
     * @return the child node entries.
     */
    @NotNull
    private Iterable<ChildNodeEntry> getChildNodeEntries(@NotNull String name,
                                                         int limit) {
        Iterable<? extends AbstractDocumentNodeState> children = store.getChildNodes(this, name, limit);
        return Iterables.transform(children, input -> {
                return new AbstractChildNodeEntry() {
                    @Override
                    public String getName() {
                        return input.getPath().getName();
                    }

                    @Override
                    public NodeState getNodeState() {
                        return input;
                    }
                };
            });
    }

    private static Map<String, PropertyState> asMap(Iterable<? extends PropertyState> props){
        Map<String, PropertyState> builder = new HashMap<>();
        for (PropertyState ps : props){
            builder.put(ps.getName(), ps);
        }
        return Collections.unmodifiableMap(builder);
    }

    /**
     * A list of children for a node.
     */
    public static class Children implements CacheValue {

        /**
         * Ascending sorted list of names of child nodes.
         */
        final ArrayList<String> children = new ArrayList<String>();
        long cachedMemory;
        boolean hasMore;

        @Override
        public int getMemory() {
            if (cachedMemory == 0) {
                long size = 48;
                if (!children.isEmpty()) {
                    size = 114;
                    for (String c : children) {
                        size += (long)estimateMemoryUsage(c) + 8;
                    }
                }
                cachedMemory = size;
            }
            if (cachedMemory > Integer.MAX_VALUE) {
                log.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", cachedMemory);
                return Integer.MAX_VALUE;
            } else {
                return (int)cachedMemory;
            }
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

        private String previousName = "";
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

    //~----------------------------------------------< Bundling >

    private AbstractDocumentNodeState createBundledState(String childNodeName, Matcher child) {
        return new DocumentNodeState(
                store,
                new Path(path, childNodeName),
                lastRevision,
                rootRevision,
                fromExternalChange,
                bundlingContext.childContext(child),
                memory);
    }

    private Iterator<ChildNodeEntry> getBundledChildren(){
        return Iterators.transform(bundlingContext.getBundledChildNodeNames().iterator(), childNodeName -> {
                return new AbstractChildNodeEntry() {
                    @Override
                    public String getName() {
                        return childNodeName;
                    }

                    @Override
                    public NodeState getNodeState() {
                        return createBundledState(childNodeName, bundlingContext.matcher.next(childNodeName));
                    }
                };
        });
    }

    private static BundlingContext createBundlingContext(Map<String, PropertyState> properties,
                                                         boolean hasNonBundledChildren) {
        PropertyState bundlorConfig = properties.get(DocumentBundlor.META_PROP_PATTERN);
        Matcher matcher = Matcher.NON_MATCHING;
        boolean hasBundledChildren = false;
        if (bundlorConfig != null){
            matcher = DocumentBundlor.from(bundlorConfig).createMatcher();
            hasBundledChildren = hasBundledProperty(properties, matcher, DocumentBundlor.META_PROP_BUNDLED_CHILD);
        }
        return new BundlingContext(matcher, properties, hasBundledChildren, hasNonBundledChildren);
    }

    private static boolean hasBundledProperty(Map<String, PropertyState> props, Matcher matcher, String propName){
        String key = concat(matcher.getMatchedPath(), propName);
        return props.containsKey(key);
    }

    protected static class BundlingContext {
        final Matcher matcher;
        final Map<String, PropertyState> rootProperties;
        final boolean hasBundledChildren;
        final boolean hasNonBundledChildren;

        public BundlingContext(Matcher matcher, Map<String, PropertyState> rootProperties,
                               boolean hasBundledChildren, boolean hasNonBundledChildren) {
            this.matcher = matcher;
            this.rootProperties = Map.copyOf(rootProperties);
            this.hasBundledChildren = hasBundledChildren;
            this.hasNonBundledChildren = hasNonBundledChildren;
        }

        public BundlingContext childContext(Matcher childMatcher){
            return new BundlingContext(childMatcher, rootProperties,
                    hasBundledChildren(childMatcher), hasNonBundledChildren(childMatcher));
        }

        public Map<String, PropertyState> getProperties(){
            if (matcher.isMatch()){
                return BundlorUtils.getMatchingProperties(rootProperties, matcher);
            }
            return rootProperties;
        }

        public boolean isBundled(){
            return matcher.isMatch();
        }

        public Map<String, PropertyState> getAllProperties(){
            return rootProperties;
        }

        public boolean hasChildNode(String relativePath){
            String key = concat(relativePath, DocumentBundlor.META_PROP_BUNDLING_PATH);
            return rootProperties.containsKey(key);
        }

        public boolean hasChildren(){
            return hasNonBundledChildren || hasBundledChildren;
        }

        public boolean hasOnlyBundledChildren(){
            return !hasNonBundledChildren;
        }

        public Set<String> getBundledChildNodeNames(){
            if (isBundled()) {
                return BundlorUtils.getChildNodeNames(rootProperties.keySet(), matcher);
            }
            return Collections.emptySet();
        }

        private boolean hasBundledChildren(Matcher matcher){
            if (isBundled()){
                return hasBundledProperty(rootProperties, matcher, DocumentBundlor.META_PROP_BUNDLED_CHILD);
            }
            return false;
        }

        private boolean hasNonBundledChildren(Matcher matcher){
            if (isBundled()){
                return hasBundledProperty(rootProperties, matcher, DocumentBundlor.META_PROP_NON_BUNDLED_CHILD);
            }
            return false;
        }

    }
}
