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
package org.apache.jackrabbit.oak.upgrade;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENMIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENPRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENUUID;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONHISTORY;
import static org.apache.jackrabbit.core.RepositoryImpl.ACTIVITIES_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.ROOT_NODE_ID;
import static org.apache.jackrabbit.core.RepositoryImpl.VERSION_STORAGE_NODE_ID;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.core.RepositoryContext;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle.ChildNodeEntry;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle.PropertyEntry;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.value.InternalValue;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JackrabbitNodeState extends AbstractNodeState {

    private static final Logger log =
            LoggerFactory.getLogger(JackrabbitNodeState.class);

    private JackrabbitNodeState parent;

    private final String name;

    private String path;

    /**
     * Bundle loader based on the source persistence manager.
     */
    private final BundleLoader loader;

    /**
     * Workspace name used for versionable paths. This is null
     * for the jcr:versionStorage and jcr:activities nodes.
     */
    private final String workspaceName;

    private final TypePredicate isReferenceable;

    private final TypePredicate isOrderable;

    private final TypePredicate isVersionable;

    private final TypePredicate isVersionHistory;

    private final TypePredicate isFrozenNode;

    private final boolean skipOnError;

    /**
     * Source namespace mappings (URI -&lt; prefix).
     */
    private final Map<String, String> uriToPrefix;

    private final boolean useBinaryReferences;

    private final Map<String, NodeId> nodes;

    private final Map<String, PropertyState> properties;

    private final Map<NodeId, JackrabbitNodeState> mountPoints;

    private final Map<NodeId, JackrabbitNodeState> nodeStateCache;

    private final List<String> ignoredPaths = ImmutableList.of("/jcr:system/jcr:nodeTypes");

    public static JackrabbitNodeState createRootNodeState(
            RepositoryContext context,
            String workspaceName,
            NodeState root,
            Map<String, String> uriToPrefix,
            boolean copyBinariesByReference,
            boolean skipOnError
    ) throws RepositoryException {

        final Map<NodeId, JackrabbitNodeState> emptyMountPoints = ImmutableMap.of();
        final PersistenceManager versionPM = context.getInternalVersionManager().getPersistenceManager();
        final JackrabbitNodeState versionStorage = new JackrabbitNodeState(
                versionPM, root, uriToPrefix,
                VERSION_STORAGE_NODE_ID, "/jcr:system/jcr:versionStorage",
                null,
                emptyMountPoints,
                copyBinariesByReference,
                skipOnError
        );

        final JackrabbitNodeState activities = new JackrabbitNodeState(
                versionPM, root, uriToPrefix,
                ACTIVITIES_NODE_ID, "/jcr:system/jcr:activities",
                null,
                emptyMountPoints,
                copyBinariesByReference,
                skipOnError
        );


        PersistenceManager pm = context.getWorkspaceInfo(workspaceName).getPersistenceManager();
        final Map<NodeId, JackrabbitNodeState> mountPoints = ImmutableMap.of(
                VERSION_STORAGE_NODE_ID, versionStorage,
                ACTIVITIES_NODE_ID, activities
        );
        return new JackrabbitNodeState(
                pm, root, uriToPrefix, ROOT_NODE_ID, "/",
                workspaceName, mountPoints, copyBinariesByReference, skipOnError);
    }

    private JackrabbitNodeState(
            JackrabbitNodeState parent, String name, NodePropBundle bundle) {
        this.parent = parent;
        this.name = name;
        this.path = null;
        this.loader = parent.loader;
        this.workspaceName = parent.workspaceName;
        this.isReferenceable = parent.isReferenceable;
        this.isOrderable = parent.isOrderable;
        this.isVersionable = parent.isVersionable;
        this.isVersionHistory = parent.isVersionHistory;
        this.isFrozenNode = parent.isFrozenNode;
        this.uriToPrefix = parent.uriToPrefix;
        this.useBinaryReferences = parent.useBinaryReferences;
        this.properties = createProperties(bundle);
        this.nodes = createNodes(bundle);
        this.skipOnError = parent.skipOnError;
        this.mountPoints = parent.mountPoints;
        this.nodeStateCache = parent.nodeStateCache;
        setChildOrder();
        fixFrozenUuid();
    }

    JackrabbitNodeState(
            PersistenceManager source, NodeState root,
            Map<String, String> uriToPrefix, NodeId id, String path,
            String workspaceName,
            Map<NodeId, JackrabbitNodeState> mountPoints,
            boolean useBinaryReferences, boolean skipOnError) {
        this.parent = null;
        this.name = PathUtils.getName(path);
        this.path = path;
        this.loader = new BundleLoader(source);
        this.workspaceName = workspaceName;
        this.isReferenceable = new TypePredicate(root, MIX_REFERENCEABLE);
        this.isOrderable = TypePredicate.isOrderable(root);
        this.isVersionable = new TypePredicate(root, MIX_VERSIONABLE);
        this.isVersionHistory = new TypePredicate(root, NT_VERSIONHISTORY);
        this.isFrozenNode = new TypePredicate(root, NT_FROZENNODE);
        this.uriToPrefix = uriToPrefix;
        this.mountPoints = mountPoints;
        final int cacheSize = 50; // cache size 50 results in > 25% cache hits during version copy
        this.nodeStateCache = new LinkedHashMap<NodeId, JackrabbitNodeState>(cacheSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<NodeId, JackrabbitNodeState> eldest) {
                return size() >= cacheSize;
            }
        };
        this.useBinaryReferences = useBinaryReferences;
        this.skipOnError = skipOnError;
        try {
            NodePropBundle bundle = loader.loadBundle(id);
            this.properties = createProperties(bundle);
            this.nodes = createNodes(bundle);
            setChildOrder();
        } catch (ItemStateException e) {
            throw new IllegalStateException("Unable to access node " + id, e);
        }
    }

    @Override
    public String toString() {
        return getPath();
    }

    //---------------------------------------------------------< NodeState >--

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        return properties.size();
    }

    @Override
    public boolean hasProperty(@Nonnull String name) {
        return properties.containsKey(name);
    }

    @Override
    public PropertyState getProperty(@Nonnull String name) {
        return properties.get(name);
    }

    @Nonnull
    @Override
    public Iterable<PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public long getChildNodeCount(long max) {
        return nodes.size();
    }

    @Override
    public boolean hasChildNode(@Nonnull String name) {
        return nodes.containsKey(name);
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        NodeId id = nodes.get(name);
        NodeState state = null;
        if (id != null) {
            state = createChildNodeState(id, name);
        }
        checkValidName(name);
        return state != null ? state : EmptyNodeState.MISSING_NODE;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return nodes.keySet();
    }

    @Nonnull
    @Override
    public Iterable<MemoryChildNodeEntry> getChildNodeEntries() {
        List<MemoryChildNodeEntry> entries = newArrayListWithCapacity(nodes.size());
        for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
            String name = entry.getKey();
            final NodeState child = createChildNodeState(entry.getValue(), name);
            if (child != null) {
                entries.add(new MemoryChildNodeEntry(name, child));
            }
        }
        return entries;
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    //-----------------------------------------------------------< private >--

    @CheckForNull
    private JackrabbitNodeState createChildNodeState(NodeId id, String name) {
        if (mountPoints.containsKey(id)) {
            final JackrabbitNodeState nodeState = mountPoints.get(id);
            checkState(name.equals(nodeState.name),
                    "Expected mounted node " + id + " to be called " + nodeState.name +
                            " instead of " + name);
            nodeState.parent = this;
            return nodeState;
        }

        JackrabbitNodeState state = nodeStateCache.get(id);
        if (state == null) {
            try {
                state = new JackrabbitNodeState(this, name, loader.loadBundle(id));
                nodeStateCache.put(id, state);
            } catch (ItemStateException e) {
                handleBundleLoadingException(name, e);
            } catch (NullPointerException e) {
                handleBundleLoadingException(name, e);
            }
        }
        return state;
    }

    private void handleBundleLoadingException(final @Nonnull String name, final Exception e) {
        if (!skipOnError) {
            throw new IllegalStateException("Unable to access child node " + name + " of " + getPath(), e);
        }
        warn("Skipping broken child node entry " + name + " and changing the primary type to nt:unstructured", e);
        properties.put(JCR_PRIMARYTYPE, PropertyStates.createProperty(
                JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME));
    }

    private void setChildOrder() {
        if (isOrderable.apply(this)) {
            properties.put(OAK_CHILD_ORDER, PropertyStates.createProperty(
                    OAK_CHILD_ORDER, nodes.keySet(), Type.NAMES));
        }
    }

    private Map<String, NodeId> createNodes(NodePropBundle bundle) {
        Map<String, NodeId> children = newLinkedHashMap();
        for (ChildNodeEntry entry : bundle.getChildNodeEntries()) {
            String base = createName(entry.getName());
            String name = base;
            for (int i = 2; children.containsKey(name); i++) {
                name = base + '[' + i + ']';
            }

            if (!ignoredPaths.contains(PathUtils.concat(getPath(), name))) {
                children.put(name, entry.getId());
            }
        }
        return children;
    }

    private Map<String, PropertyState> createProperties(NodePropBundle bundle) {
        Map<String, PropertyState> properties = newHashMap();

        String primary;
        if (bundle.getNodeTypeName() != null) {
            primary = createName(bundle.getNodeTypeName());
        } else {
            warn("Missing primary node type; defaulting to nt:unstructured");
            primary = NT_UNSTRUCTURED;
        }
        properties.put(JCR_PRIMARYTYPE, PropertyStates.createProperty(
                JCR_PRIMARYTYPE, primary, Type.NAME));

        for (PropertyEntry property : bundle.getPropertyEntries()) {
            String name = createName(property.getName());
            try {
                int type = property.getType();
                if (property.isMultiValued()) {
                    properties.put(name, createProperty(
                            name, type, property.getValues()));
                } else {
                    properties.put(name, createProperty(
                            name, type, property.getValues()[0]));
                }
            } catch (Exception e) {
                warn("Skipping broken property entry " + name, e);
            }
        }

        Set<String> mixins = newLinkedHashSet();
        if (bundle.getMixinTypeNames() != null) {
            for (Name mixin : bundle.getMixinTypeNames()) {
                mixins.add(createName(mixin));
            }
        }

        if (mixins.remove("mix:simpleVersionable")) {
            mixins.add(MIX_VERSIONABLE);
        }

        if (!mixins.isEmpty()) {
            properties.put(JCR_MIXINTYPES, PropertyStates.createProperty(
                    JCR_MIXINTYPES, mixins, Type.NAMES));
        }

        if (bundle.isReferenceable()
                || isReferenceable.apply(primary, mixins)) {
            properties.put(JCR_UUID, PropertyStates.createProperty(
                    JCR_UUID, bundle.getId().toString()));
        }

        return properties;
    }

    private void fixFrozenUuid() {
        // OAK-1789: Convert the jcr:frozenUuid of a non-referenceable
        // frozen node from UUID to a path identifier
        PropertyState frozenUuid = properties.get(JCR_FROZENUUID);
        if (frozenUuid != null
                && frozenUuid.getType() == STRING
                && isFrozenNode.apply(this)) {
            String frozenPrimary = NT_BASE;
            Set<String> frozenMixins = newHashSet();

            PropertyState property = properties.get(JCR_FROZENPRIMARYTYPE);
            if (property != null && property.getType() == NAME) {
                frozenPrimary = property.getValue(NAME);
            }
            property = properties.get(JCR_FROZENMIXINTYPES);
            if (property != null && property.getType() == NAMES) {
                addAll(frozenMixins, property.getValue(NAMES));
            }

            if (!isReferenceable.apply(frozenPrimary, frozenMixins)) {
                String parentFrozenUuid = parent.getString(JCR_FROZENUUID);
                if (parentFrozenUuid != null) {
                    frozenUuid = PropertyStates.createProperty(
                            JCR_FROZENUUID, parentFrozenUuid + "/" + name);
                    properties.put(JCR_FROZENUUID, frozenUuid);
                }
            }
        }
    }

    private org.apache.jackrabbit.oak.api.PropertyState createProperty(
            String name, int type, InternalValue value)
            throws RepositoryException, IOException {
        switch (type) {
        case PropertyType.BINARY:
            return PropertyStates.createProperty(
                    name, createBlob(value), Type.BINARY);
        case PropertyType.BOOLEAN:
            return PropertyStates.createProperty(
                    name, value.getBoolean(), Type.BOOLEAN);
        case PropertyType.DATE:
            return PropertyStates.createProperty(
                    name, value.getString(), Type.DATE);
        case PropertyType.DECIMAL:
            return PropertyStates.createProperty(
                    name, value.getDecimal(), Type.DECIMAL);
        case PropertyType.DOUBLE:
            return PropertyStates.createProperty(
                    name, value.getDouble(), Type.DOUBLE);
        case PropertyType.LONG:
            return PropertyStates.createProperty(
                    name, value.getLong(), Type.LONG);
        case PropertyType.NAME:
            return PropertyStates.createProperty(
                    name, createName(value.getName()), Type.NAME);
        case PropertyType.PATH:
            return PropertyStates.createProperty(
                    name, createPath(value.getPath()), Type.PATH);
        case PropertyType.REFERENCE:
            return PropertyStates.createProperty(
                    name, value.getNodeId().toString(), Type.REFERENCE);
        case PropertyType.STRING:
            return PropertyStates.createProperty(
                    name, value.getString(), Type.STRING);
        case PropertyType.URI:
            return PropertyStates.createProperty(
                    name, value.getURI().toString(), Type.URI);
        case PropertyType.WEAKREFERENCE:
            return PropertyStates.createProperty(
                    name, value.getNodeId().toString(), Type.WEAKREFERENCE);
        default:
            throw new RepositoryException("Unknown value type: " + type);
        }
    }

    private org.apache.jackrabbit.oak.api.PropertyState createProperty(
            String name, int type, InternalValue[] values)
            throws RepositoryException, IOException {
        switch (type) {
        case PropertyType.BINARY:
            List<Blob> binaries = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                binaries.add(createBlob(value));
            }
            return PropertyStates.createProperty(name, binaries, Type.BINARIES);
        case PropertyType.BOOLEAN:
            List<Boolean> booleans = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                booleans.add(value.getBoolean());
            }
            return PropertyStates.createProperty(name, booleans, Type.BOOLEANS);
        case PropertyType.DATE:
            List<String> dates = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                dates.add(value.getString());
            }
            return PropertyStates.createProperty(name, dates, Type.DATES);
        case PropertyType.DECIMAL:
            List<BigDecimal> decimals = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                decimals.add(value.getDecimal());
            }
            return PropertyStates.createProperty(name, decimals, Type.DECIMALS);
        case PropertyType.DOUBLE:
            List<Double> doubles = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                doubles.add(value.getDouble());
            }
            return PropertyStates.createProperty(name, doubles, Type.DOUBLES);
        case PropertyType.LONG:
            List<Long> longs = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                longs.add(value.getLong());
            }
            return PropertyStates.createProperty(name, longs, Type.LONGS);
        case PropertyType.NAME:
            List<String> names = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                names.add(createName(value.getName()));
            }
            return PropertyStates.createProperty(name, names, Type.NAMES);
        case PropertyType.PATH:
            List<String> paths = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                paths.add(createPath(value.getPath()));
            }
            return PropertyStates.createProperty(name, paths, Type.PATHS);
        case PropertyType.REFERENCE:
            List<String> references = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                references.add(value.getNodeId().toString());
            }
            return PropertyStates.createProperty(name, references, Type.REFERENCES);
        case PropertyType.STRING:
            List<String> strings = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                strings.add(value.getString());
            }
            return PropertyStates.createProperty(name, strings, Type.STRINGS);
        case PropertyType.URI:
            List<String> uris = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                uris.add(value.getURI().toString());
            }
            return PropertyStates.createProperty(name, uris, Type.URIS);
        case PropertyType.WEAKREFERENCE:
            List<String> weakreferences = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                weakreferences.add(value.getNodeId().toString());
            }
            return PropertyStates.createProperty(name, weakreferences, Type.WEAKREFERENCES);
        default:
            throw new RepositoryException("Unknown value type: " + type);
        }
    }

    private Blob createBlob(final InternalValue value) {
        checkArgument(checkNotNull(value).getType() == PropertyType.BINARY);
        return new AbstractBlob() {
            @Override
            public long length() {
                try {
                    return value.getLength();
                } catch (RepositoryException e) {
                    warn("Unable to access blob length", e);
                    return 0;
                }
            }

            @Nonnull
            @Override
            public InputStream getNewStream() {
                try {
                    return value.getStream();
                } catch (RepositoryException e) {
                    warn("Unable to access blob contents", e);
                    return new ByteArrayInputStream(new byte[0]);
                }
            }

            @Override
            public String getReference() {
                if (!useBinaryReferences) {
                    return null;
                }
                try {
                    Binary binary = value.getBinary();
                    try {
                        if (binary instanceof ReferenceBinary) {
                            return ((ReferenceBinary) binary).getReference();
                        } else {
                            return null;
                        }
                    } finally {
                        binary.dispose();
                    }
                } catch (RepositoryException e) {
                    warn("Unable to get blob reference", e);
                    return null;
                }
            }

            @Override
            public String getContentIdentity() {
                final String reference = getReference();
                if (reference != null) {
                    final int pos = reference.indexOf(":");
                    final String blobHash;
                    if (pos > -1) {
                        blobHash = reference.substring(0, pos);
                    } else {
                        blobHash = reference;
                    }
                    return blobHash + "#" + length();
                }
                return super.getContentIdentity();
            }
        };
    }

    private String createName(Name name) {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            String prefix = uriToPrefix.get(uri);
            if (prefix != null) {
                return prefix + ":" + local;
            } else {
                warn("No prefix mapping found for " + name);
                return "{" + uri + "}" + local;
            }
        }
    }

    private String createPath(Path path) throws RepositoryException {
        StringBuilder builder = new StringBuilder();
        for (Path.Element element : path.getElements()) {
            if (builder.length() > 1
                    || (builder.length() == 1 && !"/".equals(builder.toString()))) {
                builder.append('/');
            }
            if (element.denotesRoot()) {
                builder.append('/');
            } else if (element.denotesIdentifier()) {
                builder.append('[').append(element.getIdentifier()).append(']');
            } else if (element.denotesName()) {
                builder.append(createName(element.getName()));
                if (element.getIndex() >= Path.INDEX_DEFAULT) {
                    builder.append('[').append(element.getIndex()).append(']');
                }
            } else if (element.denotesParent()) {
                builder.append("..");
            } else if (element.denotesCurrent()) {
                builder.append('.');
            } else {
                warn("Unknown element in path: " + path);
                builder.append(element.getString());
            }
        }
        return builder.toString();
    }

    private String getPath() {
        if (path == null) { // implies: parent != null && name != null
            path = PathUtils.concat(parent.getPath(), name);
        }
        return path;
    }

    private void warn(String message) {
        log.warn(getPath() + ": " + message);
    }

    private void warn(String message, Throwable cause) {
        log.warn(getPath() + ": " + message, cause);
    }

}
