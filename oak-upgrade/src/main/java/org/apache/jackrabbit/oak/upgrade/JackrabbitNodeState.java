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
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_UUID;
import static org.apache.jackrabbit.JcrConstants.MIX_REFERENCEABLE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jcr.Binary;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.ReferenceBinary;
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

    /**
     * Bundle loader based on the source persistence manager.
     */
    private final BundleLoader loader;

    private final TypePredicate isReferenceable;

    private final TypePredicate isOrderable;

    /**
     * Source namespace mappings (URI -&lt; prefix).
     */
    private final Map<String, String> uriToPrefix;

    private final Map<String, NodeId> nodes;

    private final Map<String, PropertyState> properties;

    private final boolean useBinaryReferences;

    private JackrabbitNodeState(
            JackrabbitNodeState parent, NodePropBundle bundle) {
        this.loader = parent.loader;
        this.isReferenceable = parent.isReferenceable;
        this.isOrderable = parent.isOrderable;
        this.uriToPrefix = parent.uriToPrefix;
        this.nodes = createNodes(bundle);
        this.properties = createProperties(bundle);
        this.useBinaryReferences = parent.useBinaryReferences;
    }

    JackrabbitNodeState(
            PersistenceManager source, NodeState root,
            Map<String, String> uriToPrefix, NodeId id,
            boolean useBinaryReferences) {
        this.loader = new BundleLoader(source);
        this.isReferenceable = new TypePredicate(root, MIX_REFERENCEABLE);
        this.isOrderable = TypePredicate.isOrderable(root);
        this.uriToPrefix = uriToPrefix;
        try {
            NodePropBundle bundle = loader.loadBundle(id);
            this.nodes = createNodes(bundle);
            this.properties = createProperties(bundle);
        } catch (ItemStateException e) {
            throw new IllegalStateException("Unable to access node " + id, e);
        }
        this.useBinaryReferences = useBinaryReferences;
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
    public boolean hasProperty(String name) {
        return properties.containsKey(name);
    }

    @Override
    public PropertyState getProperty(String name) {
        return properties.get(name);
    }

    @Override
    public Iterable<PropertyState> getProperties() {
        return properties.values();
    }

    @Override
    public long getChildNodeCount(long max) {
        return nodes.size();
    }

    @Override
    public boolean hasChildNode(String name) {
        return nodes.containsKey(name);
    }

    @Override
    public NodeState getChildNode(String name) {
        NodeId id = nodes.get(name);
        if (id != null) {
            try {
                return new JackrabbitNodeState(this, loader.loadBundle(id));
            } catch (ItemStateException e) {
                throw new IllegalStateException(
                        "Unable to access child node " + name, e);
            }
        }
        checkValidName(name);
        return EmptyNodeState.MISSING_NODE;
    }

    @Override
    public Iterable<String> getChildNodeNames() {
        return nodes.keySet();
    }

    @Override
    public Iterable<MemoryChildNodeEntry> getChildNodeEntries() {
        List<MemoryChildNodeEntry> entries = newArrayList();
        for (Map.Entry<String, NodeId> entry : nodes.entrySet()) {
            String name = entry.getKey();
            try {
                JackrabbitNodeState child = new JackrabbitNodeState(
                        this, loader.loadBundle(entry.getValue()));
                entries.add(new MemoryChildNodeEntry(name, child));
            } catch (ItemStateException e) {
                warn("Skipping broken child node entry " + name, e);
            }
        }
        return entries;
    }

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    //-----------------------------------------------------------< private >--

    private Map<String, NodeId> createNodes(NodePropBundle bundle) {
        Map<String, NodeId> children = newLinkedHashMap();
        for (ChildNodeEntry entry : bundle.getChildNodeEntries()) {
            String base = createName(entry.getName());
            String name = base;
            for (int i = 2; children.containsKey(name); i++) {
                name = base + '[' + i + ']';
            }
            children.put(name, entry.getId());
        }
        return children;
    }

    public Map<String, PropertyState> createProperties(NodePropBundle bundle) {
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

        Set<String> mixins = newLinkedHashSet();
        if (bundle.getMixinTypeNames() != null) {
            for (Name mixin : bundle.getMixinTypeNames()) {
                mixins.add(createName(mixin));
            }
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

        if (isOrderable.apply(primary, mixins)) {
            properties.put(OAK_CHILD_ORDER, PropertyStates.createProperty(
                    OAK_CHILD_ORDER, nodes.keySet(), Type.NAMES));
        }

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

        return properties;
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

    private void warn(String message) {
        log.warn(message);
    }

    private void warn(String message, Throwable cause) {
        log.warn(message, cause);
    }

}
