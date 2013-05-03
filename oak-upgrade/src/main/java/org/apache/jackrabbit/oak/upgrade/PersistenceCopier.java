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

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.api.Type.NAME;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jcr.NamespaceRegistry;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PersistenceManager;
import org.apache.jackrabbit.core.state.ChildNodeEntry;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NodeState;
import org.apache.jackrabbit.core.state.PropertyState;
import org.apache.jackrabbit.core.value.InternalValue;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.Path;

/**
 * Tool for copying item states from a Jackrabbit persistence manager to
 * an Oak node builder. Used for migrating repository content from Jackrabbit
 * to Oak.
 */
class PersistenceCopier {

    /**
     * Source persistence manager.
     */
    private final PersistenceManager source;

    /**
     * Source namespace registry.
     */
    private final NamespaceRegistry registry;

    /**
     * Target node store.
     */
    private final NodeStore store;

    /**
     * Identifiers of the nodes that have already been copied or that
     * should explicitly not be copied. Used to avoid duplicate copies
     * of shareable nodes and to avoid trying to copy "missing" nodes
     * like the virtual "/jcr:system" node.
     */
    private final Set<NodeId> exclude = new HashSet<NodeId>();

    public PersistenceCopier(
            PersistenceManager source, NamespaceRegistry registry,
            NodeStore store) {
        this.source = source;
        this.registry = registry;
        this.store = store;
    }

    private String getOakName(Name name) throws RepositoryException {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            return registry.getPrefix(uri) + ":" + local;
        }
    }

    private String getOakPath(Path path) throws RepositoryException {
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
                builder.append(getOakName(element.getName()));
                if (element.getIndex() >= Path.INDEX_DEFAULT) {
                    builder.append('[').append(element.getIndex()).append(']');
                }
            } else if (element.denotesParent()) {
                builder.append("..");
            } else if (element.denotesCurrent()) {
                builder.append('.');
            } else {
                throw new RepositoryException(
                        "Unknown path element: " + element);
            }
        }
        return builder.toString();
    }

    /**
     * Explicitly exclude the identified node from being copied. Used for
     * excluding virtual nodes like "/jcr:system" from the copy process.
     *
     * @param id identifier of the node to be excluded
     */
    public void excludeNode(NodeId id) {
        exclude.add(id);
    }

    /**
     * Recursively copies the identified node and all its descendants.
     * Explicitly excluded nodes and nodes that have already been copied
     * are automatically skipped.
     *
     * @param id identifier of the node to be copied
     * @throws RepositoryException if the copy operation fails
     */
    public void copy(NodeId id, NodeBuilder builder)
            throws RepositoryException, IOException {
        try {
            NodeState node = source.load(id);
            copy(node, builder);

            for (ChildNodeEntry entry : node.getChildNodeEntries()) {
                NodeId childId = entry.getId();
                if (!exclude.contains(childId)) {
                    exclude.add(childId);
                    String name = getOakName(entry.getName());
                    copy(childId, builder.child(name));
                    exclude.remove(childId);
                }
            }
        } catch (ItemStateException e) {
            throw new RepositoryException("Unable to copy " + id, e);
        }
    }

    /**
     * Copies the given node state and all associated property states
     * to the node builder.
     *
     * @param sourceNode source node state
     * @throws RepositoryException if the copy operation fails
     */
    private void copy(NodeState sourceNode, NodeBuilder builder)
            throws RepositoryException, IOException, ItemStateException {
        // Copy the node state
        String primary = getOakName(sourceNode.getNodeTypeName());
        builder.setProperty(JCR_PRIMARYTYPE, primary, NAME);

        Set<Name> mixinNames = sourceNode.getMixinTypeNames();
        if (!mixinNames.isEmpty()) {
            List<String> mixins = newArrayListWithCapacity(mixinNames.size());
            for (Name name : mixinNames) {
                mixins.add(getOakName(name));
            }
            builder.setProperty(JCR_MIXINTYPES, mixins, Type.NAMES);
        }

        // Copy all associated property states
        for (Name name : sourceNode.getPropertyNames()) {
            PropertyId id = new PropertyId(sourceNode.getNodeId(), name);
            PropertyState sourceState = source.load(id);

            InternalValue[] values = sourceState.getValues();
            int type = sourceState.getType();
            String oakName = getOakName(name);
            if (sourceState.isMultiValued() || values.length != 1) {
                builder.setProperty(getProperty(oakName, values, type));
            } else {
                builder.setProperty(getProperty(oakName, values[0], type));
            }
        }
    }

    private org.apache.jackrabbit.oak.api.PropertyState getProperty(
            String name, InternalValue[] values, int type)
            throws RepositoryException, IOException {
        switch (type) {
        case PropertyType.BINARY:
            List<Blob> binaries = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                binaries.add(store.createBlob(value.getStream()));
            }
            return PropertyStates.createProperty(name, binaries, Type.BINARIES);
        case PropertyType.BOOLEAN:
            List<Boolean> booleans = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                booleans.add(value.getBoolean());
            }
            return PropertyStates.createProperty(name, booleans, Type.BOOLEANS);
        case PropertyType.DATE:
            List<Long> dates = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                dates.add(value.getCalendar().getTimeInMillis());
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
                names.add(getOakName(value.getName()));
            }
            return PropertyStates.createProperty(name, names, Type.NAMES);
        case PropertyType.PATH:
            List<String> paths = newArrayListWithCapacity(values.length);
            for (InternalValue value : values) {
                paths.add(getOakPath(value.getPath()));
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

    private org.apache.jackrabbit.oak.api.PropertyState getProperty(
            String name, InternalValue value, int type)
            throws RepositoryException, IOException {
        switch (type) {
        case PropertyType.BINARY:
            return PropertyStates.createProperty(
                    name, store.createBlob(value.getStream()), Type.BINARY);
        case PropertyType.BOOLEAN:
            return PropertyStates.createProperty(
                    name, value.getBoolean(), Type.BOOLEAN);
        case PropertyType.DATE:
            return PropertyStates.createProperty(
                    name, value.getCalendar().getTimeInMillis(), Type.DATE);
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
                    name, getOakName(value.getName()), Type.NAME);
        case PropertyType.PATH:
            return PropertyStates.createProperty(
                    name, getOakPath(value.getPath()), Type.PATH);
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

}
