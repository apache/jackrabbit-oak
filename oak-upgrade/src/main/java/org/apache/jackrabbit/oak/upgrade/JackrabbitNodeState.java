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

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;

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
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.Path;
import org.apache.jackrabbit.util.ISO8601;

class JackrabbitNodeState extends AbstractNodeState {

    /**
     * Source persistence manager.
     */
    private final PersistenceManager source;

    /**
     * Source namespace registry.
     */
    private final NamespaceRegistry registry;

    private final NodeId id;

    private NodeState state = null;

    JackrabbitNodeState(
            PersistenceManager source, NamespaceRegistry registry,
            NodeId id) {
        this.source = source;
        this.registry = registry;
        this.id = id;
    }

    private NodeState getState() {
        if (state == null) {
            try {
                state = source.load(id);
            } catch (ItemStateException e) {
                throw new IllegalStateException(e);
            }
        }
        return state;
    }

    //---------------------------------------------------------< NodeState >--

    @Override
    public boolean exists() {
        return true;
    }

    @Override
    public Iterable<org.apache.jackrabbit.oak.api.PropertyState> getProperties() {
        List<org.apache.jackrabbit.oak.api.PropertyState> properties = newArrayList();
        for (Name name : getState().getPropertyNames()) {
            try {
                PropertyState property = source.load(new PropertyId(id, name));
                int type = property.getType();
                if (property.isMultiValued()) {
                    properties.add(createProperty(
                            createName(name), type, property.getValues()));
                } else {
                    properties.add(createProperty(
                            createName(name), type, property.getValues()[0]));
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return properties;
    }

    @Override
    public org.apache.jackrabbit.oak.spi.state.NodeState getChildNode(String name) {
        for (MemoryChildNodeEntry entry : getChildNodeEntries()) {
            if (name.equals(entry.getName())) {
                return entry.getNodeState();
            }
        }
        return EmptyNodeState.MISSING_NODE;
    }

    @Override
    public Iterable<MemoryChildNodeEntry> getChildNodeEntries() {
        List<MemoryChildNodeEntry> entries = newArrayList();
        for (ChildNodeEntry entry : getState().getChildNodeEntries()) {
            try {
                String name = createName(entry.getName());
                int index = entry.getIndex();
                if (index > 1) {
                    name = name + '[' + index + ']';
                }

                JackrabbitNodeState state = new JackrabbitNodeState(
                        source, registry, entry.getId());

                entries.add(new MemoryChildNodeEntry(name, state));
            } catch (RepositoryException e) {
                throw new IllegalStateException(e);
            }
        }
        return entries;
    }

    @Override
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    //-----------------------------------------------------------< private >--

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
                    name, ISO8601.format(value.getCalendar()), Type.DATE);
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
                dates.add(ISO8601.format(value.getCalendar()));
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
                    throw new IllegalStateException(e);
                }
            }
            @Override
            public InputStream getNewStream() {
                try {
                    return value.getStream();
                } catch (RepositoryException e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    private String createName(Name name) throws RepositoryException {
        String uri = name.getNamespaceURI();
        String local = name.getLocalName();
        if (uri == null || uri.isEmpty()) {
            return local;
        } else {
            return registry.getPrefix(uri) + ":" + local;
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
                throw new RepositoryException(
                        "Unknown path element: " + element);
            }
        }
        return builder.toString();
    }

}
