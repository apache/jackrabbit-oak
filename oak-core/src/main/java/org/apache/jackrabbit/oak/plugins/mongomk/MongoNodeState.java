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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.StringCache;
import org.apache.jackrabbit.oak.kernel.TypeCodes;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * A {@link NodeState} implementation for the {@link MongoNodeStore}.
 * TODO: merge MongoNodeState with Node
 */
final class MongoNodeState extends AbstractNodeState {

    private final MongoNodeStore store;

    private final Node node;

    /**
     * TODO: OAK-1056
     */
    private boolean isBranch;

    MongoNodeState(@Nonnull MongoNodeStore store,
                   @Nonnull Node node) {
        this.store = checkNotNull(store);
        this.node = checkNotNull(node);
    }

    String getPath() {
        return node.getPath();
    }

    Revision getRevision() {
        return node.getReadRevision();
    }

    MongoNodeState setBranch() {
        isBranch = true;
        return this;
    }

    boolean isBranch() {
        return isBranch;
    }

    //--------------------------< NodeState >-----------------------------------

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof MongoNodeState) {
            MongoNodeState other = (MongoNodeState) that;
            if (getPath().equals(other.getPath())) {
                return node.getLastRevision().equals(other.node.getLastRevision());
            }
        } else if (that instanceof ModifiedNodeState) {
            ModifiedNodeState modified = (ModifiedNodeState) that;
            if (modified.getBaseState() == this) {
                return false;
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
        String value = node.getProperty(name);
        if (value == null) {
            return null;
        }
        JsopReader reader = new JsopTokenizer(value);
        if (reader.matches('[')) {
            return readArrayProperty(name, reader);
        } else {
            return readProperty(name, reader);
        }
    }

    @Override
    public boolean hasProperty(String name) {
        return node.getPropertyNames().contains(name);
    }

    @Nonnull
    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(node.getPropertyNames(), new Function<String, PropertyState>() {
            @Override
            public PropertyState apply(String name) {
                return getProperty(name);
            }
        });
    }

    @Nonnull
    @Override
    public NodeState getChildNode(@Nonnull String name) {
        if (node.hasNoChildren()) {
            return EmptyNodeState.MISSING_NODE;
        }
        String p = PathUtils.concat(getPath(), name);
        Node child = store.getNode(p, node.getLastRevision());
        if (child == null) {
            return EmptyNodeState.MISSING_NODE;
        } else {
            return new MongoNodeState(store, child);
        }
    }

    @Nonnull
    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        if (node.hasNoChildren()) {
            return Collections.emptyList();
        }
        // TODO: handle many child nodes better
        Node.Children children = store.getChildren(getPath(),
                node.getLastRevision(), 100);
        if (children.hasMore) {
            children = store.getChildren(getPath(),
                    node.getLastRevision(), Integer.MAX_VALUE);
        }
        return Iterables.transform(children.children, new Function<String, ChildNodeEntry>() {
            @Override
            public ChildNodeEntry apply(String path) {
                final String name = PathUtils.getName(path);
                return new AbstractChildNodeEntry() {
                    @Nonnull
                    @Override
                    public String getName() {
                        return name;
                    }

                    @Nonnull
                    @Override
                    public NodeState getNodeState() {
                        return getChildNode(name);
                    }
                };
            }
        });
    }

    @Nonnull
    @Override
    public NodeBuilder builder() {
        if (isBranch) {
            return new MemoryNodeBuilder(this);
        } else if ("/".equals(getPath())) {
            return new MongoRootBuilder(this, store);
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
        } else if (base instanceof MongoNodeState) {
            MongoNodeState mBase = (MongoNodeState) base;
            if (store == mBase.store) {
                if (node.getLastRevision().equals(mBase.node.getLastRevision())
                        && getPath().equals(mBase.getPath())) {
                    // no differences
                    return true; 
                }
                // TODO: use diff, similar to KernelNodeState
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
    }

    //------------------------------< internal >--------------------------------

    /**
     * FIXME: copied from KernelNodeState.
     *
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    PropertyState readProperty(String name, JsopReader reader) {
        return readProperty(name, store, reader);
    }
    
    /**
     * FIXME: copied from KernelNodeState.
     *
     * Read a {@code PropertyState} from a {@link JsopReader}.
     * 
     * @param name the name of the property state
     * @param store the store 
     * @param reader the reader
     * @return new property state
     */    
    public static PropertyState readProperty(String name, MongoNodeStore store, JsopReader reader) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            try {
                return new LongPropertyState(name, Long.parseLong(number));
            } catch (NumberFormatException e) {
                return new DoublePropertyState(name, Double.parseDouble(number));
            }
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            if (jsonString.startsWith(TypeCodes.EMPTY_ARRAY)) {
                int type = PropertyType.valueFromName(jsonString.substring(TypeCodes.EMPTY_ARRAY.length()));
                return PropertyStates.createProperty(name, emptyList(), Type.fromTag(type, true));
            }
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    return  BinaryPropertyState.binaryProperty(name, store.getBlob(value));
                } else {
                    return createProperty(name, StringCache.get(value), type);
                }
            } else {
                return StringPropertyState.stringProperty(name, StringCache.get(jsonString));
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * FIXME: copied from KernelNodeState.
     *
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}.
     * 
     * @param name the name of the property state
     * @param reader the reader
     * @return new property state
     */
    PropertyState readArrayProperty(String name, JsopReader reader) {
        return readArrayProperty(name, store, reader);
    }
    
    /**
     * FIXME: copied from KernelNodeState.
     *
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}.
     * 
     * @param name the name of the property state
     * @param store the store 
     * @param reader the reader
     * @return new property state
     */
    public static PropertyState readArrayProperty(String name, MongoNodeStore store, JsopReader reader) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                try {
                    type = PropertyType.LONG;
                    values.add(Long.parseLong(number));
                } catch (NumberFormatException e) {
                    type = PropertyType.DOUBLE;
                    values.add(Double.parseDouble(number));
                }
            } else if (reader.matches(JsopReader.TRUE)) {
                type = PropertyType.BOOLEAN;
                values.add(true);
            } else if (reader.matches(JsopReader.FALSE)) {
                type = PropertyType.BOOLEAN;
                values.add(false);
            } else if (reader.matches(JsopReader.STRING)) {
                String jsonString = reader.getToken();
                int split = TypeCodes.split(jsonString);
                if (split != -1) {
                    type = TypeCodes.decodeType(split, jsonString);
                    String value = TypeCodes.decodeName(split, jsonString);
                    if (type == PropertyType.BINARY) {
                        values.add(store.getBlob(value));
                    } else if (type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if (type == PropertyType.DECIMAL) {
                        values.add(Conversions.convert(value).toDecimal());
                    } else {
                        values.add(StringCache.get(value));
                    }
                } else {
                    type = PropertyType.STRING;
                    values.add(StringCache.get(jsonString));
                }
            } else {
                throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
            }
            reader.matches(',');
        }
        return createProperty(name, values, Type.fromTag(type, true));
    }
}
