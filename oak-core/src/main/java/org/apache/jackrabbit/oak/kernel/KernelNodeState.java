/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Basic {@link NodeState} implementation based on the {@link MicroKernel}
 * interface. This class makes an attempt to load data lazily.
 */
public final class KernelNodeState extends AbstractNodeState {

    /**
     * Maximum number of child nodes kept in memory.
     */
    static final int MAX_CHILD_NODE_NAMES = 10000;

    private final MicroKernel kernel;

    private final String path;

    private final String revision;

    private Map<String, PropertyState> properties;

    private long childNodeCount = -1;

    private String hash;

    private Map<String, String> childPaths;

    private final LoadingCache<String, KernelNodeState> cache;

    /**
     * Create a new instance of this class representing the node at the
     * given {@code path} and {@code revision}. It is an error if the
     * underlying Microkernel does not contain such a node.
     *
     * @param kernel the underlying MicroKernel
     * @param path the path of this KernelNodeState
     * @param revision the revision of the node to read from the kernel.
     * @param cache the KernelNodeState cache
     */
    public KernelNodeState(
            MicroKernel kernel, String path, String revision,
            LoadingCache<String, KernelNodeState> cache) {
        this.kernel = checkNotNull(kernel);
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
        this.cache = checkNotNull(cache);
    }

    private synchronized void init() {
        if (properties == null) {
            String json = kernel.getNodes(
                    path, revision, 0, 0, MAX_CHILD_NODE_NAMES,
                    "{\"properties\":[\"*\",\":hash\"]}");

            JsopReader reader = new JsopTokenizer(json);
            reader.read('{');
            properties = new LinkedHashMap<String, PropertyState>();
            childPaths = new LinkedHashMap<String, String>();
            do {
                String name = StringCache.get(reader.readString());
                reader.read(':');
                if (":childNodeCount".equals(name)) {
                    childNodeCount =
                            Long.valueOf(reader.read(JsopReader.NUMBER));
                } else if (":hash".equals(name)) {
                    hash = new String(reader.read(JsopReader.STRING));
                } else if (reader.matches('{')) {
                    reader.read('}');
                    String childPath = path + '/' + name;
                    if ("/".equals(path)) {
                        childPath = '/' + name;
                    }
                    childPaths.put(name, childPath);
                } else if (reader.matches('[')) {
                    properties.put(name, readArrayProperty(name, reader));
                } else {
                    properties.put(name, readProperty(name, reader));
                }
            } while (reader.matches(','));
            reader.read('}');
            reader.read(JsopReader.END);
            // optimize for empty childNodes
            if (childPaths.isEmpty()) {
                childPaths = Collections.emptyMap();
            }
        }
    }

    @Override
    public long getPropertyCount() {
        init();
        return properties.size();
    }

    @Override
    public PropertyState getProperty(String name) {
        init();
        return properties.get(name);
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        init();
        return properties.values();
    }

    @Override
    public long getChildNodeCount() {
        init();
        return childNodeCount;
    }

    @Override
    public NodeState getChildNode(String name) {
        init();
        String childPath = childPaths.get(name);
        if (childPath == null && childNodeCount > MAX_CHILD_NODE_NAMES) {
            String path = getChildPath(name);
            // OAK-506: Avoid the nodeExists() call when already cached
            NodeState state = cache.getIfPresent(revision + path);
            if (state != null) {
                return state;
            } else if (kernel.nodeExists(path, revision)) {
                childPath = path;
            }
        }
        if (childPath == null) {
            return null;
        }
        try {
            return cache.get(revision + childPath);
        } catch (ExecutionException e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        init();
        Iterable<ChildNodeEntry> iterable = iterable(childPaths.entrySet());
        if (childNodeCount > childPaths.size()) {
            List<Iterable<ChildNodeEntry>> iterables = Lists.newArrayList();
            iterables.add(iterable);
            long offset = childPaths.size();
            while (offset < childNodeCount) {
                iterables.add(getChildNodeEntries(offset, MAX_CHILD_NODE_NAMES));
                offset += MAX_CHILD_NODE_NAMES;
            }
            iterable = Iterables.concat(iterables);
        }
        return iterable;
    }

    @Override
    public NodeBuilder builder() {
        if ("/".equals(getPath())) {
            return new KernelRootBuilder(kernel, this);
        } else {
            return new MemoryNodeBuilder(this);
        }
    }

    /**
     * Optimised comparison method that can avoid traversing all properties
     * and child nodes if both this and the given base node state come from
     * the same MicroKernel and either have the same content hash (when
     * available) or are located at the same path in different revisions.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-175">OAK-175</a>
     */
    @Override
    public void compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return; // no differences
        } else if (base instanceof KernelNodeState) {
            KernelNodeState kbase = (KernelNodeState) base;
            if (kernel.equals(kbase.kernel)) {
                if (revision.equals(kbase.revision) && path.equals(kbase.path)) {
                    return; // no differences
                } else {
                    init();
                    kbase.init();
                    if (hash != null && hash.equals(kbase.hash)) {
                        return; // no differences
                    } else if (path.equals(kbase.path)) {
                        // TODO: Parse the JSON diff returned by the kernel
                        // kernel.diff(kbase.revision, revision, path);
                    }
                }
            }
        }
        // fall back to the generic node state diff algorithm
        super.compareAgainstBaseState(base, diff);
    }

    //------------------------------------------------------------< Object >--

    /**
     * Optimised equality check that can avoid a full tree comparison if
     * both instances come from the same MicroKernel and have either
     * the same revision and path or the same content hash (when available).
     * Otherwise we fall back to the default tree comparison algorithm.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-172">OAK-172</a>
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof KernelNodeState) {
            KernelNodeState that = (KernelNodeState) object;
            if (kernel.equals(that.kernel)) {
                if (revision.equals(that.revision) && path.equals(that.path)) {
                    return true;
                } else {
                    this.init();
                    that.init();
                    if (hash != null && that.hash != null) {
                        return hash.equals(that.hash);
                    }
                }
            }
        }
        // fall back to the generic tree equality comparison algorithm
        return super.equals(object);
    }

    //------------------------------------------------------------< internal >---

    @Nonnull
    String getRevision() {
        return revision;
    }

    @Nonnull
    String getPath() {
        return path;
    }

    //------------------------------------------------------------< private >---

    private Iterable<ChildNodeEntry> getChildNodeEntries(
            final long offset, final int count) {
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                List<ChildNodeEntry> entries =
                        Lists.newArrayListWithCapacity(count);
                String json = kernel.getNodes(
                        path, revision, 0, offset, count, null);
                JsopReader reader = new JsopTokenizer(json);
                reader.read('{');
                do {
                    String name = StringCache.get(reader.readString());
                    reader.read(':');
                    if (reader.matches('{')) {
                        reader.read('}');
                        String childPath = getChildPath(name);
                        entries.add(new KernelChildNodeEntry(name, childPath));
                    } else if (reader.matches('[')) {
                        while (reader.read() != ']') {
                            // skip
                        }
                    } else {
                        reader.read();
                    }
                } while (reader.matches(','));
                reader.read('}');
                reader.read(JsopReader.END);
                return entries.iterator();
            }
        };
    }

    private String getChildPath(String name) {
        if ("/".equals(path)) {
            return '/' + name;
        } else {
            return path + '/' + name;
        }
    }

    private Iterable<ChildNodeEntry> iterable(
            Iterable<Entry<String, String>> set) {
        return Iterables.transform(
                set,
                new Function<Entry<String, String>, ChildNodeEntry>() {
                    @Override
                    public ChildNodeEntry apply(Entry<String, String> input) {
                        return new KernelChildNodeEntry(input);
                    }
                });
    }

    private class KernelChildNodeEntry extends AbstractChildNodeEntry {

        private final String name;

        private final String path;

        /**
         * Creates a child node entry with the given name and referenced
         * child node state.
         *
         * @param name child node name
         * @param path child node path
         */
        public KernelChildNodeEntry(String name, String path) {
            this.name = checkNotNull(name);
            this.path = checkNotNull(path);
        }

        /**
         * Utility constructor that copies the name and referenced
         * child node state from the given map entry.
         *
         * @param entry map entry
         */
        public KernelChildNodeEntry(Map.Entry<String, String> entry) {
            this(entry.getKey(), entry.getValue());
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public NodeState getNodeState() {
            try {
                return cache.get(revision + path);
            } catch (ExecutionException e) {
                throw new MicroKernelException(e);
            }
        }

    }

    /**
     * Read a {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private PropertyState readProperty(String name, JsopReader reader) {
        if (reader.matches(JsopReader.NUMBER)) {
            String number = reader.getToken();
            return createProperty(name, number, PropertyType.LONG);
        } else if (reader.matches(JsopReader.TRUE)) {
            return BooleanPropertyState.booleanProperty(name, true);
        } else if (reader.matches(JsopReader.FALSE)) {
            return BooleanPropertyState.booleanProperty(name, false);
        } else if (reader.matches(JsopReader.STRING)) {
            String jsonString = reader.getToken();
            int split = TypeCodes.split(jsonString);
            if (split != -1) {
                int type = TypeCodes.decodeType(split, jsonString);
                String value = TypeCodes.decodeName(split, jsonString);
                if (type == PropertyType.BINARY) {
                    return  BinaryPropertyState.binaryProperty(
                            name, new KernelBlob(new String(value), kernel));
                } else {
                    return createProperty(name, StringCache.get(value), type);
                }
            } else {
                return StringPropertyState.stringProperty(
                        name, StringCache.get(jsonString));
            }
        } else {
            throw new IllegalArgumentException("Unexpected token: " + reader.getToken());
        }
    }

    /**
     * Read a multi valued {@code PropertyState} from a {@link JsopReader}
     * @param name  The name of the property state
     * @param reader  The reader
     * @return new property state
     */
    private PropertyState readArrayProperty(String name, JsopReader reader) {
        int type = PropertyType.STRING;
        List<Object> values = Lists.newArrayList();
        while (!reader.matches(']')) {
            if (reader.matches(JsopReader.NUMBER)) {
                String number = reader.getToken();
                type = PropertyType.LONG;
                values.add(Conversions.convert(number).toLong());
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
                        values.add(new KernelBlob(new String(value), kernel));
                    } else if(type == PropertyType.DOUBLE) {
                        values.add(Conversions.convert(value).toDouble());
                    } else if(type == PropertyType.DECIMAL) {
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
