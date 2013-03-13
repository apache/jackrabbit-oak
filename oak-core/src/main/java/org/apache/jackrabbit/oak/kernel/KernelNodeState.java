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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.jcr.PropertyType;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Basic {@link NodeState} implementation based on the {@link MicroKernel}
 * interface. This class makes an attempt to load data lazily.
 */
public final class KernelNodeState extends AbstractNodeState {

    /**
     * Maximum number of child nodes kept in memory.
     */
    static final int MAX_CHILD_NODE_NAMES = 1000;

    /**
     * Dummy cache instance for static {@link #NULL} kernel node state.
     */
    private static final LoadingCache<String, KernelNodeState> DUMMY_CACHE =
        CacheBuilder.newBuilder().build(new CacheLoader<String, KernelNodeState>() {
            @Override
            public KernelNodeState load(String key) throws Exception {
                throw new UnsupportedOperationException();
            }
        });

    /**
     * This {@code NULL} kernel node state is used as a value in the
     * {@link #cache} to indicate that there is no node state at the given
     * path and revision. This object is only used internally and never leaves
     * this {@link KernelNodeState}.
     */
    private static final KernelNodeState NULL = new KernelNodeState(
            (MicroKernel) Proxy.newProxyInstance(MicroKernel.class.getClassLoader(),
                    new Class[]{MicroKernel.class}, new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            throw new UnsupportedOperationException();
        }
    }), "null", "null", DUMMY_CACHE);

    private final MicroKernel kernel;

    private final String path;

    private String revision;

    private Map<String, PropertyState> properties;

    private long childNodeCount = -1;

    private String hash;

    private String id;

    private Set<String> childNames;

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

    private void init() {
        boolean initialized = false;
        synchronized (this) {
            if (properties == null) {
                String json = kernel.getNodes(
                        path, revision, 0, 0, MAX_CHILD_NODE_NAMES,
                        "{\"properties\":[\"*\",\":hash\",\":id\"]}");

                JsopReader reader = new JsopTokenizer(json);
                reader.read('{');
                properties = new LinkedHashMap<String, PropertyState>();
                childNames = new LinkedHashSet<String>();
                do {
                    String name = StringCache.get(reader.readString());
                    reader.read(':');
                    if (":childNodeCount".equals(name)) {
                        childNodeCount =
                                Long.valueOf(reader.read(JsopReader.NUMBER));
                    } else if (":hash".equals(name)) {
                        hash = new String(reader.read(JsopReader.STRING));
                        if (hash.equals(id)) {
                            // save some memory
                            hash = id;
                        }
                    } else if (":id".equals(name)) {
                        id = new String(reader.read(JsopReader.STRING));
                        if (id.equals(hash)) {
                            // save some memory
                            id = hash;
                        }
                    } else if (reader.matches('{')) {
                        reader.read('}');
                        childNames.add(name);
                    } else if (reader.matches('[')) {
                        properties.put(name, readArrayProperty(name, reader));
                    } else {
                        properties.put(name, readProperty(name, reader));
                    }
                } while (reader.matches(','));
                reader.read('}');
                reader.read(JsopReader.END);
                // optimize for empty childNodes
                if (childNames.isEmpty()) {
                    childNames = Collections.emptySet();
                }
                initialized = true;
            }
        }
        if (initialized) {
            // refresh cache to force re-calculation of weight (OAK-643)
            cache.refresh(revision + path);
        }
        if (initialized && !PathUtils.denotesRoot(path)) {
            // OAK-591: check if we can re-use a previous revision
            // by looking up the node state by hash or id (if available)
            // introducing this secondary lookup basically means we point
            // back to a subtree in an older revision, in case it didn't change
            String hashOrId = null;
            if (hash != null) {
                // hash takes precedence
                hashOrId = hash;
            } else if (id != null) {
                hashOrId = id;
            }
            if (hashOrId != null) {
                KernelNodeState cached = cache.getIfPresent(hashOrId);
                if (cached != null && cached.path.equals(this.path)) {
                    synchronized (this) {
                        this.revision = cached.revision;
                        this.childNames = cached.childNames;
                        this.properties = cached.properties;
                    }
                } else {
                    // store under secondary key
                    cache.put(hashOrId, this);
                }
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
        checkNotNull(name);
        init();
        String childPath = null;
        if (childNames.contains(name)) {
            childPath = getChildPath(name);
        }
        if (childPath == null && childNodeCount > MAX_CHILD_NODE_NAMES) {
            String path = getChildPath(name);
            // OAK-506: Avoid the nodeExists() call when already cached
            NodeState state = cache.getIfPresent(revision + path);
            if (state != null) {
                if (state != NULL) {
                    return state;
                } else {
                    return null;
                }
            }
            // not able to tell from cache if node exists
            // need to ask MicroKernel
            if (kernel.nodeExists(path, revision)) {
                childPath = path;
            } else {
                cache.put(revision + path, NULL);
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
        Iterable<ChildNodeEntry> iterable = iterable(childNames);
        if (childNodeCount > childNames.size()) {
            List<Iterable<ChildNodeEntry>> iterables = Lists.newArrayList();
            iterables.add(iterable);
            long offset = childNames.size();
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
        return new MemoryNodeBuilder(this);
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
        } else if (base == EMPTY_NODE) {
            EmptyNodeState.compareAgainstEmptyState(this, diff); // special case
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
                    } else if (id != null && id.equals(kbase.id)) {
                        return; // no differences
                    } else if (path.equals(kbase.path)
                            && childNodeCount > MAX_CHILD_NODE_NAMES) {
                        // use MK.diff() when there are 'many' child nodes
                        String jsonDiff = kernel.diff(kbase.getRevision(), revision, path, 0);
                        processJsonDiff(jsonDiff, kbase, diff);
                        return;
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
                    } else if (id != null && id.equals(that.id)) {
                        // only return result of equals if ids are equal
                        // different ids doesn't mean the node states are
                        // definitively different.
                        return true;
                    } else if (path.equals(that.path) && !path.equals("/")) {
                        String jsonDiff = kernel.diff(that.getRevision(), revision, path, 0);
                        return !hasChanges(jsonDiff);
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

    /**
     * @return the approximate memory usage of this node state.
     */
    synchronized int getMemory() {
        // base memory footprint is roughly 64 bytes
        int memory = 64;
        // path String
        memory += 12 + path.length() * 2;
        // revision String
        memory += 12 + revision.length() * 2;
        // optional hash String
        if (hash != null) {
            memory += 12 + hash.length() * 2;
        }
        // optional id String
        if (id != null && !id.equals(hash)) {
            memory += 12 + id.length() * 2;
        }
        // rough approximation for properties
        if (properties != null) {
            for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
                // name
                memory += 12 + entry.getKey().length() * 2;
                PropertyState propState = entry.getValue();
                if (propState.getType() != Type.BINARY
                        && propState.getType() != Type.BINARIES) {
                    // assume binaries go into blob store
                    for (int i = 0; i < propState.count(); i++) {
                        memory += propState.size(i);
                    }
                }
            }
        }
        // rough approximation for child nodes
        if (childNames != null) {
            memory += childNames.size() * 150;
        }
        return memory;
    }

    //------------------------------------------------------------< private >---

    private boolean hasChanges(String journal) {
        return !journal.trim().isEmpty();
    }

    /**
     * Process the given JSON diff, which is the diff of of the {@code base}
     * node state to this node state.
     *
     * @param jsonDiff the JSON diff.
     * @param base the base node state.
     * @param diff where diffs are reported to.
     */
    private void processJsonDiff(String jsonDiff,
                                 KernelNodeState base,
                                 NodeStateDiff diff) {
        if (!hasChanges(jsonDiff)) {
            return;
        }
        comparePropertiesAgainstBaseState(base, diff);
        JsopTokenizer t = new JsopTokenizer(jsonDiff);
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            switch (r) {
                case '+': {
                    String path = t.readString();
                    t.read(':');
                    t.read('{');
                    while (t.read() != '}') {
                        // skip properties
                    }
                    String name = PathUtils.getName(path);
                    diff.childNodeAdded(name, getChildNode(name));
                    break;
                }
                case '-': {
                    String path = t.readString();
                    String name = PathUtils.getName(path);
                    diff.childNodeDeleted(name, base.getChildNode(name));
                    break;
                }
                case '^': {
                    String path = t.readString();
                    t.read(':');
                    if (t.matches('{')) {
                        t.read('}');
                        String name = PathUtils.getName(path);
                        diff.childNodeChanged(name,
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
                case '>': {
                    String from = t.readString();
                    t.read(':');
                    String to = t.readString();
                    String fromName = PathUtils.getName(from);
                    diff.childNodeDeleted(fromName, base.getChildNode(fromName));
                    String toName = PathUtils.getName(to);
                    diff.childNodeAdded(toName, getChildNode(toName));
                    break;
                }
                default:
                    throw new IllegalArgumentException("jsonDiff: illegal token '"
                            + t.getToken() + "' at pos: " + t.getLastPos() + ' ' + jsonDiff);
            }
        }
    }

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
                        entries.add(new KernelChildNodeEntry(name));
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
            Iterable<String> names) {
        return Iterables.transform(
                names,
                new Function<String, ChildNodeEntry>() {
                    @Override
                    public ChildNodeEntry apply(String input) {
                        return new KernelChildNodeEntry(input);
                    }
                });
    }

    private class KernelChildNodeEntry extends AbstractChildNodeEntry {

        private final String name;

        /**
         * Creates a child node entry with the given name.
         *
         * @param name child node name
         */
        public KernelChildNodeEntry(String name) {
            this.name = checkNotNull(name);
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public NodeState getNodeState() {
            try {
                return cache.get(revision + getChildPath(name));
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
