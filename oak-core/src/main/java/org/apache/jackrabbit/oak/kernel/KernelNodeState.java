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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyList;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

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
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.BooleanPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.DoublePropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.LongPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plugins.value.Conversions;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

/**
 * Basic {@link NodeState} implementation based on the {@link MicroKernel}
 * interface. This class makes an attempt to load data lazily.
 */
public final class KernelNodeState extends AbstractNodeState {

    /**
     * Maximum number of child nodes kept in memory.
     */
    public static final int MAX_CHILD_NAMES = 100;

    /**
     * Number of child nodes beyond which {@link MicroKernel#diff(String, String, String, int)}
     * is used for diffing.
     */
    public static final int LOCAL_DIFF_THRESHOLD = 10;

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
    private static final KernelNodeState NULL = new KernelNodeState();

    private final KernelNodeStore store;

    private final MicroKernel kernel;

    private final String path;

    private String revision;

    private Map<String, PropertyState> properties;

    private long childNodeCount = -1;

    private long childNodeCountMin;

    private String hash;

    private String id;

    private Set<String> childNames;

    /**
     * {@code true} is this is a node state from a branch. {@code false}
     * otherwise.
     * <p>
     * FIXME: this is a workaround to avoid creating branches from a branch
     * until this is supported by the MicroKernel. See {@link KernelNodeState#builder()}.
     */
    private boolean isBranch;

    private final LoadingCache<String, KernelNodeState> cache;

    /**
     * Create a new instance of this class representing the node at the
     * given {@code path} and {@code revision}. It is an error if the
     * underlying Microkernel does not contain such a node.
     *
     * @param store the underlying KernelNodeStore
     * @param path the path of this KernelNodeState
     * @param revision the revision of the node to read from the kernel.
     * @param cache the KernelNodeState cache
     */
    public KernelNodeState(
            KernelNodeStore store, String path, String revision,
            LoadingCache<String, KernelNodeState> cache) {
        this.store = store;
        this.kernel = store.getKernel();
        this.path = checkNotNull(path);
        this.revision = checkNotNull(revision);
        this.cache = checkNotNull(cache);
    }

    private KernelNodeState() {
        this.store = null;
        this.kernel = null;
        this.path = "null";
        this.revision = "null";
        this.cache = DUMMY_CACHE;
    }

    private void init() {
        boolean initialized = false;
        synchronized (this) {
            if (properties == null) {
                String json = kernel.getNodes(
                        path, revision, 0, 0, MAX_CHILD_NAMES,
                        "{\"properties\":[\"*\",\":hash\",\":id\"]}");

                checkNotNull(json,"No node found at path [%s] for revision [%s]",path,revision);
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
    public boolean exists() {
        return true;
    }

    @Override
    public long getPropertyCount() {
        init();
        return properties.size();
    }

    @Override
    public boolean hasProperty(String name) {
        init();
        return properties.containsKey(name);
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
    public long getChildNodeCount(long max) {
        init();
        if (childNodeCount == Long.MAX_VALUE) {
            if (childNodeCountMin > max) {
                // getChildNodeCount(max) was already called,
                // and we know the value is higher than max
                return childNodeCountMin;
            }
            // count the entries
            Iterator<? extends ChildNodeEntry> iterator = getChildNodeEntries().iterator();
            long n = 0;
            while (n <= max) {
                if (!iterator.hasNext()) {
                    // we know the exact number now
                    childNodeCount = n;
                    return n;
                }
                iterator.next();
                n++;
            }
            // remember we have at least this number of entries
            childNodeCountMin = n;
            if (n == max) {
                // we didn't count all entries
                return max;
            }
        }
        return childNodeCount;
    }

    @Override
    public boolean hasChildNode(String name) {
        init();
        if (childNames.contains(name)) {
            return true; // the named child node exits for sure
        } else if (getChildNodeCount(MAX_CHILD_NAMES) <= MAX_CHILD_NAMES) {
            return false; // all child node names are cached, and none match
        } else {
            return isValidName(name) && getChildNode(name).exists();
        }
    }

    @Override
    public NodeState getChildNode(String name) {
        init();
        String childPath = null;
        if (childNames.contains(name)) {
            childPath = PathUtils.concat(path, name);
        } else if (!isValidName(name)) {
            throw new IllegalArgumentException("Invalid name: " + name);
        } else if (getChildNodeCount(MAX_CHILD_NAMES) <= MAX_CHILD_NAMES) {
            return MISSING_NODE;
        } else {
            childPath = PathUtils.concat(path, name);
            // OAK-506: Avoid the nodeExists() call when already cached
            NodeState state = cache.getIfPresent(revision + childPath);
            if (state == NULL) {
                return MISSING_NODE;
            } else if (state != null) {
                return state;
            }
            // not able to tell from cache if node exists
            // need to ask MicroKernel
            if (!kernel.nodeExists(childPath, revision)) {
                cache.put(revision + childPath, NULL);
                return MISSING_NODE;
            }
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
        if (childNodeCount <= MAX_CHILD_NAMES && childNodeCount <= childNames.size()) {
            return iterable(childNames);
        }
        List<Iterable<ChildNodeEntry>> iterables = Lists.newArrayList();
        iterables.add(iterable(childNames));
        iterables.add(getChildNodeEntries(childNames.size()));
        return Iterables.concat(iterables);
    }

    private Iterable<ChildNodeEntry> getChildNodeEntries(final long offset) {
        return new Iterable<ChildNodeEntry>() {
            @Override
            public Iterator<ChildNodeEntry> iterator() {
                return new Iterator<ChildNodeEntry>() {
                    private long currentOffset = offset;
                    private Iterator<ChildNodeEntry> current;

                    {
                        fetchEntries();
                    }

                    private void fetchEntries() {
                        List<ChildNodeEntry> entries = Lists
                                .newArrayListWithCapacity(MAX_CHILD_NAMES);
                        String json = kernel.getNodes(path, revision, 0,
                                currentOffset, MAX_CHILD_NAMES, null);
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
                        if (entries.isEmpty()) {
                            current = null;
                        } else {
                            currentOffset += entries.size();
                            current = entries.iterator();
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        while (true) {
                            if (current == null) {
                                return false;
                            } else if (current.hasNext()) {
                                return true;
                            }
                            fetchEntries();
                        }
                    }

                    @Override
                    public ChildNodeEntry next() {
                        if (!hasNext()) {
                            throw new IllegalStateException(
                                    "Reading past the end");
                        }
                        return current.next();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                    
                };
            }
        };
    }

    /**
     * This implementation returns a {@link KernelNodeBuilder} unless this is not
     * a root node or {@link #isBranch} is {@code true} in which case a
     * {@link MemoryNodeBuilder} is returned.
     * <p>
     * TODO: this is a workaround to avoid creating branches from a branch
     * until this is supported by the MicroKernel.
     */
    @Override
    public NodeBuilder builder() {
        if (isBranch) {
            return new MemoryNodeBuilder(this);
        } else if ("/".equals(path)) {
            return new KernelRootBuilder(this, store);
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
    public boolean compareAgainstBaseState(NodeState base, NodeStateDiff diff) {
        if (this == base) {
            return true; // no differences
        } else if (base == EMPTY_NODE || !base.exists()) { // special case
            return EmptyNodeState.compareAgainstEmptyState(this, diff);
        } else if (base instanceof KernelNodeState) {
            KernelNodeState kbase = (KernelNodeState) base;
            if (kernel.equals(kbase.kernel)) {
                if (revision.equals(kbase.revision) && path.equals(kbase.path)) {
                    return true; // no differences
                } else {
                    init();
                    kbase.init();
                    if (hash != null && hash.equals(kbase.hash)) {
                        return true; // no differences
                    } else if (id != null && id.equals(kbase.id)) {
                        return true; // no differences
                    } else if (path.equals(kbase.path)
                            && getChildNodeCount(LOCAL_DIFF_THRESHOLD) > LOCAL_DIFF_THRESHOLD) {
                        // use MK.diff() when there are 'many' child nodes
                        String jsonDiff = kernel.diff(kbase.getRevision(), revision, path, 0);
                        return processJsonDiff(jsonDiff, kbase, diff);
                    }
                }
            }
        }
        // fall back to the generic node state diff algorithm
        return super.compareAgainstBaseState(base, diff);
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
                        String r1 = revision, r2 = that.getRevision();
                        if (r1.compareTo(r2) > 0) {
                            // sort the revisions, to allow the MicroKernel to cache the result
                            String temp = r1;
                            r1 = r2;
                            r2 = temp;
                        }
                        String jsonDiff = kernel.diff(r1, r2, path, 0);
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

    /**
     * Mark this instance as from being on branch.
     * <p>
     * TODO this is a workaround to avoid creating branches from a branch
     * until this is supported by the MicroKernel. See {@link KernelNodeState#builder()}.
     * @return {@code this}
     */
    KernelNodeState setBranch() {
        isBranch = true;
        return this;
    }

    /**
     * @return  {@code true} if this instance has been marked as being on a branch by a call
     * to {@link #setBranch()}
     */
    boolean isBranch() {
        return isBranch;
    }

    /**
     * @return the approximate memory usage of this node state.
     */
    synchronized int getMemory() {
        // base memory footprint is roughly 64 bytes
        int memory = 64;
        // path String
        memory += 48 + path.length() * 2;
        // revision String
        memory += 48 + revision.length() * 2;
        // optional hash String
        if (hash != null) {
            memory += 48 + hash.length() * 2;
        }
        // optional id String
        if (id != null && !id.equals(hash)) {
            memory += 48 + id.length() * 2;
        }
        // rough approximation for properties
        if (properties != null) {
            for (Map.Entry<String, PropertyState> entry : properties.entrySet()) {
                // name
                memory += 48 + entry.getKey().length() * 2;
                PropertyState propState = entry.getValue();
                if (propState.getType() != Type.BINARY
                        && propState.getType() != Type.BINARIES) {
                    // assume binaries go into blob store
                    for (int i = 0; i < propState.count(); i++) {
                        // size() returns length of string
                        // overhead:
                        // - 8 bytes per reference in values list
                        // - 48 bytes per string
                        memory += 56 + propState.size(i) * 2;
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

    private static boolean hasChanges(String journal) {
        return !journal.trim().isEmpty();
    }

    /**
     * Process the given JSON diff, which is the diff of of the {@code base}
     * node state to this node state.
     *
     * @param jsonDiff the JSON diff.
     * @param base the base node state.
     * @param diff where diffs are reported to.
     * @return {@code true} to continue the comparison, {@code false} to stop
     */
    private boolean processJsonDiff(String jsonDiff,
                                 KernelNodeState base,
                                 NodeStateDiff diff) {
        if (!hasChanges(jsonDiff)) {
            return true;
        }
        if (!AbstractNodeState.comparePropertiesAgainstBaseState(this, base, diff)) {
            return false;
        }
        JsopTokenizer t = new JsopTokenizer(jsonDiff);
        boolean continueComparison = true;
        while (continueComparison) {
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
                    continueComparison = diff.childNodeAdded(name, getChildNode(name));
                    break;
                }
                case '-': {
                    String path = t.readString();
                    String name = PathUtils.getName(path);
                    continueComparison = diff.childNodeDeleted(name, base.getChildNode(name));
                    break;
                }
                case '^': {
                    String path = t.readString();
                    t.read(':');
                    if (t.matches('{')) {
                        t.read('}');
                        String name = PathUtils.getName(path);
                        continueComparison = diff.childNodeChanged(name,
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
                    continueComparison = diff.childNodeDeleted(
                            fromName, base.getChildNode(fromName));
                    if (!continueComparison) {
                        break;
                    }
                    String toName = PathUtils.getName(to);
                    continueComparison = diff.childNodeAdded(
                            toName, getChildNode(toName));
                    break;
                }
                default:
                    throw new IllegalArgumentException("jsonDiff: illegal token '"
                            + t.getToken() + "' at pos: " + t.getLastPos() + ' ' + jsonDiff);
            }
        }
        return continueComparison;
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
                return cache.get(revision + PathUtils.concat(path, name));
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
                int type = PropertyType.valueFromName(
                        jsonString.substring(TypeCodes.EMPTY_ARRAY.length()));
                return PropertyStates.createProperty(
                        name, emptyList(), Type.fromTag(type, true));
            }
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
                        values.add(new KernelBlob(new String(value), kernel));
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
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(path).append('@').append(revision);
        if (childNodeCount >= 0) {
            builder.append(" children: ").append(childNodeCount);
        }
        if (hash != null) {
            builder.append(" hash: ").append(hash);
        }
        if (id != null) {
            builder.append(" id: ").append(id);
        }
        builder.append(" {");
        int count = 0;
        if (properties == null) {
            builder.append(" /* props not initialized */");
        } else {
            for (PropertyState property : getProperties()) {
                if (count++ > 0) {
                    builder.append(',');
                }
                builder.append(' ').append(property);
            }
        }
        if (childNames == null) {
            builder.append(" /* child node names not initialized */");
        } else {
            for (String s : childNames) {
                if (count++ > 0) {
                    builder.append(',');
                }
                builder.append(' ').append(s);
            }
        }
        builder.append(" }");
        return builder.toString();
    }

}
