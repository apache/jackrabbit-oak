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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider.TYPE;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexUtil.encode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Is responsible for querying the property index content.
 * <br>
 * This class can be used directly on a subtree where there is an index defined
 * by supplying a {@link NodeState} root.
 * 
 * <pre>{@code
 * {
 *     NodeState state = ... // get a node state
 *     PropertyIndexLookup lookup = new PropertyIndexLookup(state);
 *     Set<String> hits = lookup.find("foo", PropertyValues.newString("xyz"));
 * }
 * }</pre>
 */
public class PropertyIndexLookup {

    static final Logger LOG = LoggerFactory.getLogger(PropertyIndexLookup.class);

    /**
     * The cost overhead to use the index in number of read operations.
     */
    public static final double COST_OVERHEAD = 2;
    
    /**
     * The maximum cost when the index can be used.
     */
    static final int MAX_COST = 100;

    /**
     * Feature toggle name for the configurable costPerEntry/costPerExecution
     * cost formula (OAK-12348).
     */
    public static final String FT_OAK_12348 = "FT_OAK-12348";

    /**
     * When {@code true} (the default), {@link #getCost} reads {@code costPerEntry}/
     * {@code costPerExecution} from the index definition ({@link #getCostConfigurable}).
     * When {@code false}, {@link #getCost} uses the original hardcoded formula
     * ({@link #getCostLegacy}) unconditionally, ignoring those properties even if
     * set. Enabled by default: the new formula reproduces the legacy one exactly
     * whenever {@code costPerEntry}/{@code costPerExecution} are absent, so this is
     * a behavior-preserving default for anyone not using the new properties -- the
     * toggle exists as an escape hatch, not as an opt-in gate.
     */
    public static final AtomicBoolean FT_OAK_12348_ENABLE = new AtomicBoolean(true);

    private final NodeState root;

    private final MountInfoProvider mountInfoProvider;

    public PropertyIndexLookup(NodeState root) {
        this(root, Mounts.defaultMountInfoProvider());
    }

    public PropertyIndexLookup(NodeState root, MountInfoProvider mountInfoProvider) {
        this.root = root;
        this.mountInfoProvider = mountInfoProvider;
    }

    /**
     * Checks whether the named property is indexed somewhere along the given
     * path. Lookup starts at the current path (at the root of this object) and
     * traverses down the path.
     * 
     * @param propertyName property name
     * @param path lookup path
     * @param filter for the node type restriction (null if no node type restriction)
     * @return true if the property is indexed
     */
    public boolean isIndexed(String propertyName, String path, Filter filter) {
        if (PathUtils.denotesRoot(path)) {
            return getIndexNode(root, propertyName, filter) != null;
        }

        NodeState node = root;
        for (String s : PathUtils.elements(path)) {
            if (getIndexNode(node, propertyName, filter) != null) {
                return true;
            }
            node = node.getChildNode(s);
        }
        return false;
    }

    public Iterable<String> query(Filter filter, String propertyName,
            PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        List<Iterable<String>> iterables = new ArrayList<>();
        ValuePattern pattern = new ValuePattern(indexMeta);
        for (IndexStoreStrategy s : getStrategies(indexMeta)) {
            iterables.add(s.query(filter, propertyName, indexMeta,
                    encode(value, pattern)));
        }
        return org.apache.jackrabbit.oak.commons.collections.IterableUtils.chainedIterable(iterables);
    }

    Set<IndexStoreStrategy> getStrategies(NodeState definition) {
        boolean unique = definition
                .getBoolean(IndexConstants.UNIQUE_PROPERTY_NAME);
        return Multiplexers.getStrategies(unique, mountInfoProvider,
                definition, INDEX_CONTENT_NODE_NAME);
    }

    /**
     * Dispatches to {@link #getCostConfigurable} or {@link #getCostLegacy}
     * depending on {@link #FT_OAK_12348_ENABLE}.
     */
    public double getCost(Filter filter, String propertyName, PropertyValue value) {
        return FT_OAK_12348_ENABLE.get()
                ? getCostConfigurable(filter, propertyName, value)
                : getCostLegacy(filter, propertyName, value);
    }

    /**
     * Original cost formula: {@code COST_OVERHEAD + entryCount}. Ignores
     * {@code costPerEntry}/{@code costPerExecution} even if set on the index
     * definition.
     */
    public double getCostLegacy(Filter filter, String propertyName, PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            return Double.POSITIVE_INFINITY;
        }
        Set<IndexStoreStrategy> strategies = getStrategies(indexMeta);
        ValuePattern pattern = new ValuePattern(indexMeta);
        double cost = strategies.isEmpty() ? MAX_COST : COST_OVERHEAD;
        for (IndexStoreStrategy s : strategies) {
            cost += s.count(filter, root, indexMeta, encode(value, pattern), MAX_COST);
        }
        return cost;
    }

    /**
     * {@code cost = costPerExecution + costPerEntry * entryCount}, both
     * optionally configured on the index definition (OAK-12348). Defaults
     * ({@code costPerEntry=1.0}, {@code costPerExecution=COST_OVERHEAD})
     * reproduce {@link #getCostLegacy} exactly.
     */
    public double getCostConfigurable(Filter filter, String propertyName, PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            return Double.POSITIVE_INFINITY;
        }
        Set<IndexStoreStrategy> strategies = getStrategies(indexMeta);
        if (strategies.isEmpty()) {
            return MAX_COST;
        }
        ValuePattern pattern = new ValuePattern(indexMeta);
        double entryCount = 0;
        for (IndexStoreStrategy s : strategies) {
            entryCount += s.count(filter, root, indexMeta, encode(value, pattern), MAX_COST);
        }
        double costPerEntry = IndexUtils.getOptionalValue(indexMeta, IndexConstants.COST_PER_ENTRY, 1.0);
        double costPerExecution = IndexUtils.getOptionalValue(indexMeta, IndexConstants.COST_PER_EXECUTION, COST_OVERHEAD);
        return costPerExecution + costPerEntry * entryCount;
    }

    /**
     * Get the node with the index definition for the given property, if there
     * is an applicable index with data.
     * 
     * @param propertyName the property name
     * @param filter the filter (which contains information of all supertypes,
     *            unless the filter matches all types)
     * @return the node where the index definition (metadata) is stored (the
     *         parent of ":index"), or null if no index definition or index data
     *         node was found
     */
    @Nullable
    NodeState getIndexNode(NodeState node, String propertyName, Filter filter) {
        // keep a fallback to a matching index def that has *no* node type constraints
        // (initially, there is no fallback)
        NodeState fallback = null;

        NodeState state = node.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            NodeState index = entry.getNodeState();
            PropertyState type = index.getProperty(TYPE_PROPERTY_NAME);
            if (type == null || type.isArray() || !getType().equals(type.getValue(Type.STRING))) {
                continue;
            }
            if (IterableUtils.contains(getNames(index, PROPERTY_NAMES), propertyName)) {
                NodeState indexContent = index.getChildNode(INDEX_CONTENT_NODE_NAME);
                if (!indexContent.exists()) {
                    continue;
                }
                Set<String> supertypes = getSuperTypes(filter);
                if (index.hasProperty(DECLARING_NODE_TYPES)) {
                    if (supertypes != null) {
                        for (String typeName : getNames(index, DECLARING_NODE_TYPES)) {
                            if (supertypes.contains(typeName)) {
                                // TODO: prefer the most specific type restriction
                                return index;
                            }
                        }
                    }
                } else if (supertypes == null) {
                    return index;
                } else if (fallback == null) {
                    // update the fallback
                    fallback = index;
                }
            }
        }
        return fallback;
    }

    /**
     * retrieve the type of the index
     * 
     * @return the type
     */
    String getType() {
        return TYPE;
    }

    @Nullable
    private static Set<String> getSuperTypes(Filter filter) {
        if (filter != null && !filter.matchesAllTypes()) {
            return filter.getSupertypes();
        }
        return null;
    }

    @NotNull
    private static Iterable<String> getNames(@NotNull NodeState state, @NotNull String propertyName) {
        Iterable<String> ret = state.getNames(propertyName);
        if (ret.iterator().hasNext()) {
            return ret;
        }

        PropertyState property = state.getProperty(propertyName);
        if (property != null) {
            LOG.warn("Expected '{}' as type of property '{}' but found '{}'. Node - '{}'",
                    Type.NAMES, propertyName, property.getType(), state);
            ret = property.getValue(Type.STRINGS);
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }
}
