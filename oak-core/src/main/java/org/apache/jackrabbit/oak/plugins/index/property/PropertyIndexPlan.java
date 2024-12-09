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

import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.cursor.Cursors;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.collect.Iterables;

/**
 * Plan for querying a given property index using a given filter.
 */
public class PropertyIndexPlan {

    static final Logger LOG = LoggerFactory.getLogger(PropertyIndexPlan.class);

    /**
     * The cost overhead to use the index in number of read operations.
     */
    public static final double COST_OVERHEAD = 2;

    /**
     * The maximum cost when the index can be used.
     */
    static final int MAX_COST = 100;

    private final NodeState definition;

    private final String name;

    private final Set<String> properties;

    private final Set<IndexStoreStrategy> strategies;

    private final Filter filter;

    private boolean matchesAllTypes;

    private boolean matchesNodeTypes;

    private final double cost;

    private final Set<String> values;

    private final int depth;

    private final PathFilter pathFilter;

    private final boolean unique;

    private final boolean deprecated;

    PropertyIndexPlan(String name, NodeState root, NodeState definition,
                      Filter filter){
        this(name, root, definition, filter, Mounts.defaultMountInfoProvider());
    }

    PropertyIndexPlan(String name, NodeState root, NodeState definition,
                      Filter filter, MountInfoProvider mountInfoProvider) {
        this.name = name;
        this.unique = definition.getBoolean(IndexConstants.UNIQUE_PROPERTY_NAME);
        this.definition = definition;
        this.properties = CollectionUtils.toSet(definition.getNames(PROPERTY_NAMES));
        pathFilter = PathFilter.from(definition.builder());
        this.strategies = getStrategies(definition, mountInfoProvider);
        this.filter = filter;

        Iterable<String> types = definition.getNames(DECLARING_NODE_TYPES);
        // if there is no such property, then all nodetypes are matched
        this.matchesAllTypes = !definition.hasProperty(DECLARING_NODE_TYPES);
        this.deprecated = definition.getBoolean(IndexConstants.INDEX_DEPRECATED);
        this.matchesNodeTypes =
                matchesAllTypes || CollectionUtils.toStream(types).anyMatch(filter.getSupertypes()::contains);

        ValuePattern valuePattern = new ValuePattern(definition);

        double bestCost = Double.POSITIVE_INFINITY;
        Set<String> bestValues = emptySet();
        int bestDepth = 1;

        if (matchesNodeTypes &&
                pathFilter.areAllDescendantsIncluded(filter.getPath())) {
            for (String property : properties) {
                PropertyRestriction restriction =
                        filter.getPropertyRestriction(property);
                int depth = 1;

                if (restriction == null) {
                    // no direct restriction, try one with a relative path
                    // TODO: avoid repeated scans through the restrictions
                    String suffix = "/" + property;
                    for (PropertyRestriction relative
                            : filter.getPropertyRestrictions()) {
                        if (relative.propertyName.endsWith(suffix)) {
                            restriction = relative;
                            depth = PathUtils.getDepth(relative.propertyName);
                        }
                    }
                }

                if (restriction != null) {
                    if (restriction.isNullRestriction()) {
                        // covering indexes are not currently supported
                        continue;
                    }
                    if (depth != 1 && !matchesAllTypes) {
                        // OAK-3589
                        // index has a nodetype condition, and the property condition is
                        // relative: can not use this index, as we don't know the nodetype
                        // of the child node (well, we could, for some node types)
                        continue;
                    }
                    Set<String> values = ValuePatternUtil.getValues(restriction, new ValuePattern());
                    if (valuePattern.matchesAll()) {
                        // matches all values: not a problem
                    } else if (values == null) {
                        // "is not null" condition, but we have a value pattern
                        // that doesn't match everything
                        String prefix = ValuePatternUtil.getLongestPrefix(filter, property);
                        if (!valuePattern.matchesPrefix(prefix)) {
                            // region match which is not fully in the pattern
                            continue;
                        }
                    } else {
                        // we have a value pattern, for example (a|b),
                        // but we search (also) for 'c': can't match
                        if (!valuePattern.matchesAll(values)) {
                            continue;
                        }
                    }
                    values = PropertyIndexUtil.encode(values);
                    double cost = strategies.isEmpty() ? MAX_COST : 0;
                    for (IndexStoreStrategy strategy : strategies) {
                        cost += strategy.count(filter, root, definition,
                                values, MAX_COST);
                    }
                    if (unique && cost <= 1) {
                        // for unique index, for the normal case
                        // (that is, for a regular lookup)
                        // no further reads are needed
                        cost = 0;
                    }
                    if (cost < bestCost) {
                        bestDepth = depth;
                        bestValues = values;
                        bestCost = cost;
                        if (bestCost == 0) {
                            // shortcut: not possible to top this
                            break;
                        }
                    }
                }
            }
        }

        this.depth = bestDepth;
        this.values = bestValues;
        this.cost = COST_OVERHEAD + bestCost;
    }

    String getName() {
        return name;
    }

    double getCost() {
        return cost;
    }

    Cursor execute() {
        QueryLimits settings = filter.getQueryLimits();
        if (deprecated) {
            final String caller = IndexUtils.getCaller(settings.getIgnoredClassNamesInCallTrace());
            LOG.warn("This index is deprecated: {}; it is used for query {} called by {}. " +
                    "Please change the query or the index definitions.", name, filter, caller);
        }
        List<Iterable<String>> iterables = new ArrayList<>();
        for (IndexStoreStrategy s : strategies) {
            iterables.add(s.query(filter, name, definition, values));
        }
        Cursor cursor = Cursors.newPathCursor(Iterables.concat(iterables),
                settings);
        if (depth > 1) {
            cursor = Cursors.newAncestorCursor(cursor, depth - 1, settings);
        }
        return cursor;
    }

    Filter getFilter() {
        return filter;
    }

    Set<IndexStoreStrategy> getStrategies(NodeState definition,
            MountInfoProvider mountInfoProvider) {
        return Multiplexers.getStrategies(unique, mountInfoProvider,
                definition, INDEX_CONTENT_NODE_NAME);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("property ").append(name).append("\n");
        buffer.append("    indexDefinition: /");
        buffer.append(IndexConstants.INDEX_DEFINITIONS_NAME);
        buffer.append("/").append(name).append("\n");
        buffer.append("    values: ");
        if (values == null) {
            buffer.append("all values in the index (warning: may be slow)");
        } else if (values.isEmpty()) {
            buffer.append("not applicable");
        } else if (values.size() == 1) {
            buffer.append(SQL2Parser.escapeStringLiteral(values.iterator().next()));
        } else {
            boolean comma = false;
            for (String value : values) {
                if (comma) {
                    buffer.append(", ");
                }
                buffer.append(SQL2Parser.escapeStringLiteral(value));
                comma = true;
            }
        }
        buffer.append("\n");
        buffer.append("    estimatedCost: ").append(cost).append("\n");
        return buffer.toString();
    }

}
