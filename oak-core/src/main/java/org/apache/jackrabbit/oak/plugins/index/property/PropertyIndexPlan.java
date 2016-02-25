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

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static java.util.Collections.emptySet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.UniqueEntryStoreStrategy;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Plan for querying a given property index using a given filter.
 */
public class PropertyIndexPlan {

    /**
     * The cost overhead to use the index in number of read operations.
     */
    static final double COST_OVERHEAD = 2;

    /**
     * The maximum cost when the index can be used.
     */
    static final int MAX_COST = 100;

    /** Index storage strategy */
    private static final IndexStoreStrategy MIRROR =
            new ContentMirrorStoreStrategy();

    /** Index storage strategy */
    private static final IndexStoreStrategy UNIQUE =
            new UniqueEntryStoreStrategy();

    private final NodeState definition;

    private final String name;

    private final Set<String> properties;

    private final IndexStoreStrategy strategy;

    private final Filter filter;

    private boolean matchesAllTypes;

    private boolean matchesNodeTypes;

    private final double cost;

    private final Set<String> values;

    private final int depth;

    private final PathFilter pathFilter;

    PropertyIndexPlan(String name, NodeState root, NodeState definition, Filter filter) {
        this.name = name;
        this.definition = definition;
        this.properties = newHashSet(definition.getNames(PROPERTY_NAMES));
        pathFilter = PathFilter.from(definition.builder());

        if (definition.getBoolean(UNIQUE_PROPERTY_NAME)) {
            this.strategy = UNIQUE;
        } else {
            this.strategy = MIRROR;
        }

        this.filter = filter;

        Iterable<String> types = definition.getNames(DECLARING_NODE_TYPES);
        // if there is no such property, then all nodetypes are matched
        this.matchesAllTypes = !definition.hasProperty(DECLARING_NODE_TYPES);
        this.matchesNodeTypes =
                matchesAllTypes || any(types, in(filter.getSupertypes()));

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
                    Set<String> values = getValues(restriction);
                    double cost = strategy.count(filter, root, definition, values, MAX_COST);
                    if (cost < bestCost) {
                        bestDepth = depth;
                        bestValues = values;
                        bestCost = cost;
                    }
                }
            }
        }

        this.depth = bestDepth;
        this.values = bestValues;
        this.cost = COST_OVERHEAD + bestCost;
    }

    private static Set<String> getValues(PropertyRestriction restriction) {
        if (restriction.firstIncluding
                && restriction.lastIncluding
                && restriction.first != null
                && restriction.first.equals(restriction.last)) {
            // "[property] = $value"
            return encode(restriction.first);
        } else if (restriction.list != null) {
            // "[property] IN (...)
            Set<String> values = newLinkedHashSet(); // keep order for testing
            for (PropertyValue value : restriction.list) {
                values.addAll(encode(value));
            }
            return values;
        } else {
            // "[property] is not null" or "[property] is null"
            return null;
        }
    }

    String getName() {
        return name;
    }

    double getCost() {
        return cost;
    }

    Cursor execute() {
        QueryEngineSettings settings = filter.getQueryEngineSettings();
        Cursor cursor = Cursors.newPathCursor(
                strategy.query(filter, name, definition, values),
                settings);
        if (depth > 1) {
            cursor = Cursors.newAncestorCursor(cursor, depth - 1, settings);
        }
        return cursor;
    }

    Filter getFilter() {
        return filter;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder("property ");
        buffer.append(name);
        if (values == null) {
            buffer.append(" IS NOT NULL");
        } else if (values.isEmpty()) {
            buffer.append(" NOT APPLICABLE");
        } else if (values.size() == 1) {
            buffer.append(" = ");
            buffer.append(values.iterator().next());
        } else {
            buffer.append(" IN (");
            boolean comma = false;
            for (String value : values) {
                if (comma) {
                    buffer.append(", ");
                }
                buffer.append(value);
                comma = true;
            }
            buffer.append(")");
        }
        return buffer.toString();
    }

}
