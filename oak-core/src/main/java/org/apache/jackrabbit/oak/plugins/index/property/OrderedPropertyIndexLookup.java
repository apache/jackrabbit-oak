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

import static com.google.common.collect.Iterables.contains;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.PropertyIndex.encode;
import static org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry.Order;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 *
 */
public class OrderedPropertyIndexLookup {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedPropertyIndexLookup.class);

    /**
     * the standard Ascending ordered index
     */
    private static final OrderedContentMirrorStoreStrategy STORE = new OrderedContentMirrorStoreStrategy();

    /**
     * the descending ordered index
     */
    private static final OrderedContentMirrorStoreStrategy REVERSED_STORE = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
    
    /**
     * we're slightly more expensive than the standard PropertyIndex.
     */
    private static final double COST_OVERHEAD = 3;

    /**
     * The maximum cost when the index can be used.
     */
    private static final int MAX_COST = 100;

    private NodeState root;

    private String name;

    private OrderedPropertyIndexLookup parent;

    public OrderedPropertyIndexLookup(NodeState root) {
        this(root, "", null);
    }

    public OrderedPropertyIndexLookup(NodeState root, String name,
                                      OrderedPropertyIndexLookup parent) {
        this.root = root;
        this.name = name;
        this.parent = parent;
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
            if (contains(index.getNames(PROPERTY_NAMES), propertyName)) {
                NodeState indexContent = index.getChildNode(INDEX_CONTENT_NODE_NAME);
                if (!indexContent.exists()) {
                    continue;
                }
                Set<String> supertypes = getSuperTypes(filter);
                if (index.hasProperty(DECLARING_NODE_TYPES)) {
                    if (supertypes != null) {
                        for (String typeName : index.getNames(DECLARING_NODE_TYPES)) {
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

    private static Set<String> getSuperTypes(Filter filter) {
        if (filter != null && !filter.matchesAllTypes()) {
            return filter.getSupertypes();
        }
        return null;
    }

    static OrderedContentMirrorStoreStrategy getStrategy(NodeState indexMeta) {
        if (OrderDirection.isAscending(indexMeta)) {
            return STORE;
        } else {
            return REVERSED_STORE;
        }
    }
    
    public boolean isAscending(NodeState root, String propertyName,
            Filter filter) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        return OrderDirection.isAscending(indexMeta);
    }

    /**
     * retrieve the type of the index
     *
     * @return the type
     */
    String getType() {
        return OrderedIndex.TYPE;
    }

    public double getCost(Filter filter, String propertyName, PropertyValue value) {
        double cost = Double.POSITIVE_INFINITY;
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta != null) {
            // we relay then on the standard property index for the cost
            cost = COST_OVERHEAD
                   + getStrategy(indexMeta).count(indexMeta, PropertyIndex.encode(value), MAX_COST);
        }
        return cost;
    }

    public Iterable<String> query(Filter filter, String propertyName, PropertyValue value) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        return getStrategy(indexMeta).query(filter, propertyName, indexMeta, encode(value));
    }

    /**
     * query the strategy for the provided constrains
     * 
     * @param filter
     * @param propertyName
     * @param pr
     * @return the result set
     */
    public Iterable<String> query(Filter filter, String propertyName, PropertyRestriction pr) {
        NodeState indexMeta = getIndexNode(root, propertyName, filter);
        if (indexMeta == null) {
            throw new IllegalArgumentException("No index for " + propertyName);
        }
        return getStrategy(indexMeta).query(filter, propertyName, indexMeta, pr);
    }

    /**
     * Collect plans for ordered indexes along the given path and order entry.
     *
     * @param filter a filter description.
     * @param path a relative path from this lookup to the filter path.
     * @param oe an order entry.
     * @param plans collected plans are added to this list.
     */
    void collectPlans(Filter filter,
                      String path,
                      QueryIndex.OrderEntry oe,
                      List<QueryIndex.IndexPlan> plans) {
        String propertyName = PathUtils.getName(oe.getPropertyName());
        NodeState definition = getIndexNode(root, propertyName, filter);
        if (definition != null) {
            Order order = OrderDirection.isAscending(definition)
                    ? Order.ASCENDING : Order.DESCENDING;
            long entryCount = getStrategy(definition).count(definition, (PropertyRestriction) null, MAX_COST);
            QueryIndex.IndexPlan.Builder b = OrderedPropertyIndex.getIndexPlanBuilder(filter);
            b.setSortOrder(ImmutableList.of(new QueryIndex.OrderEntry(oe.getPropertyName(), Type.UNDEFINED, order)));
            b.setEstimatedEntryCount(entryCount);
            b.setDefinition(definition);
            b.setPathPrefix(getPath());
            QueryIndex.IndexPlan plan = b.build();
            LOG.debug("plan: {}", plan);
            plans.add(plan);
        }
        // walk down path
        String remainder = "";
        OrderedPropertyIndexLookup lookup = null;
        for (String element : PathUtils.elements(path)) {
            if (lookup == null) {
                lookup = new OrderedPropertyIndexLookup(
                        root.getChildNode(element), element, this);
            } else {
                remainder = PathUtils.concat(remainder, element);
            }
        }
        if (lookup != null) {
            lookup.collectPlans(filter, remainder, oe, plans);
        }
    }

    /**
     * Collect plans for ordered indexes along the given path and property
     * restriction.
     *
     * @param filter a filter description.
     * @param path a relative path from this lookup to the filter path.
     * @param pr a property restriction.
     * @param plans collected plans are added to this list.
     */
    void collectPlans(Filter filter,
                      String path,
                      PropertyRestriction pr,
                      List<QueryIndex.IndexPlan> plans) {
        String propertyName = PathUtils.getName(pr.propertyName);
        NodeState definition = getIndexNode(root, propertyName, filter);
        if (definition != null) {
            PropertyValue value = null;
            boolean createPlan = false;
            if (pr.first == null && pr.last == null) {
                // open query: [property] is not null
                value = null;
                createPlan = true;
            } else if (pr.first != null && pr.first.equals(pr.last) && pr.firstIncluding
                    && pr.lastIncluding) {
                // [property]=[value]
                value = pr.first;
                createPlan = true;
            } else if (pr.first != null && !pr.first.equals(pr.last)) {
                // '>' & '>=' use cases
                value = pr.first;
                createPlan = true;
            } else if (pr.last != null && !pr.last.equals(pr.first)) {
                // '<' & '<='
                value = pr.last;
                createPlan = true;
            }
            if (createPlan) {
                // we always return a sorted set
                Order order = OrderDirection.isAscending(definition)
                        ? Order.ASCENDING : Order.DESCENDING;
                QueryIndex.IndexPlan.Builder b = OrderedPropertyIndex.getIndexPlanBuilder(filter);
                b.setDefinition(definition);
                b.setSortOrder(ImmutableList.of(new QueryIndex.OrderEntry(
                        propertyName, Type.UNDEFINED, order)));
                long count = getStrategy(definition).count(definition, pr, MAX_COST);
                b.setEstimatedEntryCount(count);
                b.setPropertyRestriction(pr);
                b.setPathPrefix(getPath());

                QueryIndex.IndexPlan plan = b.build();
                LOG.debug("plan: {}", plan);
                plans.add(plan);
            }
        }
        // walk down path
        String remainder = "";
        OrderedPropertyIndexLookup lookup = null;
        for (String element : PathUtils.elements(path)) {
            if (lookup == null) {
                lookup = new OrderedPropertyIndexLookup(
                        root.getChildNode(element), element, this);
            } else {
                remainder = PathUtils.concat(remainder, element);
            }
        }
        if (lookup != null) {
            lookup.collectPlans(filter, remainder, pr, plans);
        }
    }

    private String getPath() {
        return buildPath(new StringBuilder()).toString();
    }

    private StringBuilder buildPath(StringBuilder sb) {
        if (parent != null) {
            parent.buildPath(sb);
            sb.append("/").append(name);
        }
        return sb;
    }
}