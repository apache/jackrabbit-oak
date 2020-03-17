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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;

/**
 * Provides a QueryIndex that does lookups against a property index
 *
 * <p>
 * To define a property index on a subtree you have to add an <code>oak:index</code> node.
 * <br>
 * Next (as a child node) follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:QueryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>property</code></b></li>
 * <li>contains the <code>propertyNames</code> property that indicates what property will be stored in the index</li>
 * </ul>
 * </p>
 * <p>
 * Optionally you can specify
 * <ul> 
 * <li> a uniqueness constraint on a property index by setting the <code>unique</code> flag to <code>true</code></li>
 * <li> that the property index only applies to a certain node type by setting the <code>declaringNodeTypes</code> property</li>
 * </ul>
 * </p>
 * <p>
 * Notes:
 * <ul>
 * <li> <code>propertyNames</code> can be a list of properties, and it is optional.in case it is missing, the node name will be used as a property name reference value</li>
 * <li> <code>reindex</code> is a property that when set to <code>true</code>, triggers a full content reindex.</li>
 * </ul>
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("uuid")
 *         .setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME)
 *         .setProperty("type", "property")
 *         .setProperty("propertyNames", "jcr:uuid")
 *         .setProperty("declaringNodeTypes", "mix:referenceable")
 *         .setProperty("unique", true)
 *         .setProperty("reindex", true);
 * }
 * </code>
 * </pre>
 * 
 * @see QueryIndex
 * @see PropertyIndexLookup
 */
class PropertyIndex implements QueryIndex {

    private static final String PROPERTY = "property";

    private static final Logger LOG = LoggerFactory.getLogger(PropertyIndex.class);

    private final MountInfoProvider mountInfoProvider;

    /**
     * Cached property index plan
     */
    private PropertyIndexPlan plan;

    PropertyIndex(MountInfoProvider mountInfoProvider) {
        this.mountInfoProvider = mountInfoProvider;
    }

    private PropertyIndexPlan getPlan(NodeState root, Filter filter) {
        // Reuse cached plan if the filter is the same (which should always be the case). The filter is compared as a
        // string because it would not be possible to use its equals method since the preparing flag would be different
        // and creating a separate isSimilar method is not worth the effort since it would not be used anymore once the
        // PropertyIndex has been refactored to an AdvancedQueryIndex (which will make the plan cache obsolete).
        PropertyIndexPlan plan = this.plan;
        if (plan != null && plan.getFilter().toString().equals(filter.toString())) {
            return plan;
        } else {
            plan = createPlan(root, filter, mountInfoProvider);
            this.plan = plan;
            return plan;
        }
    }

    private static PropertyIndexPlan createPlan(NodeState root, Filter filter,
                                                MountInfoProvider mountInfoProvider) {
        PropertyIndexPlan bestPlan = null;

        // TODO support indexes on a path
        // currently, only indexes on the root node are supported
        NodeState state = root.getChildNode(INDEX_DEFINITIONS_NAME);
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            NodeState definition = entry.getNodeState();
            if (wrongIndex(entry, filter)) {
                continue;
            }
            if (PROPERTY.equals(definition.getString(TYPE_PROPERTY_NAME))
                    && definition.hasChildNode(INDEX_CONTENT_NODE_NAME)) {
                PropertyIndexPlan plan = new PropertyIndexPlan(
                        entry.getName(), root, definition, filter, mountInfoProvider);
                if (plan.getCost() != Double.POSITIVE_INFINITY) {
                    LOG.debug("property cost for {} is {}",
                            plan.getName(), plan.getCost());
                    if (bestPlan == null || plan.getCost() < bestPlan.getCost()) {
                        bestPlan = plan;
                        // Stop comparing if the costs are the minimum
                        if (plan.getCost() == PropertyIndexPlan.COST_OVERHEAD) {
                            break;
                        }
                    }
                }
            }
        }

        return bestPlan;
    }
    
    private static boolean wrongIndex(ChildNodeEntry entry, Filter filter) {
        // REMARK: similar code is used in oak-lucene, IndexPlanner
        // skip index if "option(index ...)" doesn't match
        PropertyRestriction indexName = filter.getPropertyRestriction(IndexConstants.INDEX_NAME_OPTION);
        boolean wrong = false;
        if (indexName != null && indexName.first != null) {
            String name = indexName.first.getValue(Type.STRING);
            String thisName = entry.getName();
            if (thisName.equals(name)) {
                // index name specified, and matches
                return false;
            }
            wrong = true;
        }
        PropertyRestriction indexTag = filter.getPropertyRestriction(IndexConstants.INDEX_TAG_OPTION);
        if (indexTag != null && indexTag.first != null) {
            // index tag specified
            NodeState definition = entry.getNodeState();
            String[] tags = getOptionalStrings(definition, IndexConstants.INDEX_TAGS);
            if (tags == null) {
                // no tag
                return true;
            }
            String tag = indexTag.first.getValue(Type.STRING);
            for(String t : tags) {
                if (t.equals(tag)) {
                    // tag matches
                    return false;
                }
            }
            // no tag matches
            return true;
        }
        // no tag specified
        return wrong;
    }
    
    private static String[] getOptionalStrings(NodeState defn, String propertyName) {
        PropertyState ps = defn.getProperty(propertyName);
        if (ps != null) {
            return Iterables.toArray(ps.getValue(Type.STRINGS), String.class);
        }
        return null;
    }

    //--------------------------------------------------------< QueryIndex >--

    @Override
    public double getMinimumCost() {
        return PropertyIndexPlan.COST_OVERHEAD;
    }

    @Override
    public String getIndexName() {
        return PROPERTY;
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.containsNativeConstraint()) {
            // not an appropriate index for native search
            return Double.POSITIVE_INFINITY;
        }
        if (filter.getPropertyRestrictions().isEmpty()) {
            // not an appropriate index for no property restrictions & selector constraints
            return Double.POSITIVE_INFINITY;
        }

        PropertyIndexPlan plan = getPlan(root, filter);
        if (plan != null) {
            return plan.getCost();
        } else {
            return Double.POSITIVE_INFINITY;
        }
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        PropertyIndexPlan plan = getPlan(root, filter);
        checkState(plan != null,
                "Property index is used even when no index"
                + " is available for filter " + filter);
        return plan.execute();
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        PropertyIndexPlan plan = getPlan(root, filter);
        if (plan != null) {
            return plan.toString();
        } else {
            return "property index not applicable";
        }
    }

}