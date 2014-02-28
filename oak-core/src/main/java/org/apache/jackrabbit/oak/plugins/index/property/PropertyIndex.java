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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;

/**
 * Provides a QueryIndex that does lookups against a property index
 * 
 * <p>
 * To define a property index on a subtree you have to add an <code>oak:index</code> node. <br>
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
 * <li>a uniqueness constraint on a property index by setting the <code>unique</code> flag to <code>true</code></li>
 * <li>that the property index only applies to a certain node type by setting the <code>declaringNodeTypes</code>
 * property</li>
 * </ul>
 * </p>
 * <p>
 * Notes:
 * <ul>
 * <li> <code>propertyNames</code> can be a list of properties, and it is optional.in case it is missing, the node name
 * will be used as a property name reference value</li>
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

    // TODO the max string length should be removed, or made configurable
    private static final int MAX_STRING_LENGTH = 100;

    /**
     * name used when the indexed value is an empty string
     */
    private static final String EMPTY_TOKEN = ":";

    static Set<String> encode(PropertyValue value) {
        if (value == null) {
            return null;
        }
        Set<String> values = new HashSet<String>();
        for (String v : value.getValue(Type.STRINGS)) {
            try {
                if (v.length() > MAX_STRING_LENGTH) {
                    v = v.substring(0, MAX_STRING_LENGTH);
                }
                if (v.isEmpty()) {
                    v = EMPTY_TOKEN;
                } else {
                    v = URLEncoder.encode(v, Charsets.UTF_8.name());
                }
                values.add(v);
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 is unsupported", e);
            }
        }
        return values;
    }

    // --------------------------------------------------------< QueryIndex >--

    @Override
    public String getIndexName() {
        return "property";
    }

    /**
     * return the proper implementation of the Lookup
     * 
     * @param root
     * @return
     */
    PropertyIndexLookup getLookup(NodeState root) {
        return new PropertyIndexLookup(root);
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        if (filter.getFullTextConstraint() != null) {
            // not an appropriate index for full-text search
            return Double.POSITIVE_INFINITY;
        }

        PropertyIndexLookup lookup = getLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propertyName = PathUtils.getName(pr.propertyName);
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(propertyName, "/", filter)) {
                if (pr.firstIncluding && pr.lastIncluding && pr.first != null && pr.first.equals(pr.last)) {
                    // "[property] = $value"
                    return lookup.getCost(filter, propertyName, pr.first);
                } else if (pr.list != null) {
                    double cost = 0;
                    for (PropertyValue p : pr.list) {
                        cost += lookup.getCost(filter, propertyName, p);
                    }
                    return cost;
                } else {
                    // processed as "[property] is not null"
                    return lookup.getCost(filter, propertyName, null);
                }
            }
        }
        // not an appropriate index
        return Double.POSITIVE_INFINITY;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        Iterable<String> paths = null;

        PropertyIndexLookup lookup = getLookup(root);
        int depth = 1;
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propertyName = PathUtils.getName(pr.propertyName);
            depth = PathUtils.getDepth(pr.propertyName);
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(propertyName, "/", filter)) {
                // equality
                if (pr.firstIncluding && pr.lastIncluding && pr.first != null && pr.first.equals(pr.last)) {
                    // "[property] = $value"
                    paths = lookup.query(filter, propertyName, pr.first);
                    break;
                } else if (pr.list != null) {
                    for (PropertyValue pv : pr.list) {
                        Iterable<String> p = lookup.query(filter, propertyName, pv);
                        if (paths == null) {
                            paths = p;
                        } else {
                            paths = Iterables.concat(paths, p);
                        }
                    }
                    break;
                } else {
                    // processed as "[property] is not null"
                    paths = lookup.query(filter, propertyName, null);
                    break;
                }
            }
        }
        if (paths == null) {
            throw new IllegalStateException("Property index is used even when no index is available for filter "
                + filter);
        }
        Cursor c = Cursors.newPathCursor(paths);
        if (depth > 1) {
            c = Cursors.newAncestorCursor(c, depth - 1);
        }
        return c;
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        StringBuilder buff = new StringBuilder("property");
        StringBuilder notIndexed = new StringBuilder();
        PropertyIndexLookup lookup = getLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            String propertyName = PathUtils.getName(pr.propertyName);
            // TODO support indexes on a path
            // currently, only indexes on the root node are supported
            if (lookup.isIndexed(propertyName, "/", filter)) {
                if (pr.firstIncluding && pr.lastIncluding && pr.first != null && pr.first.equals(pr.last)) {
                    buff.append(' ').append(propertyName).append('=').append(pr.first);
                } else {
                    buff.append(' ').append(propertyName);
                }
            } else if (pr.list != null) {
                buff.append(' ').append(propertyName).append(" IN(");
                int i = 0;
                for (PropertyValue pv : pr.list) {
                    if (i++ > 0) {
                        buff.append(", ");
                    }
                    buff.append(pv);
                }
                buff.append(')');
            } else {
                notIndexed.append(' ').append(propertyName);
                if (!pr.toString().isEmpty()) {
                    notIndexed.append(':').append(pr);
                }
            }
        }
        if (notIndexed.length() > 0) {
            buff.append(" (").append(notIndexed.toString().trim()).append(")");
        }
        return buff.toString();
    }

}
