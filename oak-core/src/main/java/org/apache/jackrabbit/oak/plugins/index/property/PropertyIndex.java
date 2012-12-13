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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

/**
 * Provides a QueryIndex that does lookups against a property index
 * 
 * <p>
 * To define a property index on a subtree you have to add an <code>oak:index</code> node.
 * 
 * Under it follows the index definition node that:
 * <ul>
 * <li>must be of type <code>oak:queryIndexDefinition</code></li>
 * <li>must have the <code>type</code> property set to <b><code>property</code></b></li>
 * <li>contains the <code>propertyNames</code> property that indicates what property will be stored in the index</li>
 * </ul>
 * </p>
 * <p>
 * Optionally you can specify the uniqueness constraint on a property index by
 * setting the <code>unique</code> flag to <code>true</code>.
 * </p>
 * 
 * <p>
 * Note: <code>propertyNames</code> can be a list of properties, and it is optional.in case it is missing, the node name will be used as a property name reference value
 * </p>
 * 
 * <p>
 * Note: <code>reindex</code> is a property that when set to <code>true</code>, triggers a full content reindex.
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeBuilder index = root.child("oak:index");
 *     index.child("uuid")
 *         .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
 *         .setProperty("type", "property")
 *         .setProperty("propertyNames", "jcr:uuid")
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

    public static final String TYPE = "property";

    private static final int MAX_STRING_LENGTH = 100; // TODO: configurable

    static List<String> encode(PropertyValue value) {
        List<String> values = new ArrayList<String>();

        for (String v : value.getValue(Type.STRINGS)) {
            try {
                if (v.length() > MAX_STRING_LENGTH) {
                    v = v.substring(0, MAX_STRING_LENGTH);
                }
                values.add(URLEncoder.encode(v, Charsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 is unsupported", e);
            }
        }
        return values;
    }


    //--------------------------------------------------------< QueryIndex >--

    @Override
    public String getIndexName() {
        return "oak:index";
    }

    @Override
    public double getCost(Filter filter, NodeState root) {
        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (pr.firstIncluding && pr.lastIncluding
                    && pr.first.equals(pr.last) // TODO: range queries
                    && lookup.isIndexed(pr.propertyName, "/")) { // TODO: path
                return lookup.getCost(pr.propertyName, pr.first);
            }
        }
        // not an appropriate index
        return Double.POSITIVE_INFINITY;
    }

    @Override
    public Cursor query(Filter filter, NodeState root) {
        Set<String> paths = null;

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        for (PropertyRestriction pr : filter.getPropertyRestrictions()) {
            if (pr.firstIncluding && pr.lastIncluding
                    && pr.first.equals(pr.last) // TODO: range queries
                    && lookup.isIndexed(pr.propertyName, "/")) { // TODO: path
                Set<String> set = lookup.find(pr.propertyName, pr.first);
                if (paths == null) {
                    paths = Sets.newHashSet(set);
                } else {
                    paths.retainAll(set);
                }
            }
        }

        if (paths == null) {
            throw new IllegalStateException("Property index is used even when no index is available for filter " + filter);
        }
        return Cursors.newPathCursor(paths);
    }

    @Override
    public String getPlan(Filter filter, NodeState root) {
        return "oak:index"; // TODO: better plans
    }
}