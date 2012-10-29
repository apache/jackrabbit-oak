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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Sets;

/**
 * Is responsible for querying the property index content.
 * 
 * <p>
 * This class can be used directly on a subtree where there is an index defined
 * by supplying a {@link NodeState} root.
 * </p>
 * 
 * <pre>
 * <code>
 * {
 *     NodeState state = ... // get a node state
 *     PropertyIndexLookup lookup = new PropertyIndexLookup(state);
 *     Set<String> hits = lookup.find("foo", PropertyValues.newString("xyz"));
 * }
 * </code>
 * </pre>
 */
public class PropertyIndexLookup {

    private final NodeState root;

    public PropertyIndexLookup(NodeState root) {
        this.root = root;
    }

    /**
     * Checks whether the named properties are indexed somewhere
     * along the given path.
     *
     * @param name property name
     * @param path lookup path
     */
    public boolean isIndexed(String name, String path) {
        NodeState state = root.getChildNode(INDEX_DEFINITIONS_NAME);
        if (state != null) {
            state = state.getChildNode(name);
            if (state != null) {
                return true;
            }
        }

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        int slash = path.indexOf('/');
        if (slash == -1) {
            return false;
        }

        NodeState child = root.getChildNode(path.substring(0, slash));
        return new PropertyIndexLookup(child).isIndexed(
                name, path.substring(slash));
    }

    /**
     * Searches for a given <code>String<code> value within this index.
     * 
     * <p><b>Note</b> if the property you are looking for is not of type <code>String<code>, the converted key value might not match the index key, and there will be no hits on the index.</p>
     * 
     * @param name
     *            the property name
     * @param value
     *            the property value
     * @return the set of matched paths
     */
    public Set<String> find(String name, String value) {
        return find(name, PropertyValues.newString(value));
    }

    /**
     * Searches for a given value within this index.
     * 
     * @param name the property name
     * @param value the property value
     * @return the set of matched paths
     */
    public Set<String> find(String name, PropertyValue value) {
        Set<String> paths = Sets.newHashSet();

        PropertyState property = null;
        NodeState state = root.getChildNode(INDEX_DEFINITIONS_NAME);
        if (state != null) {
            state = state.getChildNode(name);
            if (state != null) {
                state = state.getChildNode(":index");
                if (state != null) {
                    //TODO what happens when I search using an mvp?
                    property = state.getProperty(PropertyIndex.encode(value).get(0));
                }
            }
        }

        if (property != null) {
            // We have an index for this property, so use it
            for (String path : property.getValue(Type.STRINGS)) {
                paths.add(path);
            }
        } else {
            // No index available, so first check this node for a match
            property = root.getProperty(name);
            if (property != null){
                if(PropertyValues.match(property, value)){
                    paths.add("");
                }
            }

            // ... and then recursively look up from the rest of the tree
            for (ChildNodeEntry entry : root.getChildNodeEntries()) {
                String base = entry.getName();
                PropertyIndexLookup lookup =
                        new PropertyIndexLookup(entry.getNodeState());
                for (String path : lookup.find(name, value)) {
                    if (path.isEmpty()) {
                        paths.add(base);
                    } else {
                        paths.add(base + "/" + path);
                    }
                }
            }
        }

        return paths;
    }

}