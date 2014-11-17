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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.toArray;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

class RelativeProperty {
    final String propertyPath;
    final String parentPath;
    final String name;
    private final PropertyDefinition propertyDefinition;
    /**
     * Stores the parent path element in reverse order
     * parentPath -> foo/bar/baz -> [baz, bar, foo]
     */
    final String[] ancestors;

    public static boolean isRelativeProperty(String propertyName){
        return !isAbsolute(propertyName) && PathUtils.getNextSlash(propertyName, 0) > 0;
    }

    public RelativeProperty(String propertyPath){
        this(propertyPath, null);
    }

    public RelativeProperty(String propertyPath, PropertyDefinition pd){
        this.propertyPath = propertyPath;
        name = PathUtils.getName(propertyPath);
        parentPath = PathUtils.getParentPath(propertyPath);
        ancestors = computeAncestors(parentPath);
        this.propertyDefinition = pd;
    }

    /**
     * The first index definition format supported relative property definition
     * by replicating the property path in 'properties' node. Later version
     * supported property definition as immediated child node of 'properties' node
     * where the relative property path is specified as part of 'name' property
     */
    @Deprecated
    @Nonnull
    public NodeState getPropDefnNode(NodeState propNode) {
        NodeState result = propNode;
        for (String name : elements(propertyPath)){
            result = result.getChildNode(name);
        }
        return result;
    }

    @CheckForNull
    public PropertyState getProperty(NodeState state) {
        NodeState node = state;
        for (String name : elements(parentPath)){
            node = node.getChildNode(name);
            if (!node.exists()){
                return null;
            }
        }
        return node.exists() ? node.getProperty(name) : null;
    }

    public PropertyDefinition getPropertyDefinition() {
        return checkNotNull(propertyDefinition, "Property definition not specified");
    }

    @Override
    public String toString() {
        return propertyPath;
    }

    private String[] computeAncestors(String parentPath) {
        return toArray(copyOf(elements(parentPath)).reverse(), String.class);
    }
}
