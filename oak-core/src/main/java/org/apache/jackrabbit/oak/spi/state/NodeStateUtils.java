/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Utility method for code that deals with node states.
 */
public final class NodeStateUtils {

    private NodeStateUtils() {
    }

    /**
     * Check whether the node or property with the given name is hidden, that
     * is, if the node name starts with a ":".
     *
     * @param name the node or property name
     * @return true if the item is hidden
     */
    public static boolean isHidden(@Nonnull String name) {
        return !name.isEmpty() && name.charAt(0) == ':';
    }

    /**
     * Check whether the given path contains a hidden node.
     * 
     * @param path the path
     * @return true if one of the nodes is hidden
     */
    public static boolean isHiddenPath(@Nonnull String path) {
        for (String n : PathUtils.elements(path)) {
            if (isHidden(n)) {
                return true;
            }
        }
        return false;
    }

    @CheckForNull
    public static String getPrimaryTypeName(NodeState nodeState) {
        PropertyState ps = nodeState.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        return (ps == null) ? null : ps.getValue(Type.NAME);
    }

    /**
     * Get a possibly non existing child node of a node.
     * @param node  node whose child node to get
     * @param path  path of the child node
     * @return  child node of {@code node} at {@code path}.
     */
    @Nonnull
    public static NodeState getNode(@Nonnull NodeState node, @Nonnull String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            node = node.getChildNode(checkNotNull(name));
        }
        return node;
    }

}
