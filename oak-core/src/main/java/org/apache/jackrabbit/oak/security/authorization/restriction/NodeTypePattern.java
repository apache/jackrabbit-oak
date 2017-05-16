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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link RestrictionPattern} interface that returns
 * {@code true} if the primary type of the target tree (or the parent of a
 * target property) is contained in the configured node type name. This allows
 * to limit certain operations (e.g. adding or removing a child tree) to
 * nodes with a specific node type.
 */
class NodeTypePattern implements RestrictionPattern {

    private static final Logger log = LoggerFactory.getLogger(NodeTypePattern.class);

    private final Set<String> nodeTypeNames;

    NodeTypePattern(@Nonnull Iterable<String> nodeTypeNames) {
        this.nodeTypeNames = ImmutableSet.copyOf(nodeTypeNames);
    }

    @Override
    public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
        return nodeTypeNames.contains(TreeUtil.getPrimaryTypeName(tree));
    }

    @Override
    public boolean matches(@Nonnull String path) {
        log.debug("Unable to validate node type restriction.");
        return false;
    }

    @Override
    public boolean matches() {
        // node type pattern never matches for repository level permissions
        return false;
    }

    //-------------------------------------------------------------< Object >---
    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        return nodeTypeNames.hashCode();
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        return nodeTypeNames.toString();
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof NodeTypePattern) {
            NodeTypePattern other = (NodeTypePattern) obj;
            return nodeTypeNames.equals(other.nodeTypeNames);
        }
        return false;
    }
}