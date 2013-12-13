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

package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesCurrent;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesParent;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A selector for selecting a node at a relative path from the node selected by
 * an initial selector. Paths respect relative elements (i.e. {@code .} and {@code ..})
 * <em>Note:</em> selecting the parent of the root node will return a non existing
 * {@code Tree} instance.
 */
public class RelativePathSelector implements Selector {
    private static final ImmutableTree MISSING_TREE = new ImmutableTree(MISSING_NODE);

    private final String path;
    private final Selector selector;

    /**
     * @param path      path to select from
     * @param selector  selector to base {@code path} upon
     */
    public RelativePathSelector(@Nonnull String path, @Nonnull Selector selector) {
        this.path = checkNotNull(path);
        this.selector = checkNotNull(selector);
    }

    @Nonnull
    @Override
    public Tree select(@Nonnull UniversalFilter filter,
            @CheckForNull PropertyState before, @CheckForNull PropertyState after) {
        return select(selector.select(filter, before, after));
    }

    @Nonnull
    @Override
    public Tree select(@Nonnull UniversalFilter filter,
            @Nonnull String name, @Nonnull NodeState before, @Nonnull NodeState after) {
        return select(selector.select(filter, name, before, after));
    }

    private Tree select(Tree parent) {
        Tree tree = parent;
        for (String name : elements(path)) {
            if (denotesParent(name)) {
                tree = tree.isRoot() ? MISSING_TREE : tree.getParent();
            } else if (!denotesCurrent(name)) {
                tree = tree.getChild(name);
            }
        }
        return tree;
    }

}
