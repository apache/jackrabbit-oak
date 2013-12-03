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
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An universal {@code Filter} implementation, which can be parametrised by
 * a {@link Selector} and a {@code Predicate}. The selector maps a call back
 * on this filter to a tree. That tree is in turn passed to the predicate for
 * determining whether to include or to exclude the respective event.
 */
public class UniversalFilter implements Filter {
    private final Tree beforeTree;
    private final Tree afterTree;

    private final Selector selector;
    private final Predicate<Tree> predicate;

    /**
     * Create a new instance of an universal filter rooted at the passed trees.
     *
     * @param beforeRootTree  root of the before tree
     * @param afterRootTree   root of the after tree
     * @param selector        selector for selecting the tree to match the predicate against
     * @param predicate       predicate for determining whether to include or to exclude an event
     */
    public UniversalFilter(
            @Nonnull Tree beforeRootTree,
            @Nonnull Tree afterRootTree,
            @Nonnull Selector selector,
            @Nonnull Predicate<Tree> predicate) {
        this.predicate = checkNotNull(predicate);
        this.beforeTree = checkNotNull(beforeRootTree);
        this.afterTree = checkNotNull(afterRootTree);
        this.selector = checkNotNull(selector);
    }

    /**
     * A selector instance maps call backs on {@code Filters} to tree instances,
     * which should be used for determining inclusion or exclusion of the associated event.
     */
    public interface Selector {

        /**
         * Map a property event.
         * @param filter  filter instance on which respective call back occurred.
         * @param before  before state or {@code null} for
         *                {@link Filter#propertyAdded(PropertyState)}
         * @param after   after state or {@code null} for
         *                {@link Filter#propertyDeleted(PropertyState)}
         * @return a tree instance for basing the filtering criterion (predicate) upon
         */
        @Nonnull
        Tree select(@Nonnull UniversalFilter filter,
                @CheckForNull PropertyState before, @CheckForNull PropertyState after);

        /**
         * Map a node event.
         * @param filter  filter instance on which respective call back occurred.
         * @param before  before state or {@code null} for
         *                {@link Filter#childNodeAdded(String, NodeState)}
         * @param after   after state or {@code null} for
         *                {@link Filter#childNodeDeleted(String, NodeState)}
         * @return a tree instance for basing the filtering criterion (predicate) upon
         */
        @Nonnull
        Tree select(@Nonnull UniversalFilter filter, @Nonnull String name,
                @Nonnull NodeState before, @Nonnull NodeState after);
    }

    /**
     * @return  before tree this filter acts upon
     */
    public Tree getBeforeTree() {
        return beforeTree;
    }

    /**
     * @return  after tree this filter acts upon
     */
    public Tree getAfterTree() {
        return afterTree;
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return predicate.apply(selector.select(this, null, after));
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return predicate.apply(selector.select(this, before, after));
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return predicate.apply(selector.select(this, before, null));
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return predicate.apply(selector.select(this, name, MISSING_NODE, after));
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return predicate.apply(selector.select(this, name, before, after));
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return predicate.apply(selector.select(this, name, before, MISSING_NODE));
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return predicate.apply(selector.select(this, name, MISSING_NODE, moved));
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        return new UniversalFilter(beforeTree.getChild(name), afterTree.getChild(name),
                selector, predicate);
    }
}
