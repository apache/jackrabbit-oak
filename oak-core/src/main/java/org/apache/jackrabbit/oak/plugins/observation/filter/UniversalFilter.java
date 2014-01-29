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
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * An universal {@code Filter} implementation, which can be parametrised by
 * a {@link Selector} and a {@code Predicate}. The selector maps a call back
 * on this filter to a {@code NodeState}. That node state is in turn passed
 * to the predicate for determining whether to include or to exclude the
 * respective event.
 */
public class UniversalFilter implements EventFilter {
    private final NodeState beforeState;
    private final NodeState afterState;
    private final Selector selector;
    private final Predicate<NodeState> predicate;

    /**
     * Create a new instance of an universal filter rooted at the passed trees.
     *
     * @param before          before state
     * @param after           after state
     * @param selector        selector for selecting the tree to match the predicate against
     * @param predicate       predicate for determining whether to include or to exclude an event
     */
    public UniversalFilter(
            @Nonnull NodeState before, @Nonnull NodeState after,
            @Nonnull Selector selector, @Nonnull Predicate<NodeState> predicate) {
        this.beforeState = checkNotNull(before);
        this.afterState = checkNotNull(after);
        this.predicate = checkNotNull(predicate);
        this.selector = checkNotNull(selector);
    }

    /**
     * A selector instance maps call backs on {@code Filters} to {@code NodeState} instances,
     * which should be used for determining inclusion or exclusion of the associated event.
     */
    public interface Selector {

        /**
         * Map a property event.
         * @param filter  filter instance on which respective call back occurred.
         * @param before  before state or {@code null} for
         *                {@link EventFilter#includeAdd(PropertyState)}
         * @param after   after state or {@code null} for
         *                {@link EventFilter#includeDelete(PropertyState)}
         * @return a {@code NodeState} instance for basing the filtering criterion (predicate) upon
         */
        @Nonnull
        NodeState select(@Nonnull UniversalFilter filter,
                @CheckForNull PropertyState before, @CheckForNull PropertyState after);

        /**
         * Map a node event.
         * @param filter  filter instance on which respective call back occurred.
         * @param name    name of the child node state
         * @param before  before state or {@code null} for
         *                {@link EventFilter#includeAdd(String, NodeState)}
         * @param after   after state or {@code null} for
         *                {@link EventFilter#includeDelete(String, NodeState)}
         * @return a NodeState instance for basing the filtering criterion (predicate) upon
         */
        @Nonnull
        NodeState select(@Nonnull UniversalFilter filter, @Nonnull String name,
                @Nonnull NodeState before, @Nonnull NodeState after);
    }

    /**
     * @return  before state for this filter
     */
    @Nonnull
    public NodeState getBeforeState() {
        return beforeState;
    }

    /**
     * @return  after state for this filter
     */
    @Nonnull
    public NodeState getAfterState() {
        return afterState;
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
    public boolean includeDelete(String name, NodeState before) {
        return predicate.apply(selector.select(this, name, before, MISSING_NODE));
    }

    @Override
    public boolean includeMove(String sourcePath, String name, NodeState moved) {
        return predicate.apply(selector.select(this, name, MISSING_NODE, moved));
    }

    @Override
    public boolean includeReorder(String destName, String name, NodeState reordered) {
        return predicate.apply(selector.select(this, name, MISSING_NODE, reordered));
    }

    @Override
    public EventFilter create(String name, NodeState before, NodeState after) {
        return new UniversalFilter(
                beforeState.getChildNode(name), afterState.getChildNode(name),
                selector, predicate);
    }
}
