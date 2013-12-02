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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Common {@code Selector} implementations
 */
public final class Selectors {

    /**
     * A selector returning the node for which a call back on {@link EventGenerator.Filter} was
     * called or a non existing node in the case of a property call back.
     */
    public static final Selector THIS = new ThisSelector();

    /**
     * A selector returning the parent node of the item for which a call back
     * on {@link EventGenerator.Filter} was called.
     */
    public static final Selector PARENT = new ParentSelector();

    private Selectors() {
    }

    private static class ThisSelector implements Selector {
        @Override
        public ImmutableTree select(@Nonnull UniversalFilter filter,
                @CheckForNull PropertyState before, @CheckForNull PropertyState after) {
            return new ImmutableTree(MISSING_NODE);
        }

        @Override
        public ImmutableTree select(@Nonnull UniversalFilter filter, @Nonnull String name,
                @Nonnull NodeState before, @Nonnull NodeState after) {
            return after.exists()
                    ? filter.getAfterTree().getChild(name)
                    : filter.getBeforeTree().getChild(name);
        }
    }

    private static class ParentSelector implements Selector {
        @Override
        public ImmutableTree select(@Nonnull UniversalFilter filter,
                @CheckForNull PropertyState before, @CheckForNull PropertyState after) {
            return after != null
                    ? filter.getAfterTree()
                    : filter.getBeforeTree();
        }

        @Override
        public ImmutableTree select(@Nonnull UniversalFilter filter, @Nonnull String name,
                @Nonnull NodeState before, @Nonnull NodeState after) {
            return after.exists()
                    ? filter.getAfterTree()
                    : filter.getBeforeTree();
        }
    }
}
