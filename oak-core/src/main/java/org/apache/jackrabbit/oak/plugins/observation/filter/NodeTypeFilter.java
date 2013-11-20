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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.LazyValue;

/**
 * {@code EventTypeFilter} filters based on the node type of the
 * <em>associated parent node</em> as defined by
 * {@link javax.jcr.observation.ObservationManager#addEventListener(
        javax.jcr.observation.EventListener, int, String, boolean, String[], String[], boolean)
        ObservationManager.addEventListener()}.
 */
public class NodeTypeFilter implements Filter {
    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;
    private final ReadOnlyNodeTypeManager ntManager;
    private final String[] ntNames;

    private final LazyValue<Boolean> include = new LazyValue<Boolean>() {
        @Override
        protected Boolean createValue() {
            ImmutableTree associatedParent = afterTree.exists()
                    ? afterTree
                    : beforeTree;

            for (String ntName : ntNames) {
                if (ntManager.isNodeType(associatedParent, ntName)) {
                    return true;
                }
            }
            return false;
        }
    };

    /**
     * Create a new {@code Filter} instance that includes an event when the type of the
     * associated parent node is of one of the node types in {@code ntNames}.
     *
     * @param beforeTree  associated parent before state
     * @param afterTree   associated parent after state
     * @param ntManager   node type manager used to determine type inhabitation
     * @param ntNames     node type names to match
     */
    public NodeTypeFilter(@Nonnull ImmutableTree beforeTree, @Nonnull ImmutableTree afterTree,
            @Nonnull ReadOnlyNodeTypeManager ntManager, @Nonnull String[] ntNames) {
        this.beforeTree = checkNotNull(beforeTree);
        this.afterTree = checkNotNull(afterTree);
        this.ntManager = checkNotNull(ntManager);
        this.ntNames = checkNotNull(ntNames);
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return include.get();
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return include.get();
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return include.get();
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return include.get();
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return true;
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return include.get();
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return include.get();
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        return new NodeTypeFilter(
                beforeTree.getChild(name), afterTree.getChild(name), ntManager, ntNames);
    }

}
