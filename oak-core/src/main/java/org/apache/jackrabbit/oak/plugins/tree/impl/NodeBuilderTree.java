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

package org.apache.jackrabbit.oak.plugins.tree.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A mutable {@code Tree} implementation based on an underlying
 * {@code NodeBuilder}, which tracks all changes recorded through
 * this tree's mutator methods.
 */
public final class NodeBuilderTree extends AbstractMutableTree {

    private final NodeBuilderTree parent;

    private final String name;

    private final NodeBuilder nodeBuilder;

    /**
     * Create a new {@code AbstractTree} instance
     *
     * @param nodeBuilder {@code NodeBuilder} for the underlying node state
     * @param name        name of the tree
     */
    public NodeBuilderTree(@NotNull String name, @NotNull NodeBuilder nodeBuilder) {
        this(null, nodeBuilder, name);
    }

    protected NodeBuilderTree(@Nullable NodeBuilderTree parent, @NotNull NodeBuilder nodeBuilder,
           @NotNull String name) {
        this.parent = parent;
        this.name = name;
        this.nodeBuilder = nodeBuilder;
    }

    @Override
    @Nullable
    protected AbstractMutableTree getParentOrNull() {
        return parent;
    }

    @NotNull
    @Override
    protected NodeBuilder getNodeBuilder() {
        return nodeBuilder;
    }

    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @Override
    @NotNull
    protected NodeBuilderTree createChild(@NotNull String name) throws IllegalArgumentException {
        return new NodeBuilderTree(this, nodeBuilder.getChildNode(checkNotNull(name)), name);
    }

}
