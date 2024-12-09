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
package org.apache.jackrabbit.oak.plugins.tree.factories;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.tree.impl.NodeBuilderTree;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * Factory to obtain {@code Tree} objects from {@code NodeState}s
 * and {@code NodeBuilder}s.
 *
 * @deprecated Please use {@code TreeProvider} instead
 */
public final class TreeFactory {

    private TreeFactory() {}

    public static Tree createTree(@NotNull NodeBuilder builder) {
        return new NodeBuilderTree("", builder);
    }

    public static Tree createReadOnlyTree(@NotNull NodeState rootState) {
        return new ImmutableTree(rootState);
    }

    public static Tree createReadOnlyTree(@NotNull Tree readOnlyParent, @NotNull String childName, @NotNull NodeState childState) {
        checkArgument(readOnlyParent instanceof ImmutableTree);
        return new ImmutableTree((ImmutableTree) readOnlyParent, childName, childState);
    }
}
