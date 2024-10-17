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
package org.apache.jackrabbit.oak.plugins.tree.impl;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Component;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

@Component(service = {TreeProvider.class})
public class TreeProviderService implements TreeProvider {

    @NotNull
    @Override
    public Tree createReadOnlyTree(@NotNull NodeState rootState) {
        return TreeFactory.createReadOnlyTree(rootState);
    }

    @NotNull
    @Override
    public Tree createReadOnlyTree(@NotNull Tree readOnlyParent, @NotNull String childName, @NotNull NodeState childState) {
        return TreeFactory.createReadOnlyTree(readOnlyParent, childName, childState);
    }

    @NotNull
    @Override
    public NodeState asNodeState(@NotNull Tree readOnlyTree) {
        checkArgument(readOnlyTree instanceof AbstractTree);
        return ((AbstractTree) readOnlyTree).getNodeState();
    }
}
