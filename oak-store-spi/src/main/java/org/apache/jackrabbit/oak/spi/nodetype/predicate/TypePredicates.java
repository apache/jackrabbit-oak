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
package org.apache.jackrabbit.oak.spi.nodetype.predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.JCR_HASORDERABLECHILDNODES;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.util.Set;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

public class TypePredicates {

    private TypePredicates() {
    }

    @NotNull
    public static Predicate<NodeState> getNodeTypePredicate(@NotNull NodeState root, @NotNull String... names) {
        return new TypePredicate(root, names);
    }

    @NotNull
    public static Predicate<NodeState> getNodeTypePredicate(@NotNull NodeState root, @NotNull Iterable<String> names) {
        return new TypePredicate(root, names);
    }

    @NotNull
    public static Predicate<NodeState> isOrderable(@NotNull NodeState root) {
        Set<String> orderable = newHashSet();
        NodeState types = checkNotNull(root).getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
        for (ChildNodeEntry entry : types.getChildNodeEntries()) {
            NodeState type = entry.getNodeState();
            if (type.getBoolean(JCR_HASORDERABLECHILDNODES)) {
                orderable.add(entry.getName());
            }
        }
        return new TypePredicate(root, orderable);
    }
}
