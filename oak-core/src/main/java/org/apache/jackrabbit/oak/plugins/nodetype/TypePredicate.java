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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.JcrConstants.JCR_ISMIXIN;
import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_MIXIN_SUBTYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.OAK_PRIMARY_SUBTYPES;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Predicate;

/**
 * Inheritance-aware node type predicate for {@link NodeState node states}.
 *
 * @since Oak 0.11
 */
public class TypePredicate implements Predicate<NodeState> {

    private final Set<String> primaryTypes = newHashSet();

    private final Set<String> mixinTypes = newHashSet();

    /**
     * Creates a predicate for checking whether a node state is an instance of
     * the named node type. This is an O(1) operation in terms of item
     * accesses.
     *
     * @param root root node state
     * @param name Oak name of the node type to check for
     */
    public TypePredicate(@Nonnull NodeState root, @Nonnull String name) {
        NodeState types = checkNotNull(root)
                .getChildNode(JCR_SYSTEM)
                .getChildNode(JCR_NODE_TYPES);
        addNodeType(types, checkNotNull(name));
    }

    /**
     * Creates a predicate for checking whether a node state is an instance of
     * any of the named node types. This is an O(n) operation in terms of item
     * accesses, with n being the number of given node types.
     *
     * @param root root node state
     * @param names Oak names of the node types to check for
     */
    public TypePredicate(
            @Nonnull NodeState root, @Nonnull Iterable<String> names) {
        NodeState types = checkNotNull(root)
                .getChildNode(JCR_SYSTEM)
                .getChildNode(JCR_NODE_TYPES);
        for (String name : checkNotNull(names)) {
            addNodeType(types, name);
        }
    }

    private void addNodeType(NodeState types, String name) {
        NodeState type = types.getChildNode(name);
        addAll(primaryTypes, type.getNames(OAK_PRIMARY_SUBTYPES));
        if (type.getBoolean(JCR_ISMIXIN)) {
            mixinTypes.add(name);
            // Only mixin types can have mixin descendants, so we
            // only fill the mixinTypes set in this branch of code.
            addAll(mixinTypes, type.getNames(OAK_MIXIN_SUBTYPES));
        } else {
            // No need to check whether the type actually exists, as if
            // it doesn't there should in any case be no matching content.
            primaryTypes.add(name);
        }
    }

    //---------------------------------------------------------< Predicate >--

    @Override
    public boolean apply(NodeState input) {
        return primaryTypes.contains(input.getName(JCR_PRIMARYTYPE))
                || any(input.getNames(JCR_MIXINTYPES), in(mixinTypes));
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return union(primaryTypes, mixinTypes).toString();
    }

}
