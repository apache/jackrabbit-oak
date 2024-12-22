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
package org.apache.jackrabbit.oak.security.user;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import org.apache.jackrabbit.guava.common.collect.Sets;

/**
 * The {@code RepMembersConflictHandler} takes care of merging the {@code rep:members} property
 * during parallel updates.
 *<p>
 * The conflict handler deals with the following conflicts:
 * <ul>
 *     <li>{@code addExistingProperty}  : {@code Resolution.MERGED}.</li>
 *     <li>{@code changeDeletedProperty}: {@code Resolution.THEIRS}, removing the members property takes precedence.
 *     <li>{@code changeChangedProperty}: {@code Resolution.MERGED}, merge of the 2 members sets into a single one
 *     <li>{@code deleteChangedProperty}: {@code Resolution.OURS} removing the members property takes precedence.
 *     <li>{@code deleteDeletedProperty}: {@code Resolution.MERGED}.</li>
 *     <li>{@code changeDeletedNode}    : {@code Resolution.THEIRS}, removal takes precedence.</li>
 *     <li>{@code deleteChangedNode}    : {@code Resolution.OURS}, removal takes precedence.</li>
 *     <li>{@code deleteDeletedNode}    : {@code Resolution.MERGED}.</li>
 * </ul>
 */
class RepMembersConflictHandler implements ThreeWayConflictHandler {

    @NotNull
    @Override
    public Resolution addExistingProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState theirs) {
        if (isRepMembersProperty(theirs)) {
            mergeChange(parent, ours, theirs, Set.of());
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    @Override
    @NotNull
    public Resolution changeDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState base) {
        if (isRepMembersProperty(ours)) {
            // removing the members property takes precedence
            return Resolution.THEIRS;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution changeChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState ours,
            @NotNull PropertyState theirs, @NotNull PropertyState base) {
        if (isRepMembersProperty(theirs)) {
            Set<String> baseMembers = CollectionUtils.toSet(base.getValue(Type.STRINGS));
            mergeChange(parent, ours, theirs, baseMembers);
            return Resolution.MERGED;
        } else {
             return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution deleteDeletedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState base) {
        if (isRepMembersProperty(base)) {
            // both are removing the members property
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution deleteChangedProperty(@NotNull NodeBuilder parent, @NotNull PropertyState theirs,
            @NotNull PropertyState base) {
        if (isRepMembersProperty(theirs)) {
            // removing the members property takes precedence
            return Resolution.OURS;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution addExistingNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState theirs) {
        return Resolution.IGNORED;
    }

    @NotNull
    @Override
    public Resolution changeDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState ours,
            @NotNull NodeState base) {
        if (isMemberRefType(base)) {
            return Resolution.THEIRS;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution deleteChangedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState theirs,
            @NotNull NodeState base) {
        if (isMemberRefType(base)) {
            return Resolution.OURS;
        } else {
            return Resolution.IGNORED;
        }
    }

    @NotNull
    @Override
    public Resolution deleteDeletedNode(@NotNull NodeBuilder parent, @NotNull String name, @NotNull NodeState base) {
        if (isMemberRefType(base)) {
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    //----------------------------< internal >----------------------------------

    private static void mergeChange(@NotNull NodeBuilder parent, @NotNull PropertyState ours, @NotNull PropertyState theirs, @NotNull Set<String> base) {
        PropertyBuilder<String> merged = PropertyBuilder.array(Type.WEAKREFERENCE);
        merged.setName(UserConstants.REP_MEMBERS);

        Set<String> theirMembers = CollectionUtils.toSet(theirs.getValue(Type.STRINGS));
        Set<String> ourMembers = CollectionUtils.toSet(ours.getValue(Type.STRINGS));

        // merge ours and theirs to a de-duplicated set
        Set<String> combined = new LinkedHashSet<>(Sets.intersection(ourMembers, theirMembers));
        for (String m : Sets.difference(ourMembers, theirMembers)) {
            if (!base.contains(m)) {
                combined.add(m);
            }
        }
        for (String m : Sets.difference(theirMembers, ourMembers)) {
            if (!base.contains(m)) {
                combined.add(m);
            }
        }
        merged.addValues(combined);
        parent.setProperty(merged.getPropertyState());
    }

    private static boolean isRepMembersProperty(PropertyState p) {
        return UserConstants.REP_MEMBERS.equals(p.getName());
    }

    private static boolean isMemberRefType(NodeState base) {
        String type = base.getName(JcrConstants.JCR_PRIMARYTYPE);
        return UserConstants.NT_REP_MEMBER_REFERENCES.equals(type);
    }
}
