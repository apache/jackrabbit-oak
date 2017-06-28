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

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyBuilder;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.Sets;

/**
 * The {@code RepMembersConflictHandler} takes care of merging the {@code rep:members} property
 * during parallel updates.
 *<p>
 * The conflict handler deals with the following conflicts:
 * <ul>
 *     <li>{@code addExistingProperty}  : {@code Resolution.MERGED},</li>
 *     <li>{@code changeDeletedProperty}: {@code Resolution.THEIRS}, removing the members property takes precedence.
 *     <li>{@code changeChangedProperty}: {@code Resolution.MERGED}, merge of the 2 members sets into a single one
 *     <li>{@code deleteChangedProperty}: {@code Resolution.OURS} removing the members property takes precedence.
 * </ul>
 */
public class RepMembersConflictHandler implements ThreeWayConflictHandler {

    @Nonnull
    @Override
    public Resolution addExistingProperty(@Nonnull NodeBuilder parent, @Nonnull PropertyState ours,
            @Nonnull PropertyState theirs) {
        if (isRepMembersProperty(theirs)) {
            mergeChange(parent, ours, theirs,Sets.newHashSet());
            return Resolution.MERGED;
        } else {
            return Resolution.IGNORED;
        }
    }

    @Override
    @Nonnull
    public Resolution changeDeletedProperty(@Nonnull NodeBuilder parent, @Nonnull PropertyState ours,
            @Nonnull PropertyState base) {
        if (isRepMembersProperty(ours)) {
            // removing the members property takes precedence
            return Resolution.THEIRS;
        } else {
            return Resolution.IGNORED;
        }
    }

    @Nonnull
    @Override
    public Resolution changeChangedProperty(@Nonnull NodeBuilder parent, @Nonnull PropertyState ours,
            @Nonnull PropertyState theirs, @Nonnull PropertyState base) {
        if (isRepMembersProperty(theirs)) {
            Set<String> baseMembers = Sets.newHashSet(base.getValue(Type.STRINGS));
            mergeChange(parent, ours, theirs, baseMembers);
            return Resolution.MERGED;
        } else {
             return Resolution.IGNORED;
        }
    }

    @Nonnull
    @Override
    public Resolution deleteDeletedProperty(@Nonnull NodeBuilder parent, @Nonnull PropertyState base) {
        // both are removing the members property, ignoring
        return Resolution.IGNORED;
    }

    @Nonnull
    @Override
    public Resolution deleteChangedProperty(@Nonnull NodeBuilder parent, @Nonnull PropertyState theirs,
            @Nonnull PropertyState base) {
        if (isRepMembersProperty(theirs)) {
            // removing the members property takes precedence
            return Resolution.OURS;
        } else {
            return Resolution.IGNORED;
        }
    }


    @Nonnull
    @Override
    public Resolution addExistingNode(@Nonnull NodeBuilder parent, @Nonnull String name, @Nonnull NodeState ours,
            @Nonnull NodeState theirs) {
        return Resolution.IGNORED;
    }

    @Nonnull
    @Override
    public Resolution changeDeletedNode(@Nonnull NodeBuilder parent, @Nonnull String name, @Nonnull NodeState ours,
            @Nonnull NodeState base) {
        return Resolution.IGNORED;
    }

    @Nonnull
    @Override
    public Resolution deleteChangedNode(@Nonnull NodeBuilder parent, @Nonnull String name, @Nonnull NodeState theirs,
            @Nonnull NodeState base) {
        return Resolution.IGNORED;
    }

    @Nonnull
    @Override
    public Resolution deleteDeletedNode(@Nonnull NodeBuilder parent, @Nonnull String name, @Nonnull NodeState base) {
        return Resolution.IGNORED;
    }

    //----------------------------< internal >----------------------------------

    private static void mergeChange(NodeBuilder parent, PropertyState ours, PropertyState theirs, Set<String> base) {
        PropertyBuilder<String> merged = PropertyBuilder.array(Type.STRING);
        merged.setName(UserConstants.REP_MEMBERS);

        Set<String> theirMembers = Sets.newHashSet(theirs.getValue(Type.STRINGS));
        Set<String> ourMembers = Sets.newHashSet(ours.getValue(Type.STRINGS));

        // merge ours and theirs to a de-duplicated set
        Set<String> combined = Sets.newHashSet(Sets.intersection(ourMembers, theirMembers));
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

}
