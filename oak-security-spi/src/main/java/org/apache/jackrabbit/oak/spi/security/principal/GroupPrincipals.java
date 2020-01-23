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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.jetbrains.annotations.NotNull;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

/**
 * Helper class to deal with the migration between the 2 types of groups
 *
 */
public final class GroupPrincipals {

    private static final GroupTransformer TRANSFORMER = new GroupTransformer();

    private GroupPrincipals() {
    }

    /**
     * Checks if the provided principal is a group.
     *
     * @param principal
     *            to be checked.
     *
     * @return true if the principal is of type group.
     */
    public static boolean isGroup(@NotNull Principal principal) {
        return principal instanceof Group || principal instanceof GroupPrincipal;
    }

    /**
     * Returns an enumeration of the members in the group.
     * @param principal the principal whose membership is listed.
     * @return an enumeration of the group members.
     */
    @NotNull
    public static Enumeration<? extends Principal> members(@NotNull Principal principal) {
        if (principal instanceof Group) {
            return ((Group) principal).members();
        }
        if (principal instanceof GroupPrincipal) {
            return ((GroupPrincipal) principal).members();
        }
        return Collections.emptyEnumeration();
    }

    /**
     * Returns true if the passed principal is a member of the group.
     * @param principal the principal whose members are being checked.
     * @param member the principal whose membership is to be checked.
     * @return true if the principal is a member of this group, false otherwise.
     */
    public static boolean isMember(@NotNull Principal principal, @NotNull Principal member) {
        if (principal instanceof Group) {
            return ((Group) principal).isMember(member);
        }
        if (principal instanceof GroupPrincipal) {
            return ((GroupPrincipal) principal).isMember(member);
        }
        return false;
    }

    @NotNull
    public static Set<Principal> transform(@NotNull Set<Group> groups) {
        ImmutableSet.Builder<Principal> g2 = ImmutableSet.builder();
        for (Group g : groups) {
            g2.add(new GroupPrincipalWrapper(g));
        }
        return g2.build();
    }

    @NotNull
    public static Enumeration<? extends Principal> transform(@NotNull Enumeration<? extends Principal> members) {
        Iterator<Principal> m2 = Iterators.transform(Iterators.forEnumeration(members), TRANSFORMER);
        return Iterators.asEnumeration(m2);
    }

    private static class GroupTransformer implements Function<Principal, Principal> {

        @Override
        public Principal apply(Principal input) {
            if (input instanceof Group) {
                return new GroupPrincipalWrapper((Group) input);
            } else {
                return input;
            }
        }
    }
}
