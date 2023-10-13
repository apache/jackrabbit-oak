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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;

/**
 * <code>GroupMembershipReader</code>...
 */
class GroupMembershipReader {

    private final MembershipProvider membershipProvider;

    protected final GroupPrincipalFactory groupPrincipalFactory;

    GroupMembershipReader(@NotNull MembershipProvider membershipProvider,
                          @NotNull GroupPrincipalFactory groupPrincipalFactory) {
        this.membershipProvider = checkNotNull(membershipProvider);
        this.groupPrincipalFactory = checkNotNull(groupPrincipalFactory);
    }

    void getMembership(@NotNull Tree authorizable,
                       @NotNull Set<Principal> groupPrincipals) {
        loadGroupPrincipals(authorizable, groupPrincipals);
    }

    protected final void loadGroupPrincipals(@NotNull Tree authorizableTree,
                                             @NotNull Set<Principal> groupPrincipals) {
        Iterator<Tree> groupTrees = membershipProvider.getMembership(authorizableTree, true);
        while (groupTrees.hasNext()) {
            Tree groupTree = groupTrees.next();
            if (UserUtil.isType(groupTree, AuthorizableType.GROUP)) {
                Principal gr = groupPrincipalFactory.create(groupTree);
                if (gr != null) {
                    groupPrincipals.add(gr);
                }
            }
        }
    }

    interface GroupPrincipalFactory {

        Principal create(@NotNull Tree authorizable);

        Principal create(@NotNull String principalName);
    }

}
