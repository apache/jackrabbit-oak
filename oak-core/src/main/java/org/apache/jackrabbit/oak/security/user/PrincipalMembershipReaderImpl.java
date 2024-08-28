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

import java.util.HashSet;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;

/**
 * This class has been extracted from {@code UserPrincipalProvider} and calculates membership for a given 
 * user principal.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/OAK-10451">OAK-10451</a>
 */
class PrincipalMembershipReaderImpl implements PrincipalMembershipReader {

    private final MembershipProvider membershipProvider;

    private final GroupPrincipalFactory groupPrincipalFactory;

    PrincipalMembershipReaderImpl(@NotNull MembershipProvider membershipProvider,
                              @NotNull GroupPrincipalFactory groupPrincipalFactory) {
        this.membershipProvider = membershipProvider;
        this.groupPrincipalFactory = groupPrincipalFactory;
    }
    
    @NotNull GroupPrincipalFactory getGroupPrincipalFactory() {
        return groupPrincipalFactory;
    }

    @Override
    public Set<Principal> readMembership(@NotNull Tree authorizableTree) {
        Set<Principal> groupPrincipals = new HashSet<>();
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
        return groupPrincipals;
    }

}
