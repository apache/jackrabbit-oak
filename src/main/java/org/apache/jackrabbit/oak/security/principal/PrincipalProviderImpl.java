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
package org.apache.jackrabbit.oak.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.PathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link UserProvider} and {@link MembershipProvider}.
 */
public class PrincipalProviderImpl implements PrincipalProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrincipalProviderImpl.class);

    private final UserProvider userProvider;
    private final MembershipProvider membershipProvider;
    private final PathMapper pathMapper;

    public PrincipalProviderImpl(UserProvider userProvider,
                                 MembershipProvider membershipProvider,
                                 PathMapper pathMapper) {
        this.userProvider = userProvider;
        this.membershipProvider = membershipProvider;
        this.pathMapper = pathMapper;
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(final String principalName) {
        Tree tree = userProvider.getAuthorizableByPrincipal(new Principal() {
            @Override
            public String getName() {
                return principalName;
            }
        });

        if (tree != null) {
            return (isGroup(tree)) ? new TreeBasedGroup(tree) : new TreeBasedPrincipal(tree, pathMapper);
        } else {
            return null;
        }
    }

    @Override
    public Set<Group> getGroupMembership(Principal principal) {
        Tree authTree = userProvider.getAuthorizableByPrincipal(principal);
        if (authTree == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(authTree);
        }
    }

    @Override
    public Set<? extends Principal> getPrincipals(String userID) {
        Set<Principal> principals;
        Tree userTree = userProvider.getAuthorizable(userID, Type.USER);
        if (userTree != null) {
            principals = new HashSet<Principal>();
            Principal userPrincipal = new TreeBasedPrincipal(userTree, pathMapper);
            principals.add(userPrincipal);
            principals.addAll(getGroupMembership(userPrincipal));
            if (userProvider.isAdminUser(userTree)) {
                principals.add(AdminPrincipal.INSTANCE);
            }
        } else {
            principals = Collections.emptySet();
        }
        return principals;
    }

    @Override
    public Iterator<? extends Principal> findPrincipals(String nameHint, int searchType) {
        String[] propNames = new String[] {UserConstants.REP_PRINCIPAL_NAME};
        String[] ntNames = new String[] {UserConstants.NT_REP_AUTHORIZABLE};
        Iterator<Tree> authorizables = userProvider.findAuthorizables(propNames, nameHint, ntNames, false, Long.MAX_VALUE, Type.AUTHORIZABLE);

        return Iterators.transform(authorizables, new AuthorizableToPrincipal());
    }

    //------------------------------------------------------------< private >---

    private Set<Group> getGroupMembership(Tree authorizableTree) {
        Iterator<String> groupPaths = membershipProvider.getMembership(authorizableTree, true);
        Set<Group> groups = new HashSet<Group>();
        groups.add(EveryonePrincipal.getInstance());

        while (groupPaths.hasNext()) {
            String path = groupPaths.next();
            Tree groupTree = userProvider.getAuthorizableByPath(path);
            if (groupTree != null) {
                groups.add(new TreeBasedGroup(groupTree));
            }
        }
        return groups;
    }

    private boolean isGroup(Tree authorizableTree) {
        assert authorizableTree != null;
        assert authorizableTree.hasProperty(JcrConstants.JCR_PRIMARYTYPE);

        String ntName = authorizableTree.getProperty(JcrConstants.JCR_PRIMARYTYPE).getValue().getString();
        return UserConstants.NT_REP_GROUP.equals(ntName);
    }

    /**
     * Function to covert an authorizable tree to a principal.
     */
    private final class AuthorizableToPrincipal implements Function<Tree, TreeBasedPrincipal> {

        @Override
        public TreeBasedPrincipal apply(@Nullable Tree tree) {
            if (tree == null) {
                throw new IllegalArgumentException("null tree.");
            }
            if (userProvider.isAuthorizableType(tree, Type.GROUP)) {
                return new TreeBasedGroup(tree);
            } else {
                return new TreeBasedPrincipal(tree, pathMapper);
            }
        }
    }

    /**
     * Tree-based principal implementation that marks the principal as group.
     */
    private final class TreeBasedGroup extends TreeBasedPrincipal implements Group {

        public TreeBasedGroup(Tree tree) {
            super(tree, pathMapper);
        }

        @Override
        public boolean addMember(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeMember(Principal principal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMember(Principal principal) {
            return membershipProvider.isMember(getTree(), userProvider.getAuthorizableByPrincipal(principal), true);
        }

        @Override
        public Enumeration<? extends Principal> members() {
            Iterator<String> declaredMemberPaths = membershipProvider.getMembers(getTree(), Type.AUTHORIZABLE, false);
            Iterator<? extends Principal> members = Iterators.transform(declaredMemberPaths, new Function<String, Principal>() {
                @Override
                public Principal apply(@Nullable String oakPath) {
                    Tree tree = userProvider.getAuthorizableByPath(oakPath);
                    if (tree != null) {
                        if (isGroup(tree)) {
                            return new TreeBasedGroup(tree);
                        } else {
                            return new TreeBasedPrincipal(tree, pathMapper);
                        }
                    }
                    return null;
                }
            });
            return Iterators.asEnumeration(Iterators.filter(members, Predicates.notNull()));
        }
    }
}