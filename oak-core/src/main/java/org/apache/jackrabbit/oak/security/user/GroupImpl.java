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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroupImpl...
 */
class GroupImpl extends AuthorizableImpl implements Group {

    private static final Logger log = LoggerFactory.getLogger(GroupImpl.class);

    GroupImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);
    }

    //---------------------------------------------------< AuthorizableImpl >---
    @Override
    void checkValidTree(Tree tree) throws RepositoryException {
        if (tree == null || !UserUtil.isType(tree, AuthorizableType.GROUP)) {
            throw new IllegalArgumentException("Invalid group node: node type rep:Group expected.");
        }
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return true;
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        return new GroupPrincipal(getPrincipalName(), getTree());
    }

    //--------------------------------------------------------------< Group >---
    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return getMembers(false);
    }

    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return getMembers(true);
    }

    @Override
    public boolean isDeclaredMember(Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, false);
    }

    @Override
    public boolean isMember(Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, true);
    }

    @Override
    public boolean addMember(Authorizable authorizable) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            log.warn("Invalid Authorizable: {}", authorizable);
            return false;
        }

        AuthorizableImpl authorizableImpl = ((AuthorizableImpl) authorizable);
        if (isEveryone() || authorizableImpl.isEveryone()) {
            return false;
        }

        String memberID = authorizable.getID();
        if (authorizableImpl.isGroup()) {
            if (getID().equals(memberID)) {
                String msg = "Attempt to add a group as member of itself (" + getID() + ").";
                log.debug(msg);
                return false;
            }
            if (isCyclicMembership(authorizableImpl)) {
                log.warn("Attempt to create circular group membership.");
                return false;
            }
        }

        if (isDeclaredMember(authorizable)) {
            log.debug("Authorizable {} is already declared member of {}", memberID, getID());
            return false;
        }

        return getMembershipProvider().addMember(getTree(), authorizableImpl.getTree());
    }

    /**
     * Returns {@code true} if the given {@code newMember} is a Group
     * and contains {@code this} Group as declared or inherited member.
     *
     * @param newMember The new member to be tested for cyclic membership.
     * @return true if the 'newMember' is a group and 'this' is an declared or
     * inherited member of it.
     */
    private boolean isCyclicMembership(AuthorizableImpl newMember) {
        if (newMember.isGroup()) {
            MembershipProvider mProvider = getMembershipProvider();
            String contentId = mProvider.getContentID(getTree());
            if (mProvider.isMember(newMember.getTree(), contentId, true)) {
                // found cyclic group membership
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean removeMember(Authorizable authorizable) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            log.warn("Invalid Authorizable: {}", authorizable);
            return false;
        }
        if (isEveryone()) {
            return false;
        } else {
            Tree memberTree = ((AuthorizableImpl) authorizable).getTree();
            return getMembershipProvider().removeMember(getTree(), memberTree);
        }
    }

    //--------------------------------------------------------------------------
    /**
     * Internal implementation of {@link #getDeclaredMembers()} and {@link #getMembers()}.
     *
     * @param includeInherited Flag indicating if only the declared or all members
     * should be returned.
     * @return Iterator of authorizables being member of this group.
     * @throws RepositoryException If an error occurs.
     */
    private Iterator<Authorizable> getMembers(boolean includeInherited) throws RepositoryException {
        UserManagerImpl userMgr = getUserManager();
        if (isEveryone()) {
            String propName = getUserManager().getNamePathMapper().getJcrName((REP_PRINCIPAL_NAME));
            return Iterators.filter(
                    userMgr.findAuthorizables(propName, null, UserManager.SEARCH_TYPE_AUTHORIZABLE),
                    new Predicate<Authorizable>() {
                        @Override
                        public boolean apply(@Nullable Authorizable authorizable) {
                            if (authorizable == null) {
                                return false;
                            }
                            if (authorizable.isGroup()) {
                                try {
                                    return !((GroupImpl) authorizable).isEveryone();
                                } catch (RepositoryException e) {
                                    log.warn("Unable to evaluate if authorizable is the 'everyone' group.", e);
                                }
                            }
                            return true;
                        }
                    }
            );
        } else {
            Iterator<String> oakPaths = getMembershipProvider().getMembers(getTree(), AuthorizableType.AUTHORIZABLE, includeInherited);
            if (oakPaths.hasNext()) {
                AuthorizableIterator iterator = AuthorizableIterator.create(oakPaths, userMgr, AuthorizableType.AUTHORIZABLE);
                return new RangeIteratorAdapter(iterator, iterator.getSize());
            } else {
                return RangeIteratorAdapter.EMPTY;
            }
        }
    }

    /**
     * Internal implementation of {@link #isDeclaredMember(Authorizable)} and {@link #isMember(Authorizable)}.
     *
     * @param authorizable The authorizable to test.
     * @param includeInherited Flag indicating if only declared or all members
     * should taken into account.
     * @return {@code true} if the specified authorizable is member or declared
     * member of this group; {@code false} otherwise.
     * @throws RepositoryException If an error occurs.
     */
    private boolean isMember(Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            return false;
        }

        if (getID().equals(authorizable.getID())) {
            return false;
        } else if (isEveryone()) {
            return true;
        } else {
            Tree authorizableTree = ((AuthorizableImpl) authorizable).getTree();
            MembershipProvider mgr = getUserManager().getMembershipProvider();
            return mgr.isMember(this.getTree(), authorizableTree, includeInherited);
        }
    }

    /**
     * Principal representation of this group instance.
     */
    private final class GroupPrincipal extends AbstractGroupPrincipal {

        private GroupPrincipal(String principalName, Tree groupTree) {
            super(principalName, groupTree, GroupImpl.this.getUserManager().getNamePathMapper());
        }

        @Override
        UserManager getUserManager() {
            return GroupImpl.this.getUserManager();
        }

        @Override
        boolean isEveryone() throws RepositoryException {
            return GroupImpl.this.isEveryone();
        }

        @Override
        boolean isMember(@Nonnull Authorizable authorizable) throws RepositoryException {
            return GroupImpl.this.isMember(authorizable);
        }

        @Nonnull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            return GroupImpl.this.getMembers();
        }
    }
}