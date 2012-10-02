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
import java.util.Enumeration;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.TreeBasedPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GroupImpl...
 */
class GroupImpl extends AuthorizableImpl implements Group {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(GroupImpl.class);

    GroupImpl(String id, Tree tree, UserManagerImpl userManager) throws RepositoryException {
        super(id, tree, userManager);
    }

    @Override
    void checkValidTree(Tree tree) throws RepositoryException {
        if (tree == null || !getUserManager().getUserProvider().isAuthorizableType(tree, Type.GROUP)) {
            throw new IllegalArgumentException("Invalid group node: node type rep:Group expected.");
        }
    }

    //-------------------------------------------------------< Authorizable >---
    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#isGroup()
     */
    @Override
    public boolean isGroup() {
        return true;
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Authorizable#getPrincipal()
     */
    @Override
    public Principal getPrincipal() throws RepositoryException {
        return new GroupPrincipal(getPrincipalName(), getTree());
    }

    //--------------------------------------------------------------< Group >---
    /**
     * @see org.apache.jackrabbit.api.security.user.Group#getDeclaredMembers()
     */
    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return getMembers(false);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Group#getMembers()
     */
    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return getMembers(true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Group#isDeclaredMember(org.apache.jackrabbit.api.security.user.Authorizable)
     */
    @Override
    public boolean isDeclaredMember(Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, false);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Group#isMember(org.apache.jackrabbit.api.security.user.Authorizable)
     */
    @Override
    public boolean isMember(Authorizable authorizable) throws RepositoryException {
        return isMember(authorizable, true);
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Group#addMember(org.apache.jackrabbit.api.security.user.Authorizable)
     */
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
            if (((Group) authorizableImpl).isMember(this)) {
                log.debug("Attempt to create circular group membership.");
                return false;
            }
        }

        if (isDeclaredMember(authorizable)) {
            log.debug("Authorizable {} is already declared member of {}", memberID, getID());
            return false;
        }

        return getUserManager().getMembershipProvider().addMember(getTree(), authorizableImpl.getTree());
    }

    /**
     * @see org.apache.jackrabbit.api.security.user.Group#removeMember(org.apache.jackrabbit.api.security.user.Authorizable)
     */
    @Override
    public boolean removeMember(Authorizable authorizable) throws RepositoryException {
        if (!isValidAuthorizableImpl(authorizable)) {
            log.warn("Invalid Authorizable: {}", authorizable);
            return false;
        }
        if (isEveryone()) {
            return false;
        } else {
            MembershipProvider mMgr = getUserManager().getMembershipProvider();
            return mMgr.removeMember(getTree(), ((AuthorizableImpl) authorizable).getTree());
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
        UserManagerImpl uMgr = getUserManager();
        if (isEveryone()) {
            // TODO: improve using authorizable-query
            String propName = getJcrName(REP_PRINCIPAL_NAME);
            return uMgr.findAuthorizables(propName, null, UserManager.SEARCH_TYPE_AUTHORIZABLE);
        } else {
            MembershipProvider mMgr = uMgr.getMembershipProvider();
            Iterator oakPaths = mMgr.getMembers(getTree(), Type.AUTHORIZABLE, includeInherited);
            if (!oakPaths.hasNext()) {
                AuthorizableIterator iterator = AuthorizableIterator.create(oakPaths, uMgr, UserManager.SEARCH_TYPE_AUTHORIZABLE);
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

        if (isEveryone()) {
            return true;
        } else if (getID().equals(authorizable.getID())) {
            return false;
        } else {
            Tree authorizableTree = ((AuthorizableImpl) authorizable).getTree();
            MembershipProvider mgr = getUserManager().getMembershipProvider();
            return mgr.isMember(this.getTree(), authorizableTree, includeInherited);
        }
    }

    /**
     * Principal representation of this group instance.
     */
    private class GroupPrincipal extends TreeBasedPrincipal implements java.security.acl.Group {

        GroupPrincipal(String principalName, Tree groupTree) {
            super(principalName, groupTree, getUserManager().getNamePathMapper());
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
            boolean isMember = false;
            try {
                // shortcut for everyone group -> avoid collecting all members
                // as all users and groups are member of everyone.
                if (isEveryone()) {
                    isMember = !EveryonePrincipal.NAME.equals(principal.getName());
                } else {
                    Authorizable a = getUserManager().getAuthorizable(principal);
                    if (a != null) {
                        isMember = GroupImpl.this.isMember(a);
                    }
                }
            } catch (RepositoryException e) {
                log.warn("Failed to determine group membership", e.getMessage());
            }

            // principal doesn't represent a known authorizable or an error occurred.
            return isMember;
        }

        @Override
        public Enumeration<? extends Principal> members() {
            final Iterator<Authorizable> members;
            try {
                members = GroupImpl.this.getMembers();
            } catch (RepositoryException e) {
                // should not occur.
                String msg = "Unable to retrieve Group members: " + e.getMessage();
                log.error(msg);
                throw new IllegalStateException(msg);
            }

            Iterator<Principal> principals = Iterators.transform(members, new Function<Authorizable, Principal>() {
                @Override
                public Principal apply(@Nullable Authorizable authorizable) {
                    assert authorizable != null;
                    try {
                        return authorizable.getPrincipal();
                    } catch (RepositoryException e) {
                        String msg = "Internal error while retrieving principal: " + e.getMessage();
                        log.error(msg);
                        throw new IllegalStateException(msg);
                    }
                }
            });
            return Iterators.asEnumeration(principals);
        }
    }
}