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
package org.apache.jackrabbit.oak.jcr.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.jcr.security.principal.EveryonePrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * GroupImpl...
 */
class GroupImpl extends AuthorizableImpl implements Group {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(GroupImpl.class);

    GroupImpl(Node node, UserManagerImpl userManager) throws RepositoryException {
        super(node, userManager);
    }

    @Override
    void checkValidNode(Node node) throws RepositoryException {
        if (node == null || !node.isNodeType(AuthorizableImpl.NT_REP_GROUP)) {
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
        return new GroupPrincipal(getPrincipalName(), getNode().getPath());
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

        if (authorizableImpl.isGroup()) {
            if (getID().equals(authorizableImpl.getID())) {
                String msg = "Attempt to add a group as member of itself (" + getID() + ").";
                log.warn(msg);
                return false;
            }
            if (((Group) authorizableImpl).isMember(this)) {
                log.warn("Attempt to create circular group membership.");
                return false;
            }
        }

        return getUserManager().getMembershipManager().addMember(this, authorizableImpl);
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
        }

        return getUserManager().getMembershipManager().removeMember(this, (AuthorizableImpl) authorizable);
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
        if (isEveryone()) {
            return getUserManager().findAuthorizables(AuthorizableImpl.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_AUTHORIZABLE);
        } else if (includeInherited) {
            return getUserManager().getMembershipManager().getMembers(this);
        } else {
            return getUserManager().getMembershipManager().getDeclaredMembers(this);
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
        } else if (getNode().isSame(((AuthorizableImpl) authorizable).getNode())) {
            return false;
        } else if (isEveryone()) {
            return true;
        } else if (includeInherited) {
            return getUserManager().getMembershipManager().hasMember(this, (AuthorizableImpl) authorizable);
        } else {
            return getUserManager().getMembershipManager().hasDeclaredMember(this, (AuthorizableImpl) authorizable);
        }
    }

    /**
     * Principal representation of this group instance.
     */
    private class GroupPrincipal extends ItemBasedPrincipalImpl implements java.security.acl.Group {

        GroupPrincipal(String principalName, String nodePath) {
            super(principalName, getNode());
        }

        @Override
        public boolean addMember(Principal principal) {
            return false;
        }

        @Override
        public boolean removeMember(Principal principal) {
            return false;
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
            final Iterator<Authorizable> iterator;
            try {
                iterator = GroupImpl.this.getMembers();
            } catch (RepositoryException e) {
                // should not occur.
                String msg = "Unable to retrieve Group members: " + e.getMessage();
                log.error(msg);
                throw new IllegalStateException(msg);
            }

            Enumeration<Principal> members = new Enumeration<Principal>() {

                @Override
                public boolean hasMoreElements() {
                    return iterator.hasNext();
                }

                @Override
                public Principal nextElement() {
                    try {
                        return iterator.next().getPrincipal();
                    } catch (RepositoryException e) {
                        String msg = "Internal error while retrieving principal: " + e.getMessage();
                        log.error(msg);
                        throw new IllegalStateException(msg);
                    }
                }
            };
            return members;
        }
    }
}