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
     *
     * @param includeInherited
     * @return
     * @throws RepositoryException
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
     *
     * @param authorizable
     * @param includeInherited
     * @return
     * @throws RepositoryException
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
     *
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
            // TODO
            return false;
        }

        @Override
        public Enumeration<? extends Principal> members() {
            // TODO
            return null;
        }
    }
}