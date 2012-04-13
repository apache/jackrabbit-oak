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
import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * GroupImpl...
 */
public class GroupImpl extends AuthorizableImpl implements Group {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(GroupImpl.class);

    public GroupImpl(NodeImpl node, UserManagerImpl userManager) {
        super(node, userManager);
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return true;
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        return new GroupPrincipal(getPrincipalName(), getNode().getPath());
    }

    //--------------------------------------------------------------< Group >---

    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        // TODO
        return null;
    }

    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        // TODO
        return null;
    }

    @Override
    public boolean isDeclaredMember(Authorizable authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    @Override
    public boolean isMember(Authorizable authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    @Override
    public boolean addMember(Authorizable authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    @Override
    public boolean removeMember(Authorizable authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    //--------------------------------------------------------------------------
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