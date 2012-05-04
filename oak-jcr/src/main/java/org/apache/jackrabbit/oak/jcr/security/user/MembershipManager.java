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
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.util.Iterator;

/**
 * MembershipManager...
 */
class MembershipManager {

    private static final Logger log = LoggerFactory.getLogger(MembershipManager.class);

    private final UserManagerImpl userManager;
    private final SessionDelegate sessionDelegate;

    MembershipManager(UserManagerImpl userManager, SessionDelegate sessionDelegate) {
        this.userManager = userManager;
        this.sessionDelegate = sessionDelegate;
    }

    Iterator<Group> getDeclaredMembership(AuthorizableImpl authorizable) throws RepositoryException {
        // TODO
        return null;
    }

    Iterator<Group> getMembership(AuthorizableImpl authorizable) throws RepositoryException {
        // TODO
        return null;
    }

    boolean hasDeclaredMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    boolean hasMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        // TODO
        return false;
    }

    Iterator<Authorizable> getDeclaredMembers(GroupImpl group) throws RepositoryException {
        // TODO
        return null;
    }

    Iterator<Authorizable> getMembers(GroupImpl group) throws RepositoryException {
        // TODO
        return null;
    }

    boolean addMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
       // todo
        return false;
    }

    boolean removeMember(GroupImpl group, AuthorizableImpl authorizable) throws RepositoryException {
        // todo
        return false;
    }
}