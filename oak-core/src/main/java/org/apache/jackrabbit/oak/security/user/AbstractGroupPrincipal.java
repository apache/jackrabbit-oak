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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * Base class for {@code Group} principals.
 */
abstract class AbstractGroupPrincipal extends TreeBasedPrincipal implements GroupPrincipal {

    private static final Logger log = LoggerFactory.getLogger(AbstractGroupPrincipal.class);

    AbstractGroupPrincipal(@NotNull String principalName, @NotNull Tree groupTree, @NotNull NamePathMapper namePathMapper) {
        super(principalName, groupTree, namePathMapper);
    }

    AbstractGroupPrincipal(@NotNull String principalName, @NotNull String groupPath, @NotNull NamePathMapper namePathMapper) {
        super(principalName, groupPath, namePathMapper);
    }

    abstract UserManager getUserManager();

    abstract boolean isEveryone() throws RepositoryException;

    abstract boolean isMember(@NotNull Authorizable authorizable) throws RepositoryException;

    @NotNull
    abstract Iterator<Authorizable> getMembers() throws RepositoryException;

    //--------------------------------------------------------------< Group >---
    @Override
    public boolean isMember(@NotNull Principal principal) {
        boolean isMember = false;
        try {
            // shortcut for everyone group -> avoid collecting all members
            // as all users and groups are member of everyone.
            if (isEveryone()) {
                isMember = !EveryonePrincipal.NAME.equals(principal.getName());
            } else {
                Authorizable a = getUserManager().getAuthorizable(principal);
                if (a != null) {
                    isMember = isMember(a);
                }
            }
        } catch (RepositoryException e) {
            log.warn("Failed to determine group membership: {}", e.getMessage());
        }

        // principal doesn't represent a known authorizable or an error occurred.
        return isMember;
    }

    @NotNull
    @Override
    public Enumeration<? extends Principal> members() {
        final Iterator<Authorizable> members;
        try {
            members = getMembers();
        } catch (RepositoryException e) {
            // should not occur.
            String msg = "Unable to retrieve Group members: " + e.getMessage();
            log.error(msg);
            throw new IllegalStateException(msg, e);
        }

        Iterator<Principal> principals = Iterators.transform(members, authorizable -> {
            if (authorizable == null) {
                return null;
            }
            try {
                return authorizable.getPrincipal();
            } catch (RepositoryException e) {
                String msg = "Internal error while retrieving principal: " + e.getMessage();
                log.error(msg);
                throw new IllegalStateException(msg, e);
            }
        });
        return Iterators.asEnumeration(Iterators.filter(principals, Predicates.<Object>notNull()));
    }
}
