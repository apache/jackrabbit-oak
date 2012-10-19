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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link UserManager}.
 */
public class PrincipalProviderImpl implements PrincipalProvider {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(PrincipalProviderImpl.class);

    private final UserManager userManager;

    public PrincipalProviderImpl(Root root,
                                 UserConfiguration userConfiguration,
                                 NamePathMapper namePathMapper) {
        this.userManager = userConfiguration.getUserManager(root, namePathMapper);
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(final String principalName) {
        Authorizable authorizable = getAuthorizable(new Principal() {
            @Override
            public String getName() {
                return principalName;
            }
        });
        if (authorizable != null) {
            try {
                return authorizable.getPrincipal();
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }
        }

        // no such principal or error while accessing principal from user/group
        return (EveryonePrincipal.NAME.equals(principalName)) ? EveryonePrincipal.getInstance() : null;
    }

    @Override
    public Set<Group> getGroupMembership(Principal principal) {
        Authorizable authorizable = getAuthorizable(principal);
        if (authorizable == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(authorizable);
        }
    }

    @Override
    public Set<? extends Principal> getPrincipals(String userID) {
        Set<Principal> principals = new HashSet<Principal>();
        try {
            Authorizable authorizable = userManager.getAuthorizable(userID);
            if (authorizable != null && !authorizable.isGroup()) {
                principals.add(authorizable.getPrincipal());
                principals.addAll(getGroupMembership(authorizable));
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
        }
        return principals;
    }

    @Override
    public Iterator<? extends Principal> findPrincipals(String nameHint, int searchType) {
        try {
        Iterator<Authorizable> authorizables = userManager.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, nameHint, UserManager.SEARCH_TYPE_AUTHORIZABLE);
        return Iterators.transform(
                Iterators.filter(authorizables, Predicates.<Object>notNull()),
                new AuthorizableToPrincipal());
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return Iterators.emptyIterator();
        }
    }

    //------------------------------------------------------------< private >---
    private Authorizable getAuthorizable(Principal principal) {
        try {
            return userManager.getAuthorizable(principal);
        } catch (RepositoryException e) {
            log.debug("Error while retrieving principal: ", e.getMessage());
            return null;
        }
    }

    private Set<Group> getGroupMembership(Authorizable authorizable) {
        Set<java.security.acl.Group> groupPrincipals = new HashSet<Group>();
        groupPrincipals.add(EveryonePrincipal.getInstance());
        try {
            Iterator<org.apache.jackrabbit.api.security.user.Group> groups = authorizable.memberOf();
            while (groups.hasNext()) {
                Principal grPrincipal = groups.next().getPrincipal();
                if (grPrincipal instanceof Group) {
                    groupPrincipals.add((Group) grPrincipal);
                }
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
        }
        return groupPrincipals;
    }

    //--------------------------------------------------------------------------

    /**
     * Function to covert an authorizable tree to a principal.
     */
    private final class AuthorizableToPrincipal implements Function<Authorizable, Principal> {
        @Override
        public Principal apply(Authorizable authorizable) {
            try {
                return authorizable.getPrincipal();
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
                return null;
            }
        }
    }
}