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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link UserManager}.
 */
class PrincipalProviderImpl implements PrincipalProvider {

    private static final Logger log = LoggerFactory.getLogger(PrincipalProviderImpl.class);

    private final UserManager userManager;

    PrincipalProviderImpl(@Nonnull Root root,
                          @Nonnull UserConfiguration userConfiguration,
                          @Nonnull NamePathMapper namePathMapper) {
        this.userManager = userConfiguration.getUserManager(root, namePathMapper);
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        Authorizable authorizable = getAuthorizable(new PrincipalImpl(principalName));
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

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        Authorizable authorizable = getAuthorizable(principal);
        if (authorizable == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(authorizable);
        }
    }

    @Nonnull
    @Override
    public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
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

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable final String nameHint,
                                                        final int searchType) {
        try {
            Iterator<Authorizable> authorizables = findAuthorizables(nameHint, searchType);
            Iterator<Principal> principals = Iterators.transform(
                    Iterators.filter(authorizables, Predicates.notNull()),
                    new AuthorizableToPrincipal());

            if (matchesEveryone(nameHint, searchType)) {
                principals = Iterators.concat(principals, Iterators.singletonIterator(EveryonePrincipal.getInstance()));
                return Iterators.filter(principals, new EveryonePredicate());
            } else {
                return principals;
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return Iterators.emptyIterator();
        }
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }

    //------------------------------------------------------------< private >---
    private Authorizable getAuthorizable(Principal principal) {
        try {
            return userManager.getAuthorizable(principal);
        } catch (RepositoryException e) {
            log.debug("Error while retrieving principal: {}", e.getMessage());
            return null;
        }
    }

    private Set<Group> getGroupMembership(Authorizable authorizable) {
        Set<java.security.acl.Group> groupPrincipals = new HashSet<Group>();
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
        groupPrincipals.add(EveryonePrincipal.getInstance());
        return groupPrincipals;
    }

    private Iterator<Authorizable> findAuthorizables(@Nullable final String nameHint,
                                                     final int searchType) throws RepositoryException {
        Query userQuery = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.like('@' +UserConstants.REP_PRINCIPAL_NAME, buildSearchPattern(nameHint)));
                builder.setSelector(AuthorizableType.getType(searchType).getAuthorizableClass());
            }
        };
        return userManager.findAuthorizables(userQuery);
    }

    private static String buildSearchPattern(String nameHint) {
        if (nameHint == null) {
            return "%";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append('%');
            sb.append(nameHint.replace("%", "\\%").replace("_", "\\_"));
            sb.append('%');
            return sb.toString();
        }

    }

    private static boolean matchesEveryone(String nameHint, int searchType) {
        return searchType != PrincipalManager.SEARCH_TYPE_NOT_GROUP &&
                (nameHint == null || EveryonePrincipal.NAME.contains(nameHint));
    }

    //--------------------------------------------------------------------------
    /**
     * Function to covert an authorizable tree to a principal.
     */
    private static final class AuthorizableToPrincipal implements Function<Authorizable, Principal> {
        @Override
        public Principal apply(@Nullable Authorizable authorizable) {
            if (authorizable != null) {
                try {
                    return authorizable.getPrincipal();
                } catch (RepositoryException e) {
                    log.debug(e.getMessage());
                }
            }
            return null;
        }
    }

    /**
     * Predicate to make sure the everyone principal is only included once in
     * the result set.
     */
    private static final class EveryonePredicate implements Predicate<Principal> {
        private boolean servedEveryone = false;
        @Override
        public boolean apply(@Nullable Principal principal) {
            String pName = (principal == null) ? null : principal.getName();
            if (EveryonePrincipal.NAME.equals(pName)) {
                if (servedEveryone) {
                    return false;
                } else {
                    servedEveryone = true;
                    return true;
                }
            } else {
                // not everyone
                return true;
            }
        }
    }
}
