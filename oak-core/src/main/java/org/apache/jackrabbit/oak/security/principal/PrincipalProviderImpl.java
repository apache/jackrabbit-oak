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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.QueryBuilder.Direction;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link UserManager}.
 */
class PrincipalProviderImpl implements PrincipalProvider {

    private static final Logger log = LoggerFactory.getLogger(PrincipalProviderImpl.class);

    private final UserManager userManager;
    private final NamePathMapper namePathMapper;

    PrincipalProviderImpl(@NotNull Root root,
                          @NotNull UserConfiguration userConfiguration,
                          @NotNull NamePathMapper namePathMapper) {
        this.userManager = userConfiguration.getUserManager(root, namePathMapper);
        this.namePathMapper = namePathMapper;
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Nullable
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
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

    @Nullable
    @Override
    public ItemBasedPrincipal getItemBasedPrincipal(@NotNull String principalOakPath) {
        try {
            Authorizable authorizable = userManager.getAuthorizableByPath(namePathMapper.getJcrPath(principalOakPath));
            if (authorizable != null) {
                Principal principal = authorizable.getPrincipal();
                if (principal instanceof ItemBasedPrincipal) {
                    return (ItemBasedPrincipal) principal;
                }
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
        }
        return null;
    }

    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        Authorizable authorizable = getAuthorizable(principal);
        if (authorizable == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(authorizable);
        }
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        Set<Principal> principals = new HashSet<>();
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

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable final String nameHint, final int searchType) {
        return findPrincipals(nameHint, false, searchType, 0, -1);
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable final String nameHint, final boolean fullText, final int searchType, long offset,
            long limit) {
        if (offset < 0) {
            offset = 0;
        }
        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }
        try {
            Iterator<Authorizable> authorizables = findAuthorizables(nameHint, searchType, offset, limit);
            Iterator<Principal> principals = Iterators.filter(Iterators.transform(authorizables, new AuthorizableToPrincipal()), Objects::nonNull);

            // everyone is injected only in complete set, not on pages
            boolean noRange = offset == 0 && limit == Long.MAX_VALUE;
            if (noRange && matchesEveryone(nameHint, searchType)) {
                principals = Iterators.concat(principals, Iterators.singletonIterator(EveryonePrincipal.getInstance()));
                return Iterators.filter(principals, new EveryonePredicate());
            } else {
                return principals;
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return Collections.emptyIterator();
        }
    }

    @NotNull
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

    private Set<Principal> getGroupMembership(Authorizable authorizable) {
        Set<Principal> groupPrincipals = new HashSet<>();
        try {
            Iterator<org.apache.jackrabbit.api.security.user.Group> groups = authorizable.memberOf();
            while (groups.hasNext()) {
                Principal grPrincipal = groups.next().getPrincipal();
                if (GroupPrincipals.isGroup(grPrincipal)) {
                    groupPrincipals.add(grPrincipal);
                }
            }
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
        }
        groupPrincipals.add(EveryonePrincipal.getInstance());
        return groupPrincipals;
    }

    private Iterator<Authorizable> findAuthorizables(@Nullable final String nameHint,
                                                     final int searchType, final long offset,
                                                     final long limit) throws RepositoryException {
        Query userQuery = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.like('@' +UserConstants.REP_PRINCIPAL_NAME, buildSearchPattern(nameHint)));
                builder.setSelector(AuthorizableType.getType(searchType).getAuthorizableClass());
                builder.setSortOrder(UserConstants.REP_PRINCIPAL_NAME, Direction.ASCENDING);
                builder.setLimit(offset, limit);
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
        public boolean apply(Principal principal) {
            // principal must never be null as the result was already filtered for null.
            if (EveryonePrincipal.NAME.equals(principal.getName())) {
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
