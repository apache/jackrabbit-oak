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

import java.util.Objects;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.principal.EveryoneFilter;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachedMembershipReader;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheLoader;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachePrincipalFactory;
import org.apache.jackrabbit.oak.security.user.query.QueryUtil;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link org.apache.jackrabbit.api.security.user.UserManager}.
 */
class UserPrincipalProvider implements PrincipalProvider {

    static final String REP_GROUP_PRINCIPAL_NAMES = "rep:groupPrincipalNames";
    private static final Logger log = LoggerFactory.getLogger(UserPrincipalProvider.class);

    private final Root root;
    private final UserConfiguration config;
    private final NamePathMapper namePathMapper;

    private final UserProvider userProvider;
    private final CacheLoader principalMembershipReader;

    UserPrincipalProvider(@NotNull Root root,
                          @NotNull UserConfiguration userConfiguration,
                          @NotNull NamePathMapper namePathMapper) {
        this.root = root;
        this.config = userConfiguration;
        this.namePathMapper = namePathMapper;
        this.userProvider = new UserProvider(root, config.getParameters());

        MembershipProvider membershipProvider = new MembershipProvider(root, config.getParameters());
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(config, REP_GROUP_PRINCIPAL_NAMES);
        boolean cacheEnabled = (cacheConfiguration.isCacheEnabled() && root.getContentSession().getAuthInfo().getPrincipals().contains(SystemPrincipal.INSTANCE));
        if (cacheEnabled) {
            CachedMembershipReader cachedPrincipalMembershipReader = new CachedPrincipalMembershipReader(
                    cacheConfiguration,
                    root,
                    createPrincipalFactory());
            principalMembershipReader = (tree) -> cachedPrincipalMembershipReader.readMembership(tree, readMembershipFromProvider(membershipProvider));
        } else {
            principalMembershipReader = readMembershipFromProvider(membershipProvider);
        }
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
        Tree authorizableTree = userProvider.getAuthorizableByPrincipal(new PrincipalImpl(principalName));
        Principal principal = createPrincipal(authorizableTree);

        if (principal == null) {
            // no such principal or error while accessing principal from user/group
            return (EveryonePrincipal.NAME.equals(principalName)) ? EveryonePrincipal.getInstance() : null;
        } else {
            return principal;
        }
    }

    @Nullable
    @Override
    public ItemBasedPrincipal getItemBasedPrincipal(@NotNull String principalOakPath) {
        Tree authorizableTree = userProvider.getAuthorizableByPath(principalOakPath);
        Principal principal = createPrincipal(authorizableTree);

        if (principal instanceof ItemBasedPrincipal) {
            return (ItemBasedPrincipal) principal;
        } else {
            return null;
        }
    }


    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        Tree tree = getAuthorizableTree(principal);
        if (tree == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(tree);
        }
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        Set<Principal> principals = new HashSet<>();
        Tree tree = userProvider.getAuthorizable(userID);
        if (UserUtil.isType(tree, AuthorizableType.USER)) {
            Principal userPrincipal = createUserPrincipal(userID, tree);
            if (userPrincipal != null) {
                principals.add(userPrincipal);
                principals.addAll(getGroupMembership(tree));
            }
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
    public Iterator<? extends Principal> findPrincipals(@Nullable final String nameHint, final boolean fullText, final int searchType, long offset, long limit) {
        if (offset < 0) {
            offset = 0;
        }
        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }
        try {

            String lookupClause = "";
            if (nameHint != null && !nameHint.isEmpty()) {
                if (fullText) {
                    lookupClause = String.format("[jcr:contains(.,'%s')]", buildSearchPatternFT(nameHint));
                } else {
                    lookupClause = String.format("[jcr:like(@rep:principalName,'%s')]", buildSearchPatternContains(nameHint));
                }
            }
            AuthorizableType type = AuthorizableType.getType(searchType);
            StringBuilder statement = new StringBuilder()
                    .append(QueryUtil.getSearchRoot(type, config.getParameters()))
                    .append("//element(*,").append(QueryUtil.getNodeTypeName(type)).append(')')
                    .append(lookupClause)
                    .append(" order by @rep:principalName");
            Result result = root.getQueryEngine().executeQuery(
                    statement.toString(), javax.jcr.query.Query.XPATH,
                    limit, offset, NO_BINDINGS, namePathMapper.getSessionLocalMappings());

            Iterator<Principal> principals = Iterators.filter(
                    Iterators.transform(result.getRows().iterator(), new ResultRowToPrincipal()::apply),
                    Objects::nonNull);

            // everyone is injected only in complete set, not on pages
            return EveryoneFilter.filter(principals, nameHint, searchType, offset, limit);
        } catch (ParseException e) {
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

    private @NotNull CachePrincipalFactory createPrincipalFactory() {
        return (principalName) -> new CachedGroupPrincipal(principalName, namePathMapper, root, config);
    }

    @Nullable
    private Tree getAuthorizableTree(@NotNull Principal principal) {
        return userProvider.getAuthorizableByPrincipal(principal);
    }

    @Nullable
    private Principal createPrincipal(@Nullable Tree authorizableTree) {
        if (authorizableTree != null) {
            AuthorizableType type = UserUtil.getType(authorizableTree);
            if (AuthorizableType.GROUP == type) {
                return createGroupPrincipal(authorizableTree);
            } else if (AuthorizableType.USER == type) {
                return createUserPrincipal(UserUtil.getAuthorizableId(authorizableTree, AuthorizableType.USER), authorizableTree);
            }
        }
        return null;
    }

    @Nullable
    private Principal createUserPrincipal(@NotNull String id, @NotNull Tree userTree) {
        String principalName = getPrincipalName(userTree);
        if (principalName == null) {
            return null;
        }
        if (UserUtil.isSystemUser(userTree)) {
            return new SystemUserPrincipalImpl(principalName, userTree, namePathMapper);
        } else if (UserUtil.isAdmin(config.getParameters(), id)) {
            return new AdminPrincipalImpl(principalName, userTree, namePathMapper);
        } else {
            return new TreeBasedPrincipal(principalName, userTree, namePathMapper);
        }
    }

    @Nullable
    private Principal createGroupPrincipal(@NotNull Tree groupTree) {
        String principalName = getPrincipalName(groupTree);
        if (principalName == null) {
            return null;
        }
        return new GroupPrincipalImpl(principalName, groupTree.getPath(), namePathMapper, root, config);
    }

    @Nullable
    private static String getPrincipalName(@NotNull Tree tree) {
        PropertyState principalName = tree.getProperty(UserConstants.REP_PRINCIPAL_NAME);
        if (principalName != null) {
            return principalName.getValue(STRING);
        } else {
            String msg = "Authorizable without principal name " + UserUtil.getAuthorizableId(tree);
            log.warn(msg);
            return null;
        }
    }

    @NotNull
    private Set<Principal> getGroupMembership(@NotNull Tree authorizableTree) {
        Set<Principal> groupPrincipals = new HashSet<>();
        groupPrincipals.addAll(principalMembershipReader.load(authorizableTree));

        // add the dynamic everyone principal group which is not included in
        // the 'getMembership' call.
        groupPrincipals.add(EveryonePrincipal.getInstance());
        return groupPrincipals;
    }


    private static String buildSearchPatternContains(@NotNull String nameHint) {
        StringBuilder sb = new StringBuilder();
        sb.append('%');
        sb.append(nameHint.replace("%", "\\%").replace("_", "\\_"));
        sb.append('%');
        return sb.toString();
    }

    private static String buildSearchPatternFT(@NotNull String nameHint) {
        if (nameHint.contains("*")) {
            return QueryUtil.escapeForQuery(nameHint);
        } else {
            return QueryUtil.escapeForQuery(nameHint) + "*";
        }
    }

    private CacheLoader readMembershipFromProvider(final MembershipProvider membershipProvider) {
        return authorizableTree -> {
            Set<Principal> groupPrincipals = new HashSet<>();
            Iterator<Tree> groupTrees = membershipProvider.getMembership(authorizableTree, true);
            while (groupTrees.hasNext()) {
                Tree groupTree = groupTrees.next();
                if (UserUtil.isType(groupTree, AuthorizableType.GROUP)) {
                    Principal gr = createGroupPrincipal(groupTree);
                    if (gr != null) {
                        groupPrincipals.add(gr);
                    }
                }
            }
            return groupPrincipals;
        };
    }

    //--------------------------------------------------------------------------
    /**
     * Function to covert an authorizable tree (as obtained from the query result) to a principal.
     */
    private final class ResultRowToPrincipal implements Function<ResultRow, Principal> {
        @Override
        public Principal apply(ResultRow resultRow) {
            return createPrincipal(resultRow.getTree(null));
        }
    }

    //--------------------------------------------------------------------------
    // Group Principal implementations that retrieve member information on demand
    //--------------------------------------------------------------------------

    private abstract static class BaseGroupPrincipal extends AbstractGroupPrincipal {

        private final Root root;
        private final UserConfiguration config;
        private UserManager userManager;

        BaseGroupPrincipal(@NotNull String principalName, @NotNull String groupPath,
                @NotNull NamePathMapper namePathMapper, @NotNull Root root, @NotNull UserConfiguration config) {
            super(principalName, groupPath, namePathMapper);
            this.root = root;
            this.config = config;
        }

        @NotNull
        @Override
        UserManager getUserManager() {
            if (userManager == null) {
                userManager = config.getUserManager(root, getNamePathMapper());
            }
            return userManager;
        }

        @Override
        boolean isEveryone() {
            return EveryonePrincipal.NAME.equals(getName());
        }

        @Override
        boolean isMember(@NotNull Authorizable authorizable) throws RepositoryException {
            org.apache.jackrabbit.api.security.user.Group g = getGroup();
            return g != null && g.isMember(authorizable);
        }

        @NotNull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            org.apache.jackrabbit.api.security.user.Group g = getGroup();
            return (g == null) ? Collections.emptyIterator() : g.getMembers();
        }

        @Nullable
        abstract org.apache.jackrabbit.api.security.user.Group getGroup()throws RepositoryException;
    }

    /**
     * Implementation of {@link AbstractGroupPrincipal} that reads the underlying
     * authorizable group lazily in case the group membership must be retrieved.
     */
    private static final class GroupPrincipalImpl extends BaseGroupPrincipal {

        private org.apache.jackrabbit.api.security.user.Group group;

        GroupPrincipalImpl(@NotNull String principalName, @NotNull String groupPath,
                @NotNull NamePathMapper namePathMapper, @NotNull Root root, @NotNull UserConfiguration config) {
            super(principalName, groupPath, namePathMapper, root, config);
        }

        @Override
        @Nullable
        org.apache.jackrabbit.api.security.user.Group getGroup() throws RepositoryException {
            if (group == null) {
                Authorizable authorizable = getUserManager().getAuthorizable(this);
                if (authorizable != null && authorizable.isGroup()) {
                    group = (org.apache.jackrabbit.api.security.user.Group) authorizable;
                }
            }
            return group;
        }
    }

    private static final class CachedGroupPrincipal extends BaseGroupPrincipal {

        private org.apache.jackrabbit.api.security.user.Group group;

        CachedGroupPrincipal(@NotNull String principalName, @NotNull NamePathMapper namePathMapper,
                @NotNull Root root, @NotNull UserConfiguration config) {
            super(principalName, "", namePathMapper, root, config);
        }

        @NotNull
        @Override
        String getOakPath() throws RepositoryException {
            String oakPath = getNamePathMapper().getOakPath(getPath());
            if (oakPath == null) {
                throw new RepositoryException("Failed to retrieve path of group principal " + getName());
            }
            return oakPath;
        }

        @NotNull
        @Override
        public String getPath() throws RepositoryException {
            org.apache.jackrabbit.api.security.user.Group gr = getGroup();
            if (gr == null) {
                throw new RepositoryException("Failed to retrieve path of group principal " + getName());
            }
            return gr.getPath();
        }

        @Override
        @Nullable
        org.apache.jackrabbit.api.security.user.Group getGroup() throws RepositoryException {
            if (group == null) {
                Authorizable authorizable = getUserManager().getAuthorizable(new PrincipalImpl(getName()));
                if (authorizable != null && authorizable.isGroup()) {
                    group = (org.apache.jackrabbit.api.security.user.Group) authorizable;
                }
            }
            return group;
        }
    }
}


