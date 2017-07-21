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
import java.security.acl.Group;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.LongUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.user.query.QueryUtil;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_BINDINGS;
import static org.apache.jackrabbit.oak.api.Type.STRING;

/**
 * The {@code PrincipalProviderImpl} is a principal provider implementation
 * that operates on principal information read from user information exposed by
 * the configured {@link org.apache.jackrabbit.api.security.user.UserManager}.
 */
class UserPrincipalProvider implements PrincipalProvider {

    private static final Logger log = LoggerFactory.getLogger(UserPrincipalProvider.class);

    static final String PARAM_CACHE_EXPIRATION = "cacheExpiration";
    static final long EXPIRATION_NO_CACHE = 0;

    private static final long MEMBERSHIP_THRESHOLD = 0;

    private final Root root;
    private final UserConfiguration config;
    private final NamePathMapper namePathMapper;

    private final UserProvider userProvider;
    private final MembershipProvider membershipProvider;

    private final long expiration;
    private final boolean cacheEnabled;

    UserPrincipalProvider(@Nonnull Root root,
                          @Nonnull UserConfiguration userConfiguration,
                          @Nonnull NamePathMapper namePathMapper) {
        this.root = root;
        this.config = userConfiguration;
        this.namePathMapper = namePathMapper;

        this.userProvider = new UserProvider(root, config.getParameters());
        this.membershipProvider = new MembershipProvider(root, config.getParameters());

        expiration = config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE);
        cacheEnabled = (expiration > EXPIRATION_NO_CACHE && root.getContentSession().getAuthInfo().getPrincipals().contains(SystemPrincipal.INSTANCE));
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        Tree authorizableTree = userProvider.getAuthorizableByPrincipal(new PrincipalImpl(principalName));
        Principal principal = createPrincipal(authorizableTree);

        if (principal == null) {
            // no such principal or error while accessing principal from user/group
            return (EveryonePrincipal.NAME.equals(principalName)) ? EveryonePrincipal.getInstance() : null;
        } else {
            return principal;
        }
    }

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        Tree tree = getAuthorizableTree(principal);
        if (tree == null) {
            return Collections.emptySet();
        } else {
            return getGroupMembership(tree);
        }
    }

    @Nonnull
    @Override
    public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
        Set<Principal> principals = new HashSet<Principal>();
        Tree tree = userProvider.getAuthorizable(userID);
        if (tree != null && UserUtil.isType(tree, AuthorizableType.USER)) {
            Principal userPrincipal = createUserPrincipal(userID, tree);
            if (userPrincipal != null) {
                principals.add(userPrincipal);
                principals.addAll(getGroupMembership(tree));
            }
        }
        return principals;
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(final String nameHint,
                                                        final int searchType) {
        try {
            AuthorizableType type = AuthorizableType.getType(searchType);
            StringBuilder statement = new StringBuilder()
                    .append(QueryUtil.getSearchRoot(type, config.getParameters()))
                    .append("//element(*,").append(QueryUtil.getNodeTypeName(type)).append(')')
                    .append("[jcr:like(@rep:principalName,'")
                    .append(buildSearchPattern(nameHint))
                    .append("')]");

            Result result = root.getQueryEngine().executeQuery(
                    statement.toString(), javax.jcr.query.Query.XPATH,
                    NO_BINDINGS, namePathMapper.getSessionLocalMappings());

            Iterator<Principal> principals = Iterators.filter(
                    Iterators.transform(result.getRows().iterator(), new ResultRowToPrincipal()),
                    Predicates.notNull());

            if (matchesEveryone(nameHint, searchType)) {
                principals = Iterators.concat(principals, Iterators.singletonIterator(EveryonePrincipal.getInstance()));
                return Iterators.filter(principals, new EveryonePredicate());
            } else {
                return principals;
            }
        } catch (ParseException e) {
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
    @CheckForNull
    private Tree getAuthorizableTree(@Nonnull Principal principal) {
        return userProvider.getAuthorizableByPrincipal(principal);
    }

    @CheckForNull
    private Principal createPrincipal(@CheckForNull Tree authorizableTree) {
        Principal principal = null;
        if (authorizableTree != null) {
            AuthorizableType type = UserUtil.getType(authorizableTree);
            if (AuthorizableType.GROUP == type) {
                principal = createGroupPrincipal(authorizableTree);
            } else if (AuthorizableType.USER == type) {
                principal = createUserPrincipal(UserUtil.getAuthorizableId(authorizableTree, type), authorizableTree);
            }
        }
        return principal;
    }

    @CheckForNull
    private Principal createUserPrincipal(@Nonnull String id, @Nonnull Tree userTree) {
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

    @CheckForNull
    private Group createGroupPrincipal(@Nonnull Tree groupTree) {
        String principalName = getPrincipalName(groupTree);
        if (principalName == null) {
            return null;
        }
        return new GroupPrincipal(principalName, groupTree);
    }

    @CheckForNull
    private static String getPrincipalName(@Nonnull Tree tree) {
        PropertyState principalName = tree.getProperty(UserConstants.REP_PRINCIPAL_NAME);
        if (principalName != null) {
            return principalName.getValue(STRING);
        } else {
            String msg = "Authorizable without principal name " + UserUtil.getAuthorizableId(tree);
            log.warn(msg);
            return null;
        }
    }

    @Nonnull
    private Set<Group> getGroupMembership(@Nonnull Tree authorizableTree) {
        Set<Group> groupPrincipals = null;
        boolean doCache = cacheEnabled && UserUtil.isType(authorizableTree, AuthorizableType.USER);
        if (doCache) {
            groupPrincipals = readGroupsFromCache(authorizableTree);
        }

        // caching not configured or cache expired: use the membershipProvider to calculate
        if (groupPrincipals == null) {
            groupPrincipals = new HashSet<Group>();
            Iterator<String> groupPaths = membershipProvider.getMembership(authorizableTree, true);
            while (groupPaths.hasNext()) {
                Tree groupTree = userProvider.getAuthorizableByPath(groupPaths.next());
                if (groupTree != null && UserUtil.isType(groupTree, AuthorizableType.GROUP)) {
                    Group gr = createGroupPrincipal(groupTree);
                    if (gr != null) {
                        groupPrincipals.add(gr);
                    }
                }
            }

            // remember the regular groups in case caching is enabled
            if (doCache) {
                cacheGroups(authorizableTree, groupPrincipals);
            }
        }

        // add the dynamic everyone principal group which is not included in
        // the 'getMembership' call.
        groupPrincipals.add(EveryonePrincipal.getInstance());
        return groupPrincipals;
    }

    private void cacheGroups(@Nonnull Tree authorizableNode, @Nonnull Set<Group> groupPrincipals) {
        try {
            root.refresh();
            Tree cache = authorizableNode.getChild(CacheConstants.REP_CACHE);
            if (!cache.exists()) {
                if (groupPrincipals.size() <= MEMBERSHIP_THRESHOLD) {
                    log.debug("Omit cache creation for user without group membership at " + authorizableNode.getPath());
                    return;
                } else {
                    log.debug("Create new group membership cache at " + authorizableNode.getPath());
                    cache = TreeUtil.addChild(authorizableNode, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
                }
            }

            cache.setProperty(CacheConstants.REP_EXPIRATION, LongUtils.calculateExpirationTime(expiration));
            String value = (groupPrincipals.isEmpty()) ? "" : Joiner.on(",").join(Iterables.transform(groupPrincipals, new Function<Group, String>() {
                @Override
                public String apply(Group input) {
                    return Text.escape(input.getName());
                }
            }));
            cache.setProperty(CacheConstants.REP_GROUP_PRINCIPAL_NAMES, value);

            root.commit(CacheValidatorProvider.asCommitAttributes());
            log.debug("Cached group membership at " + authorizableNode.getPath());

        } catch (AccessDeniedException e) {
            log.debug("Failed to cache group membership", e.getMessage());
        } catch (CommitFailedException e) {
            log.debug("Failed to cache group membership", e.getMessage(), e);
        } finally {
            root.refresh();
        }
    }

    @CheckForNull
    private Set<Group> readGroupsFromCache(@Nonnull Tree authorizableNode) {
        Tree principalCache = authorizableNode.getChild(CacheConstants.REP_CACHE);
        if (!principalCache.exists()) {
            log.debug("No group cache at " + authorizableNode.getPath());
            return null;
        }

        if (isValidCache(principalCache)) {
            log.debug("Reading group membership at " + authorizableNode.getPath());

            String str = TreeUtil.getString(principalCache, CacheConstants.REP_GROUP_PRINCIPAL_NAMES);
            if (str == null || str.isEmpty()) {
                return new HashSet<Group>(1);
            }

            Set<Group> groups = new HashSet<Group>();
            for (String s : Text.explode(str, ',')) {
                final String name = Text.unescape(s);
                groups.add(new CachedGroupPrincipal(name));
            }
            return groups;
        } else {
            log.debug("Expired group cache for " + authorizableNode.getPath());
            return null;
        }
    }

    private static boolean isValidCache(Tree principalCache)  {
        long expirationTime = TreeUtil.getLong(principalCache, CacheConstants.REP_EXPIRATION, EXPIRATION_NO_CACHE);
        long now = new Date().getTime();
        return expirationTime > EXPIRATION_NO_CACHE && now < expirationTime;
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
     * Function to covert an authorizable tree (as obtained from the query result) to a principal.
     */
    private final class ResultRowToPrincipal implements Function<ResultRow, Principal> {
        @Override
        public Principal apply(@Nullable ResultRow resultRow) {
            return (resultRow != null) ? createPrincipal(resultRow.getTree(null)) : null;
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

    //--------------------------------------------------------------------------
    // Group Principal implementations that retrieve member information on demand
    //--------------------------------------------------------------------------

    private abstract class BaseGroupPrincipal extends AbstractGroupPrincipal {

        private UserManager userManager;

        BaseGroupPrincipal(@Nonnull String principalName, @Nonnull Tree groupTree) {
            super(principalName, groupTree, namePathMapper);
        }

        BaseGroupPrincipal(@Nonnull String principalName, @Nonnull String groupPath) {
            super(principalName, groupPath, namePathMapper);
        }

        @Override
        UserManager getUserManager() {
            if (userManager == null) {
                userManager = config.getUserManager(root, namePathMapper);
            }
            return userManager;
        }

        @Override
        boolean isEveryone() {
            return EveryonePrincipal.NAME.equals(getName());
        }

        @Override
        boolean isMember(@Nonnull Authorizable authorizable) throws RepositoryException {
            org.apache.jackrabbit.api.security.user.Group g = getGroup();
            return g != null && g.isMember(authorizable);
        }

        @Nonnull
        @Override
        Iterator<Authorizable> getMembers() throws RepositoryException {
            org.apache.jackrabbit.api.security.user.Group g = getGroup();
            return (g == null) ? Iterators.<Authorizable>emptyIterator() : g.getMembers();
        }

        @CheckForNull
        abstract org.apache.jackrabbit.api.security.user.Group getGroup()throws RepositoryException;
    }

    /**
     * Implementation of {@link AbstractGroupPrincipal} that reads the underlying
     * authorizable group lazily in case the group membership must be retrieved.
     */
    private final class GroupPrincipal extends BaseGroupPrincipal {

        private org.apache.jackrabbit.api.security.user.Group group;

        GroupPrincipal(@Nonnull String principalName, @Nonnull Tree groupTree) {
            super(principalName, groupTree);
        }

        @Override
        @CheckForNull
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

    private final class CachedGroupPrincipal extends BaseGroupPrincipal {

        private org.apache.jackrabbit.api.security.user.Group group;

        CachedGroupPrincipal(@Nonnull String principalName) {
            super(principalName, "");
        }

        @Override
        String getOakPath() {
            String groupPath = getPath();
            return (groupPath == null) ? null : namePathMapper.getOakPath(getPath());
        }

        @Override
        public String getPath() {
            try {
                org.apache.jackrabbit.api.security.user.Group gr = getGroup();
                return (gr == null) ? null : gr.getPath();
            } catch (RepositoryException e) {
                log.error("Failed to retrieve path from group principal", e.getMessage());
                return null;
            }
        }

        @Override
        @CheckForNull
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


