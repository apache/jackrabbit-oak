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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.text.ParseException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.Query;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@code PrincipalProvider} interface that exposes
 * 'external' principals of type {@link java.security.acl.Group}. 'External'
 * refers to the fact that these principals are defined and managed by an
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider}.
 *
 * For performance reasons this implementation doesn't lookup principals on the IDP
 * but relies on a persisted cache inside the repository where the names of these
 * external principals are synchronized to based on a configurable expiration time.
 *
 * Currently, the implementation respects the {@code rep:externalPrincipalNames}
 * properties, where group membership of external users gets synchronized if
 * {@link DefaultSyncConfig.User#getDynamicMembership() dynamic membership} has
 * been enabled. This synchronization is take care of by the {@link PrincipalSyncContext}.
 *
 * Please note that in contrast to the default principal provider implementation
 * shipped with Oak the group principals known and exposed by this provider are
 * not backed by a authorizable group and thus cannot be retrieved using
 * Jackrabbit user management API.
 */
class ExternalGroupPrincipalProvider implements PrincipalProvider, ExternalIdentityConstants {

    private static final Logger log = LoggerFactory.getLogger(ExternalGroupPrincipalProvider.class);

    private final Root root;
    private final NamePathMapper namePathMapper;
    private final UserManager userManager;

    ExternalGroupPrincipalProvider(Root root, UserConfiguration uc, NamePathMapper namePathMapper) {
        this.root = root;
        this.namePathMapper = namePathMapper;
        userManager = uc.getUserManager(root, namePathMapper);
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        Result result = findPrincipals(principalName, true);
        if (result != null && result.getRows().iterator().hasNext()) {
            return new ExternalGroupPrincipal(principalName);
        } else {
            return null;
        }
    }

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        if (!(principal instanceof Group)) {
            try {
                if (principal instanceof ItemBasedPrincipal) {
                    Tree t = root.getTree(((ItemBasedPrincipal) principal).getPath());
                    return getGroupPrincipals(t);
                } else {
                    return getGroupPrincipals(userManager.getAuthorizable(principal));
                }
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }

        }
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
        try {
            return getGroupPrincipals(userManager.getAuthorizable(userID));
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return ImmutableSet.of();
        }
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        if (PrincipalManager.SEARCH_TYPE_NOT_GROUP != searchType) {
            Result result = findPrincipals(buildSearchPattern(nameHint), false);
            if (result != null) {
                return Iterators.filter(new GroupPrincipalIterator(nameHint, result), Predicates.notNull());
            }
        }

        return Iterators.emptyIterator();
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }

    //--------------------------------------------------------------------------
    private Set<Group> getGroupPrincipals(@CheckForNull Authorizable authorizable) throws RepositoryException {
        if (authorizable != null && !authorizable.isGroup()) {
            Tree userTree = root.getTree(authorizable.getPath());
            return getGroupPrincipals(userTree);
        } else {
            return ImmutableSet.of();
        }
    }

    private Set<Group> getGroupPrincipals(@Nonnull Tree userTree) {
        if (userTree.exists() && UserUtil.isType(userTree, AuthorizableType.USER) && userTree.hasProperty(REP_EXTERNAL_PRINCIPAL_NAMES)) {
            PropertyState ps = userTree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
            if (ps != null) {
                Set<Group> groupPrincipals = Sets.newHashSet();
                for (String principalName : ps.getValue(Type.STRINGS)) {
                    groupPrincipals.add(new ExternalGroupPrincipal(principalName));
                }
                return groupPrincipals;
            }
        }
        // no group principals defined
        return ImmutableSet.of();
    }

    @CheckForNull
    private Result findPrincipals(@Nonnull String queryString, boolean exactMatch) {
        try {
            Map<String, ? extends PropertyValue> bindings = Collections.singletonMap("princNames", PropertyValues.newString(queryString));
            String op = (exactMatch) ? " = " : " LIKE ";
            String statement = "SELECT '" + REP_EXTERNAL_PRINCIPAL_NAMES + "' FROM [rep:User] WHERE PROPERTY(["
                    + REP_EXTERNAL_PRINCIPAL_NAMES + "], '" + PropertyType.TYPENAME_STRING + "')"
                    + op + "$princNames" + QueryEngine.INTERNAL_SQL2_QUERY;
            return root.getQueryEngine().executeQuery(statement, Query.JCR_SQL2, bindings, namePathMapper.getSessionLocalMappings());
        } catch (ParseException e) {
            return null;
        }
    }

    @Nonnull
    private static String buildSearchPattern(@Nullable String nameHint) {
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

    /**
     *
     */
    private final class ExternalGroupPrincipal extends PrincipalImpl implements java.security.acl.Group {

        public ExternalGroupPrincipal(String principalName) {
            super(principalName);

        }

        @Override
        public boolean addMember(Principal user) {
            if (isMember(user)) {
                return false;
            } else {
                throw new UnsupportedOperationException("Adding members to external group principals is not supported.");
            }
        }

        @Override
        public boolean removeMember(Principal user) {
            if (!isMember(user)) {
                return false;
            } else {
                throw new UnsupportedOperationException("Removing members from external group principals is not supported.");
            }
        }

        @Override
        public boolean isMember(Principal member) {
            if (member instanceof Group) {
                return false;
            }
            try {
                String name = getName();
                if (member instanceof ItemBasedPrincipal) {
                    Tree tree = root.getTree(((ItemBasedPrincipal) member).getPath());
                    if (UserUtil.isType(tree, AuthorizableType.USER)) {
                        PropertyState ps = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
                        return (ps != null && Iterables.contains(ps.getValue(Type.STRINGS), name));
                    }
                } else {
                    Authorizable a = userManager.getAuthorizable(member);
                    if (a != null && !a.isGroup()) {
                        Value[] vs = a.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
                        if (vs != null) {
                            for (Value v : vs) {
                                if (name.equals(v.getString())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }
            return false;
        }

        @Override
        public Enumeration<? extends Principal> members() {
            Result result = findPrincipals(getName(), true);
            if (result != null) {
                return Iterators.asEnumeration(new MemberIterator(result));
            } else {
                return Iterators.asEnumeration(Iterators.<Principal>emptyIterator());
            }
        }
    }

    private final class GroupPrincipalIterator extends AbstractLazyIterator<Principal> {

        private final Set<String> processed = new HashSet<String>();

        private final String queryString;
        private final Iterator<? extends ResultRow> rows;

        private Iterator<String> propValues = Iterators.emptyIterator();

        private GroupPrincipalIterator(@Nullable String queryString, @Nonnull Result queryResult) {
            this.queryString = queryString;
            rows = queryResult.getRows().iterator();
        }

        @Override
        protected Principal getNext() {
            if (!propValues.hasNext()) {
                if (rows.hasNext()) {
                    propValues = rows.next().getValue(REP_EXTERNAL_PRINCIPAL_NAMES).getValue(Type.STRINGS).iterator();
                } else {
                    propValues = Iterators.emptyIterator();
                }
            }
            while (propValues.hasNext()) {
                String princName = propValues.next();
                if (princName != null && matchesQuery(princName) && !processed.contains(princName)) {
                    processed.add(princName);
                    return new ExternalGroupPrincipal(princName);
                }
            }
            return null;
        }

        private boolean matchesQuery(@Nonnull String principalName) {
            if (queryString == null) {
                return true;
            } else {
                return principalName.contains(queryString);
            }
        }
    }

    private final class MemberIterator extends AbstractLazyIterator<Principal> {

        private final Set<String> processed = new HashSet<String>();

        private final Iterator<? extends ResultRow> rows;
        private Iterator<String> propValues = Iterators.emptyIterator();

        private MemberIterator(@Nonnull Result queryResult) {
            rows = queryResult.getRows().iterator();
        }

        @Override
        protected Principal getNext() {
            while (rows.hasNext()) {
                String userPath = rows.next().getPath();
                if (processed.add(userPath)) {
                    try {
                        Authorizable authorizable = userManager.getAuthorizableByPath(userPath);
                        if (authorizable != null) {
                            return authorizable.getPrincipal();
                        }
                    } catch (RepositoryException e) {
                        log.debug(e.getMessage());
                    }
                }
            }
            return null;
        }
    }
}