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
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.Query;

import com.google.common.base.Predicates;
import com.google.common.base.Strings;
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
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
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
 * been enabled.
 *
 * Please note that in contrast to the default principal provider implementation
 * shipped with Oak the group principals known and exposed by this provider are
 * not backed by an authorizable group and thus cannot be retrieved using
 * Jackrabbit user management API.
 *
 * @since Oak 1.5.3
 * @see org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DynamicSyncContext
 */
class ExternalGroupPrincipalProvider implements PrincipalProvider, ExternalIdentityConstants {

    private static final Logger log = LoggerFactory.getLogger(ExternalGroupPrincipalProvider.class);

    private static final String BINDING_PRINCIPAL_NAMES = "principalNames";

    private final Root root;
    private final NamePathMapper namePathMapper;

    private final UserManager userManager;
    private final AutoMembershipPrincipals autoMembershipPrincipals;

    ExternalGroupPrincipalProvider(@Nonnull Root root, @Nonnull UserConfiguration uc,
                                   @Nonnull NamePathMapper namePathMapper,
                                   @Nonnull Map<String, String[]> autoMembershipMapping) {
        this.root = root;
        this.namePathMapper = namePathMapper;

        userManager = uc.getUserManager(root, namePathMapper);
        autoMembershipPrincipals = new AutoMembershipPrincipals(autoMembershipMapping);
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
            Result result = findPrincipals(Strings.nullToEmpty(nameHint), false);
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

    //------------------------------------------------------------< private >---
    @CheckForNull
    private String getIdpName(@Nonnull Tree userTree) {
        PropertyState ps = userTree.getProperty(REP_EXTERNAL_ID);
        if (ps != null) {
            return ExternalIdentityRef.fromString(ps.getValue(Type.STRING)).getProviderName();
        } else {
            return null;
        }
    }

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
                // we have an 'external' user that has been synchronized with the dynamic-membership option
                Set<Group> groupPrincipals = Sets.newHashSet();
                for (String principalName : ps.getValue(Type.STRINGS)) {
                    groupPrincipals.add(new ExternalGroupPrincipal(principalName));
                }

                // add existing group principals as defined with the _autoMembership_ option.
                groupPrincipals.addAll(autoMembershipPrincipals.get(getIdpName(userTree)));
                return groupPrincipals;
            }
        }
        // group principals cannot be retrieved
        return ImmutableSet.of();
    }

    /**
     * Runs an Oak query searching for {@link #REP_EXTERNAL_PRINCIPAL_NAMES} properties
     * that match the given name or name hint.
     *
     * NOTE: ignore any principals listed in the {@link DefaultSyncConfig.User#autoMembership}
     * because they are expected to exist in the system and thus will be found
     * by another principal provider instance.
     *
     * @param nameHint The principal name or name hint to be searched for.
     * @param exactMatch boolean flag indicating if the query should search for
     *                   exact matching.
     * @return The query result.
     */
    @CheckForNull
    private Result findPrincipals(@Nonnull String nameHint, boolean exactMatch) {
        try {
            Map<String, ? extends PropertyValue> bindings = buildBinding(nameHint, exactMatch);
            String op = (exactMatch) ? " = " : " LIKE ";
            String statement = "SELECT '" + REP_EXTERNAL_PRINCIPAL_NAMES + "' FROM [rep:User] WHERE PROPERTY(["
                    + REP_EXTERNAL_PRINCIPAL_NAMES + "], '" + PropertyType.TYPENAME_STRING + "')"
                    + op + "$" + BINDING_PRINCIPAL_NAMES + QueryEngine.INTERNAL_SQL2_QUERY;
            return root.getQueryEngine().executeQuery(statement, Query.JCR_SQL2, bindings, namePathMapper.getSessionLocalMappings());
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * Build the map used for the query bindings.
     *
     * @param nameHint The name hint
     * @param exactMatch boolean flag indicating if the query should search for exact matching.
     * @return the bindings
     */
    @Nonnull
    private static Map<String, ? extends PropertyValue> buildBinding(@Nonnull String nameHint, boolean exactMatch) {
        String val = nameHint;
        if (!exactMatch) {
            // not-exact query matching required => add leading and trailing %
            if (nameHint.isEmpty()) {
                val = "%";
            } else {
                StringBuilder sb = new StringBuilder();
                sb.append('%');
                sb.append(nameHint.replace("%", "\\%").replace("_", "\\_"));
                sb.append('%');
                val = sb.toString();
            }
        }
        return Collections.singletonMap(BINDING_PRINCIPAL_NAMES, PropertyValues.newString(val));
    }

    //------------------------------------------------------< inner classes >---

    /**
     * Implementation of the {@link Group} interface representing external group
     * identities that are <strong>not</strong> represented as authorizable group
     * in the repository's user management.
     */
    private final class ExternalGroupPrincipal extends PrincipalImpl implements java.security.acl.Group {

        private ExternalGroupPrincipal(String principalName) {
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

    /**
     * {@link Group} principal iterator converting the query results of
     * {@link #findPrincipals(String, int)} and {@link #findPrincipals(int)}.
     * Since each result row provides the values of the {@code PropertyState},
     * which matched the query, this iterator needs to filter the individual
     * property values.
     *
     * Additional the iterator keeps track of principal names that have already
     * been served and will not return duplicates.
     *
     * @see #findPrincipals(String, int)
     * @see #findPrincipals(int)
     */
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
                String principalName = propValues.next();
                if (principalName != null && !processed.contains(principalName) && matchesQuery(principalName) ) {
                    processed.add(principalName);
                    return new ExternalGroupPrincipal(principalName);
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

    /**
     * {@code Principal} iterator representing the members of a given
     * {@link ExternalGroupPrincipal}. The members are collected through an
     * Oak {@link org.apache.jackrabbit.oak.query.Query Query}.
     *
     * Note that the query result is subject to permission evaluation for
     * the editing {@link Root} based on the accessibility of the individual
     * {@link #REP_EXTERNAL_PRINCIPAL_NAMES} properties that contain the
     * exact name of the external group principal.
     *
     * @see ExternalGroupPrincipal#members()
     */
    private final class MemberIterator extends AbstractLazyIterator<Principal> {

        /**
         * The query results containing the path of the user accounts
         * (i.e. members) that contain the target group principal in the
         * {@link #REP_EXTERNAL_PRINCIPAL_NAMES} property values.
         */
        private final Iterator<? extends ResultRow> rows;

        private MemberIterator(@Nonnull Result queryResult) {
            rows = queryResult.getRows().iterator();
        }

        @Override
        protected Principal getNext() {
            while (rows.hasNext()) {
                String userPath = rows.next().getPath();
                try {
                    Authorizable authorizable = userManager.getAuthorizableByPath(userPath);
                    if (authorizable != null) {
                        return authorizable.getPrincipal();
                    }
                } catch (RepositoryException e) {
                    log.debug("{}", e.getMessage());
                }
            }
            return null;
        }
    }

    private final class AutoMembershipPrincipals {

        private final Map<String, String[]> autoMembershipMapping;
        private final Map<String, Set<Group>> principalMap;

        private AutoMembershipPrincipals(@Nonnull Map<String, String[]> autoMembershipMapping) {
            this.autoMembershipMapping = autoMembershipMapping;
            this.principalMap = new ConcurrentHashMap<String, Set<Group>>(autoMembershipMapping.size());
        }

        @Nonnull
        private Collection<Group> get(@CheckForNull String idpName) {
            if (idpName == null) {
                return ImmutableSet.of();
            }

            Set<Group> principals;
            if (!principalMap.containsKey(idpName)) {
                String[] vs = autoMembershipMapping.get(idpName);
                if (vs == null) {
                    principals = ImmutableSet.of();
                } else {
                    ImmutableSet.Builder<Group> builder = ImmutableSet.builder();
                    for (String groupId : autoMembershipMapping.get(idpName)) {
                        try {
                            Authorizable gr = userManager.getAuthorizable(groupId);
                            if (gr != null && gr.isGroup()) {
                                Principal grPrincipal = gr.getPrincipal();
                                if (grPrincipal instanceof Group) {
                                    builder.add((Group) grPrincipal);
                                } else {
                                    log.warn("Principal of group {} is not of type java.security.acl.Group -> Ignoring", groupId);
                                }
                            } else {
                                log.warn("Configured auto-membership group {} does not exist -> Ignoring", groupId);
                            }
                        } catch (RepositoryException e) {
                            log.debug("Failed to retrieved 'auto-membership' group with id {}", groupId, e);
                        }
                    }
                    principals = builder.build();
                }
                principalMap.put(idpName, principals);
            } else {
                principals = principalMap.get(idpName);
            }
            return principals;
        }
    }
}