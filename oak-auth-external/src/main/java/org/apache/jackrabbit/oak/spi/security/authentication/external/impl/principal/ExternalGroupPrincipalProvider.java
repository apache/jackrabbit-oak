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

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.getIdpName;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.isSameIDP;

import java.security.Principal;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.query.Query;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
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
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachedMembershipReader;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@code PrincipalProvider} interface that exposes
 * 'external' principals of type {@link org.apache.jackrabbit.api.security.principal.GroupPrincipal}. 'External'
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
class ExternalGroupPrincipalProvider implements PrincipalProvider, ExternalIdentityConstants, DynamicMembershipProvider {

    private static final Logger log = LoggerFactory.getLogger(ExternalGroupPrincipalProvider.class);
    static final String CACHE_PRINCIPAL_NAMES = "rep:externalLocalPrincipalNames";

    private static final String BINDING_PRINCIPAL_NAMES = "principalNames";

    private final Root root;
    private final NamePathMapper namePathMapper;

    private final UserManager userManager;
    private final Set<String> idpNamesWithDynamicGroups;
    private final boolean hasOnlyDynamicGroups;

    private final AutoMembershipPrincipals autoMembershipPrincipals;
    private final AutoMembershipPrincipals groupAutoMembershipPrincipals;

    private Function<String, CachedMembershipReader> cacheReaderFactory = (name) -> null;

    ExternalGroupPrincipalProvider(@NotNull Root root, @NotNull UserManager userManager,
                                   @NotNull NamePathMapper namePathMapper,
                                   @NotNull SyncConfigTracker syncConfigTracker) {
        this.root = root;
        this.namePathMapper = namePathMapper;
        this.userManager = userManager;

        idpNamesWithDynamicGroups = syncConfigTracker.getIdpNamesWithDynamicGroups();
        hasOnlyDynamicGroups = (idpNamesWithDynamicGroups.size() == syncConfigTracker.getServiceReferences().length);

        autoMembershipPrincipals = new AutoMembershipPrincipals(userManager, syncConfigTracker.getAutoMembership(), syncConfigTracker.getAutoMembershipConfig());
        groupAutoMembershipPrincipals = (idpNamesWithDynamicGroups.isEmpty()) ? null : new AutoMembershipPrincipals(userManager, syncConfigTracker.getGroupAutoMembership(), syncConfigTracker.getAutoMembershipConfig());
    }

    ExternalGroupPrincipalProvider(@NotNull Root root,
                                   @NotNull UserConfiguration userConfiguration,
                                   @NotNull NamePathMapper namePathMapper,
                                   @NotNull SyncConfigTracker syncConfigTracker) {
        this.root = root;
        this.namePathMapper = namePathMapper;
        this.userManager = userConfiguration.getUserManager(root, namePathMapper);

        idpNamesWithDynamicGroups = syncConfigTracker.getIdpNamesWithDynamicGroups();
        hasOnlyDynamicGroups = (idpNamesWithDynamicGroups.size() == syncConfigTracker.getServiceReferences().length);

        autoMembershipPrincipals = new AutoMembershipPrincipals(userManager, syncConfigTracker.getAutoMembership(), syncConfigTracker.getAutoMembershipConfig());
        groupAutoMembershipPrincipals = (idpNamesWithDynamicGroups.isEmpty()) ? null : new AutoMembershipPrincipals(userManager, syncConfigTracker.getGroupAutoMembership(), syncConfigTracker.getAutoMembershipConfig());

        cacheReaderFactory = (String idpName) -> userConfiguration.getCachedMembershipReader(root,
                (principalName) -> new CachedGroupPrincipal(principalName, userManager),
                CACHE_PRINCIPAL_NAMES);
    }

    // Tests only
    ExternalGroupPrincipalProvider(@NotNull Root root, @NotNull UserConfiguration userConfiguration,
                                   @NotNull NamePathMapper namePathMapper, 
                                   @NotNull String idpName,
                                   @NotNull DefaultSyncConfig syncConfig,
                                   @NotNull Set<String> idpNamesWithDynamicGroups, boolean hasOnlyDynamicGroups) {
        this.root = root;
        this.namePathMapper = namePathMapper;
        this.userManager = userConfiguration.getUserManager(root, namePathMapper);

        this.idpNamesWithDynamicGroups = idpNamesWithDynamicGroups;
        this.hasOnlyDynamicGroups = hasOnlyDynamicGroups;

        autoMembershipPrincipals = new AutoMembershipPrincipals(userManager, 
                Collections.singletonMap(idpName, Iterables.toArray(Iterables.concat(syncConfig.user().getAutoMembership(),syncConfig.group().getAutoMembership()), String.class)),
                Collections.singletonMap(idpName, syncConfig.user().getAutoMembershipConfig()));
        groupAutoMembershipPrincipals = (idpNamesWithDynamicGroups.isEmpty()) ? null : 
                new AutoMembershipPrincipals(userManager, 
                Collections.singletonMap(idpName, syncConfig.group().getAutoMembership().toArray(new String[0])),
                Collections.singletonMap(idpName, syncConfig.group().getAutoMembershipConfig()));
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
        if (hasOnlyDynamicGroups) {
            // shortcut: the default user-principal-provider will return the group principal
            return null;
        }

        Result result = findPrincipals(principalName, true);
        Iterator<? extends ResultRow> rows = (result == null) ? Collections.emptyIterator() : result.getRows().iterator();
        if (rows.hasNext()) {
            return createExternalGroupPrincipal(principalName, getIdpName(rows.next()));
        }
        return null;
    }

    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        if (hasDynamicMembershipPrincipals(principal)) {
            try {
                if (principal instanceof ItemBasedPrincipal) {
                    String path = ((ItemBasedPrincipal) principal).getPath();
                    Tree t = root.getTree(path);
                    Authorizable a = userManager.getAuthorizableByPath(path);
                    if (a != null) {
                        return getGroupPrincipals(a, t);
                    }
                } else {
                    return getGroupPrincipals(userManager.getAuthorizable(principal), false);
                }
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
            }

        }
        return Set.of();
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        try {
            return getGroupPrincipals(userManager.getAuthorizable(userID), true);
        } catch (RepositoryException e) {
            log.debug(e.getMessage());
            return Set.of();
        }
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        // this provider only serves GroupPrincipal instances for external group accounts that have
        // not been synchronized into the repository. if groups for all configured IDPs are synchronzied as
        // dynamic groups the default principal-provider backed by user/group accounts will be in charge.
        if (PrincipalManager.SEARCH_TYPE_NOT_GROUP == searchType || hasOnlyDynamicGroups) {
            return Collections.emptyIterator();
        }

        // search for external group principals that have not been synchronzied into the repository
        Result result = findPrincipals(Objects.toString(nameHint, ""), false);
        if (result != null) {
            return Iterators.filter(new GroupPrincipalIterator(nameHint, result), Objects::nonNull);
        } else {
            return Collections.emptyIterator();
        }
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, boolean fullText, int searchType,
                                                        long offset, long limit) {
        Iterator<? extends Principal> principals = findPrincipals(nameHint, searchType);
        if (!principals.hasNext()) {
            return Collections.emptyIterator();
        }

        Spliterator<? extends Principal> spliterator = Spliterators.spliteratorUnknownSize(principals, 0);
        Stream<? extends Principal> stream = StreamSupport.stream(spliterator, false);
        stream = stream.sorted(Comparator.comparing(Principal::getName));
        if (offset > 0) {
            stream = stream.skip(offset);
        }
        if (limit >= 0) {
            stream = stream.limit(limit);
        }
        return stream.iterator();
    }

    //------------------------------------------< DynamicMembershipProvider >---

    @Override
    public boolean coversAllMembers(@NotNull Group group) {
        return isDynamic(group) && !DynamicGroupUtil.hasStoredMemberInfo(group, root);
    }

    @Override
    public @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) throws RepositoryException {
        if (!isDynamic(group)) {
            return Collections.emptyIterator();
        } else {
            Result result = findPrincipals(group.getPrincipal().getName(), true);
            if (result != null) {
                return new MemberIterator<Authorizable>(result) {
                    @Override
                    Authorizable get(@NotNull Authorizable authorizable) {
                        return authorizable;
                    }
                };
            } else {
                return Collections.emptyIterator();
            }
        }
    }

    @Override
    public boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        if (authorizable.isGroup() || !isDynamic(authorizable)) {
            return false;
        } else {
            if (isDynamic(group) && isDynamicMember(group.getPrincipal().getName(), authorizable)) {
                return true;
            }
            // test for inheritance from dynamic groups through cross-IDP membership
            if (includeInherited) {
                Iterator<Group> dynamicGroups = getMembership(authorizable, false);
                while (dynamicGroups.hasNext()) {
                    Group dynamicGroup = dynamicGroups.next();
                    if (group.isMember(dynamicGroup)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    @Override
    public @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        if (authorizable.isGroup() || !isDynamic(authorizable)) {
            return Collections.emptyIterator();
        } else {
            Value[] vs = authorizable.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
            if (vs == null || vs.length == 0) {
                return Collections.emptyIterator();
            }

            Set<Value> valueSet = ImmutableSet.copyOf(vs);
            Iterator<Group> declared = Iterators.filter(Iterators.transform(valueSet.iterator(), value -> {
                try {
                    String groupPrincipalName = value.getString();
                    Authorizable gr = userManager.getAuthorizable(new PrincipalImpl(groupPrincipalName));
                    return isValidGroup(gr, authorizable) ? (Group) gr : null;
                } catch (RepositoryException e) {
                    return null;
                }
            }), Objects::nonNull);
            if (includeInherited) {
                // retrieve groups inherited from dynamic groups through cross-IDP membership
                return new InheritedMembershipIterator(declared);
            } else {
                return declared;
            }
        }
    }

    /**
     * Verify that the dynamic external group belongs to the same IDP as the member for which groups are being retrieved.
     * Note that {@code DynamicSyncContext#syncMembership} also asserts there are no collisions between external principal names.
     * 
     * @param group The authorizable matching an entry in the {@code REP_EXTERNAL_PRINCIPAL_NAMES} property of the 
     *              target member.
     * @param member The target member
     * @return {@code true} if the given authorizable is a valid external group that belongs to the same IDP; {@code false} otherwise.
     * @throws RepositoryException If an error occors while retrieving the IDP name
     */
    private static boolean isValidGroup(@Nullable Authorizable group, @NotNull Authorizable member) throws RepositoryException {
        if (group == null || !group.isGroup()) {
            return false;
        }
        return isSameIDP(group, member);
    }

    /**
     * Returns true if the given user/group belongs to an IDP that has dynamic-group configuration option enabled.
     */
    private boolean isDynamic(@NotNull Authorizable authorizable) {
        if (idpNamesWithDynamicGroups.isEmpty()) {
            return false;
        }
        try {
            ExternalIdentityRef extIdRef = DefaultSyncContext.getIdentityRef(authorizable);
            if (extIdRef == null) {
                return false;
            } else {
                return idpNamesWithDynamicGroups.contains(extIdRef.getProviderName());
            }
        } catch (RepositoryException e) {
            log.warn("Cannot retrieve rep:externalId property from identity {}", authorizable);
            return false;
        }
    }

    //------------------------------------------------------------< private >---

    @NotNull
    private Set<Principal> getGroupPrincipals(@Nullable Authorizable authorizable, boolean ignoreGroup) throws RepositoryException {
        if (authorizable == null || (authorizable.isGroup() && ignoreGroup)) {
            return Set.of();
        } else {
            return getGroupPrincipals(authorizable, DynamicGroupUtil.getTree(authorizable, root));
        }
    }

    @NotNull
    private Set<Principal> getGroupPrincipals(@NotNull Authorizable authorizable, @NotNull Tree tree) throws RepositoryException {
        if (!tree.exists()) {
            return Collections.emptySet();
        }
        String idpName = getIdpName(tree);
        if (idpName == null) {
            // a tree without rep:externalid that would mark a valid synchronized external identity
            return Collections.emptySet();
        }
        if (UserUtil.isType(tree, AuthorizableType.USER)) {
            PropertyState ps = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
            if (ps != null) {
                Set<Principal> externalGroups = new HashSet<>();
                for (String principalName : ps.getValue(Type.STRINGS)) {
                    externalGroups.add(createExternalGroupPrincipal(principalName, idpName));
                }

                Set<Principal> groupPrincipals = new HashSet<>(externalGroups);
                // Caching the inherited group principals is only required for dynamic groups
                CachedMembershipReader membershipReader = cacheReaderFactory.apply(idpName);
                if (membershipReader != null) {
                    groupPrincipals.addAll(membershipReader.readMembership(tree, (userTree) -> getInheritedPrincipals(externalGroups, idpName)));
                } else {
                    groupPrincipals.addAll(getInheritedPrincipals(groupPrincipals, idpName));
                }

                // add existing group principals as defined with the _autoMembership_ option.
                groupPrincipals.addAll(getAutomembershipPrincipals(idpName, authorizable));

                return groupPrincipals;
            } else {
                return Collections.emptySet();
            }
        } else {
            // resolve automembership for dynamic groups
            // NOTE: no need to resolve inherited local principals as this is covered by the default principal provider
            return getAutomembershipPrincipals(idpName, authorizable);
        }
    }

    /**
     * Special handling for the case where dynamic groups have been added to local groups
     * @return set of inherited group principals 
     */
    private Set<Principal> getInheritedPrincipals(@NotNull Set<Principal> externalGroupPrincipals, @NotNull String idpName) {
        if (idpNamesWithDynamicGroups.contains(idpName)) {
            Set<Principal> inherited = new HashSet<>();
            for (Principal p : externalGroupPrincipals) {inherited.addAll(DynamicGroupUtil.getInheritedPrincipals(p, userManager));
            }
            return inherited;
        } else {
            // no dynamic groups created => membership with local groups cannot be created
            return Collections.emptySet();
        }
    }

    private Set<Principal> getAutomembershipPrincipals(@NotNull String idpName, @NotNull Authorizable authorizable) {
        if (authorizable.isGroup()) {
            // no need to check for 'groupAutoMembershipPrincipals' being null as it is created if 'idpNamesWithDynamicGroups' is not empty
            return (idpNamesWithDynamicGroups.contains(idpName)) ? 
                    groupAutoMembershipPrincipals.getAutoMembership(idpName, authorizable, true).keySet() :
                    Collections.emptySet();
        } else {
            return autoMembershipPrincipals.getAutoMembership(idpName, authorizable, true).keySet();
        }
    }

    /**
     * Runs an Oak query searching for {@link #REP_EXTERNAL_PRINCIPAL_NAMES} properties
     * that match the given name or name hint.
     *
     * NOTE: ignore any principals listed in the {@link DefaultSyncConfig.User#getAutoMembership()}
     * because they are expected to exist in the system and thus will be found
     * by another principal provider instance.
     *
     * @param nameHint The principal name or name hint to be searched for.
     * @param exactMatch boolean flag indicating if the query should search for
     *                   exact matching.
     * @return The query result.
     */
    @Nullable
    private Result findPrincipals(@NotNull String nameHint, boolean exactMatch) {
        try {
            Map<String, ? extends PropertyValue> bindings = buildBinding(nameHint, exactMatch);
            String op = (exactMatch) ? " = " : " LIKE ";
            String statement = "SELECT [" + REP_EXTERNAL_PRINCIPAL_NAMES + "] FROM [rep:User] WHERE PROPERTY(["
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
    @NotNull
    private static Map<String, ? extends PropertyValue> buildBinding(@NotNull String nameHint, boolean exactMatch) {
        String val = nameHint;
        if (!exactMatch) {
            // not-exact query matching required => add leading and trailing %
            if (nameHint.isEmpty()) {
                val = "%";
            } else {
                val = '%' + nameHint.replace("%", "\\%").replace("_", "\\_") + '%';
            }
        }
        return Collections.singletonMap(BINDING_PRINCIPAL_NAMES, PropertyValues.newString(val));
    }

    private static boolean isDynamicMember(@NotNull String groupPrincipalName, @Nullable Authorizable member) throws RepositoryException {
        if (member == null || member.isGroup()) {
            return false;
        }

        Value[] vs = member.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
        if (vs == null) {
            return false;
        }
        for (Value v : vs) {
            if (groupPrincipalName.equals(v.getString())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasDynamicMembershipPrincipals(@NotNull Principal principal) {
        if (!GroupPrincipals.isGroup(principal)) {
            return true;
        } else if (principal instanceof ExternalGroupPrincipal) {
            return idpNamesWithDynamicGroups.contains(((ExternalGroupPrincipal) principal).getIdpName());
        } else {
            return principal instanceof ItemBasedPrincipal;
        }
    }

    //------------------------------------------------------< inner classes >---

    private GroupPrincipal createExternalGroupPrincipal(@NotNull String principalName, @Nullable String idpName) {
        if (idpNamesWithDynamicGroups.contains(idpName)) {
            return new ExternalGroupPrincipalItemBased(principalName, idpName);
        } else {
            return new ExternalGroupPrincipal(principalName, idpName);
        }
    }

    /**
     * Implementation of the {@link org.apache.jackrabbit.api.security.principal.GroupPrincipal} interface representing 
     * external group identities that are represented as authorizable group in the repository's user management i.e.   
     * the {@code SyncHandler} configured for the IDP with the given name has dynamic-group option enabled.
     */
    private final class ExternalGroupPrincipalItemBased extends ExternalGroupPrincipal implements ItemBasedPrincipal {

        private ExternalGroupPrincipalItemBased(@NotNull String principalName, @Nullable String idpName) {
            super(principalName, idpName);
        }

        @Override
        public @NotNull String getPath() throws RepositoryException {
            Authorizable a = userManager.getAuthorizable(this);
            if (a == null) {
                throw new RepositoryException("Cannot determine path for principal '" + getName() + "'. Group with this principal name does not exist.");
            } else {
                return a.getPath();
            }
        }
    }

    /**
     * Implementation of the {@link org.apache.jackrabbit.api.security.principal.GroupPrincipal} interface representing external group
     * identities that are <strong>not</strong> represented as authorizable group
     * in the repository's user management.
     */
    private class ExternalGroupPrincipal extends PrincipalImpl implements GroupPrincipal {

        private final String idpName;

        private ExternalGroupPrincipal(@NotNull String principalName, @Nullable String idpName) {
            super(principalName);
            this.idpName = Objects.toString(idpName, "");
        }

        /**
         * @return The IDP-name of the external user on which this external-group principal name was contained in the rep:externalPrincipalNames property.
         */
        private @NotNull String getIdpName() {
            return idpName;
        }

        @Override
        public boolean isMember(@NotNull Principal member) {
            if (GroupPrincipals.isGroup(member)) {
                return false;
            }
            try {
                return isContainedInExternalPrincipalNames(member);
            } catch (RepositoryException e) {
                log.debug(e.getMessage());
                return false;
            }
        }

        private boolean isContainedInExternalPrincipalNames(@NotNull Principal member) throws RepositoryException {
            String name = getName();
            if (member instanceof ItemBasedPrincipal) {
                Tree tree = root.getTree(((ItemBasedPrincipal) member).getPath());
                if (UserUtil.isType(tree, AuthorizableType.USER)) {
                    PropertyState ps = tree.getProperty(REP_EXTERNAL_PRINCIPAL_NAMES);
                    return (ps != null && Iterables.contains(ps.getValue(Type.STRINGS), name));
                }
            } else {
                Authorizable a = userManager.getAuthorizable(member);
                return isDynamicMember(name, a);
            }
            return false;
        }

        @NotNull
        @Override
        public Enumeration<? extends Principal> members() {
            Result result = findPrincipals(getName(), true);
            if (result != null) {
                return Iterators.asEnumeration(new MemberIterator<Principal>(result) {
                    @Override
                    Principal get(@NotNull Authorizable authorizable) throws RepositoryException {
                        return authorizable.getPrincipal();
                    }
                });
            } else {
                return Iterators.asEnumeration(Collections.emptyIterator());
            }
        }
    }

    /**
     * Principal iterator converting the query results of
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

        private final Set<String> processed = new HashSet<>();

        private final String queryString;
        private final Iterator<? extends ResultRow> rows;

        private Iterator<String> propValues = Collections.emptyIterator();
        private String idpName = "";

        private GroupPrincipalIterator(@Nullable String queryString, @NotNull Result queryResult) {
            this.queryString = queryString;
            rows = queryResult.getRows().iterator();
        }

        @Override
        protected @Nullable Principal getNext() {
            if (!propValues.hasNext()) {
                if (rows.hasNext()) {
                    ResultRow row = rows.next();
                    propValues = Iterators.filter(row.getValue(REP_EXTERNAL_PRINCIPAL_NAMES).getValue(Type.STRINGS).iterator(), Objects::nonNull);
                    idpName = getIdpName(row);
                } else {
                    propValues = Collections.emptyIterator();
                }
            }
            while (propValues.hasNext()) {
                String principalName = propValues.next();
                if (!processed.contains(principalName) && matchesQuery(principalName) ) {
                    processed.add(principalName);
                    return createExternalGroupPrincipal(principalName, idpName);
                }
            }
            return null;
        }

        private boolean matchesQuery(@NotNull String principalName) {
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
     * @see ExternalGroupPrincipalProvider#getMembers(Group, boolean) 
     */
    private abstract class MemberIterator<T> extends AbstractLazyIterator<T> {

        /**
         * The query results containing the path of the user accounts
         * (i.e. members) that contain the target group principal in the
         * {@link #REP_EXTERNAL_PRINCIPAL_NAMES} property values.
         */
        private final Iterator<? extends ResultRow> rows;

        private MemberIterator(@NotNull Result queryResult) {
            rows = queryResult.getRows().iterator();
        }

        @Override
        protected @Nullable T getNext() {
            while (rows.hasNext()) {
                String userPath = rows.next().getPath();
                try {
                    Authorizable authorizable = userManager.getAuthorizableByPath(userPath);
                    if (authorizable != null) {
                        return get(authorizable);
                    }
                } catch (RepositoryException e) {
                    log.debug("{}", e.getMessage());
                }
            }
            return null;
        }

        abstract T get(@NotNull Authorizable authorizable) throws RepositoryException;
    }

}
