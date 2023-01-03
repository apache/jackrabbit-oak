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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.commons.iterator.RangeIteratorAdapter;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.DynamicGroupUtil.getIdpName;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_AUTHORIZABLE;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_GROUP;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.NT_REP_USER;
import static org.apache.jackrabbit.oak.spi.security.user.UserConstants.REP_AUTHORIZABLE_ID;

class AutoMembershipProvider implements DynamicMembershipProvider {

    private static final Logger log = LoggerFactory.getLogger(AutoMembershipProvider.class);

    private static final String BINDING_AUTHORIZABLE_IDS = "authorizableIds";

    private final Root root;
    private final UserManager userManager;
    private final NamePathMapper namePathMapper;
    private final AutoMembershipPrincipals autoMembershipPrincipals;
    private final AutoMembershipPrincipals groupAutoMembershipPrincipals;
    
    AutoMembershipProvider(@NotNull Root root,
                           @NotNull UserManager userManager, @NotNull NamePathMapper namePathMapper,
                           @NotNull Map<String, String[]> autoMembershipMapping,
                           @Nullable Map<String, String[]> groupAutoMembershipMapping,
                           @NotNull Map<String, AutoMembershipConfig> autoMembershipConfigMap) {
        this.root = root;
        this.userManager = userManager;
        this.namePathMapper = namePathMapper;
        this.autoMembershipPrincipals = new AutoMembershipPrincipals(userManager, autoMembershipMapping, autoMembershipConfigMap);
        this.groupAutoMembershipPrincipals = (groupAutoMembershipMapping == null) ? null : new AutoMembershipPrincipals(userManager, groupAutoMembershipMapping, autoMembershipConfigMap);
    }

    AutoMembershipProvider(@NotNull Root root,
                           @NotNull UserManager userManager, @NotNull NamePathMapper namePathMapper,
                           @NotNull SyncConfigTracker scTracker) {
        this(root, userManager, namePathMapper, scTracker.getAutoMembership(), (scTracker.hasDynamicGroupsEnabled() ? scTracker.getGroupAutoMembership() : null), scTracker.getAutoMembershipConfig());
    }
    
    @Override
    public boolean coversAllMembers(@NotNull Group group) {
        return false;
    }

    @Override
    public @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) throws RepositoryException {
        List<Iterator<Authorizable>> results = new ArrayList<>();
        // add members resulting from global automembership mapping
        searchGlobalMembers(group, results);
        // add members from conditional automembership mapping
        results.add(autoMembershipPrincipals.getMembersFromAutoMembershipConfig(group));
        return Iterators.concat(results.iterator());
    }

    @Override
    public boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        String idpName = getIdpName(authorizable);
        if (idpName == null) {
            // not an external identity
            return false;
        }
        
        // the authorizablle to test is a group
        if (authorizable.isGroup()) {
            // not an external user (NOTE: with dynamic membership enabled external groups will only be sync into the 
            // repository if 'dynamic-group' option is enabled in addition)
            if (groupAutoMembershipPrincipals == null) {
                return false;
            } else if (group.getID().equals(authorizable.getID())) {
                // shortcut for the authorizable to test being the group itself
                return false;
            } else {
                return isMember(groupAutoMembershipPrincipals, idpName, group, authorizable, includeInherited);
            } 
        }
        
        // an external user
        return isMember(autoMembershipPrincipals, idpName, group, authorizable, includeInherited);
    }
    
    private static boolean isMember(@NotNull AutoMembershipPrincipals amPrincipals, @NotNull String idpName, 
                                    @NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        if (includeInherited) {
            return amPrincipals.isInheritedMember(idpName, group, authorizable);
        } else {
            return amPrincipals.isMember(idpName, group.getID(), authorizable);
        }
    }

    @Override
    public @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException {
        String idpName = getIdpName(authorizable);
        if (idpName == null) {
            // not an external identity
            return RangeIteratorAdapter.EMPTY;
        }

        Map<Principal, Group> m;
        if (authorizable.isGroup()) {
            // not an external user (NOTE: with dynamic membership enabled external groups will only be sync into the 
            // repository if 'dynamic-group' option is enabled in addition)
            if (groupAutoMembershipPrincipals == null) {
                m = Collections.emptyMap();
            } else {
                m = groupAutoMembershipPrincipals.getAutoMembership(idpName, authorizable, false);
            }
        } else {
            // an external user
            m = autoMembershipPrincipals.getAutoMembership(idpName, authorizable, false);
        }
        
        return getGroupIterator(m.values(), includeInherited);
    }
    
    @NotNull
    private static Iterator<Group> getGroupIterator(@NotNull Collection<Group> groups, boolean includeInherited) {
        if (groups.isEmpty()) {
            return RangeIteratorAdapter.EMPTY;
        }
        Iterator<Group> groupIt = new RangeIteratorAdapter(groups);
        if (!includeInherited) {
            return groupIt;
        } else {
            Set<Group> processed = new HashSet<>();
            return Iterators.filter(new InheritedMembershipIterator(groupIt), processed::add);
        }
    }

    private void searchGlobalMembers(@NotNull Group group, @NotNull List<Iterator<Authorizable>> results) throws RepositoryException {
        Principal p = getPrincipalOrNull(group);
        if (p == null) {
            return;
        }

        // retrieve all idp-names for which the given group-principal is configured in the auto-membership option
        // NOTE: while the configuration takes the group-id the cache in 'autoMembershipPrincipals' is built based on the principal
        Set<String> idpNames = autoMembershipPrincipals.getConfiguredIdpNames(p);
        Set<String> groupIdpNames = Collections.emptySet();
        if (groupAutoMembershipPrincipals != null) {
            groupIdpNames = groupAutoMembershipPrincipals.getConfiguredIdpNames(p);
            idpNames.addAll(groupIdpNames);
        }
        if (idpNames.isEmpty()) {
            return;
        }

        String nodeType = (groupIdpNames.isEmpty()) ? NT_REP_USER : (idpNames.size() == groupIdpNames.size()) ? NT_REP_GROUP : NT_REP_AUTHORIZABLE;

        // since this provider is only enabled for dynamic-automembership the 'includeInherited' flag can be ignored.
        // as group-membership for dynamic users is flattened and automembership-configuration for groups is included.
        // TODO: execute a single (more complex) query ?
        for (String idpName : idpNames) {
            Map<String, ? extends PropertyValue> bindings = buildBinding(idpName);
            String statement = "SELECT '" + REP_AUTHORIZABLE_ID + "' FROM ["+nodeType+"] WHERE PROPERTY(["
                    + REP_EXTERNAL_ID + "], '" + PropertyType.TYPENAME_STRING + "')"
                    + " LIKE $" + BINDING_AUTHORIZABLE_IDS + QueryEngine.INTERNAL_SQL2_QUERY;
            try {
                Result qResult = root.getQueryEngine().executeQuery(statement, Query.JCR_SQL2, bindings, namePathMapper.getSessionLocalMappings());
                Iterator<Authorizable> it = StreamSupport.stream(qResult.getRows().spliterator(), false).map((Function<ResultRow, Authorizable>) resultRow -> {
                    try {
                        return userManager.getAuthorizableByPath(namePathMapper.getJcrPath(resultRow.getPath()));
                    } catch (RepositoryException e) {
                        return null;
                    }
                }).filter(Objects::nonNull).iterator();
                results.add(it);
            } catch (ParseException e) {
                throw new RepositoryException("Failed to retrieve members of auto-membership group "+ group);
            }
        }
    }

    @Nullable
    private static Principal getPrincipalOrNull(@NotNull Group group) {
        try {
            return group.getPrincipal();
        } catch (RepositoryException e) {
            return null;
        }
    }

    @NotNull
    private static Map<String, ? extends PropertyValue> buildBinding(@NotNull String idpName) {
        // idp-name is stored as trailing end after external id followed by ';' => add leading % to the binding
        String val = "%;" + idpName.replace("%", "\\%").replace("_", "\\_");
        return Collections.singletonMap(BINDING_AUTHORIZABLE_IDS, PropertyValues.newString(val));
    }
    
    private static class InheritedMembershipIterator extends AbstractLazyIterator<Group> {

        private final Iterator<Group> groupIterator;
        private final List<Iterator<Group>> inherited = new ArrayList<>();
        private Iterator<Group> inheritedIterator = null;
        
        private InheritedMembershipIterator(Iterator<Group> groupIterator) {
            this.groupIterator = groupIterator;
        }
        
        @Nullable
        @Override
        protected Group getNext() {
            if (groupIterator.hasNext()) {
                Group gr = groupIterator.next();
                try {
                    // call 'memberof' to cover nested inheritance
                    Iterator<Group> it = gr.memberOf();
                    if (it.hasNext()) {
                        inherited.add(it);
                    }
                } catch (RepositoryException e) {
                    log.error("Failed to retrieve membership of group {}", gr, e);
                }
                return gr;
            }
            
            if (inheritedIterator == null || !inheritedIterator.hasNext()) {
                inheritedIterator = getNextInheritedIterator();
            }
            
            if (inheritedIterator.hasNext()) {
                return inheritedIterator.next();
            } else {
                // all inherited groups have been processed
                return null;
            }
        }
        
        @NotNull
        private Iterator<Group> getNextInheritedIterator() {
            if (inherited.isEmpty()) {
                // no more inherited groups to retrieve
                return Collections.emptyIterator();
            } else {
                // no need to verify if the inherited iterator has any elements as this has been asserted before
                // adding it to the list.
                return inherited.remove(0);
            }
        }
    }
}