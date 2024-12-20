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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class AutoMembershipPrincipals {

    private static final Logger log = LoggerFactory.getLogger(AutoMembershipPrincipals.class);

    private final UserManager userManager;
    private final Map<String, String[]> autoMembershipMapping;
    private final Map<String, AutoMembershipConfig> autoMembershipConfigMap;
    private final Map<String, Set<Principal>> principalMap;

    AutoMembershipPrincipals(@NotNull UserManager userManager,
                             @NotNull Map<String, String[]> autoMembershipMapping,
                             @NotNull Map<String, AutoMembershipConfig> autoMembershipConfigMap) {
        this.userManager = userManager;
        this.autoMembershipMapping = autoMembershipMapping;
        this.autoMembershipConfigMap = autoMembershipConfigMap;
        this.principalMap = new ConcurrentHashMap<>(autoMembershipMapping.size());
    }
    
    /**
     * Return the IDP names for which the given group principal is configured in the global auto-membership configuration.
     * 
     * @param groupPrincipal A group principal
     * @return A set of IDP names.
     * @see AutoMembershipProvider#getMembers(Group, boolean) 
     */
    Set<String> getConfiguredIdpNames(@NotNull Principal groupPrincipal) {
        // populate principal-map for all IDP-names in the mapping
        if (principalMap.isEmpty() && !autoMembershipMapping.isEmpty()) {
            for (String idpName : autoMembershipMapping.keySet()) {
                collectGlobalAutoMembershipPrincipals(idpName);
            }
        }
        
        String name = groupPrincipal.getName();
        Set<String> idpNames = new HashSet<>(principalMap.size());
        principalMap.forEach((idpName, principals) -> {
            if (principals.stream().anyMatch(principal -> name.equals(principal.getName()))) {
                idpNames.add(idpName);
            }
        });
        return idpNames;
    }

    /**
     * Returns the automatic members of the given group across all configured IDPs according to the result of  
     * {@link AutoMembershipConfig#getAutoMembers(UserManager, Group)}
     * 
     * @param group The target group
     * @return An iterator of users/groups that have the given group as auto-membership group defined in {@link AutoMembershipConfig#getAutoMembership(Authorizable)}
     * @see AutoMembershipProvider#getMembers(Group, boolean) 
     */
    @NotNull
    Iterator<Authorizable> getMembersFromAutoMembershipConfig(@NotNull Group group) {
        List<Iterator<? extends Authorizable>> results = new ArrayList<>();
        autoMembershipConfigMap.values().forEach(autoMembershipConfig -> results.add(autoMembershipConfig.getAutoMembers(userManager, group)));
        return Iterators.concat(results.iterator());
    }

    /**
     * Tests if the given authorizabe is an automatic member of the group identified by {@code groupId}. Note that this 
     * method evaluates both global auto-membership mapping and {@link AutoMembershipConfig} if they exist for the given IDP name.
     * 
     * @param idpName The name of an IDP
     * @param groupId The target group id
     * @param authorizable The authorizable for which to evaluation if it is an automatic member of the group identified by {@code groupId}.
     * @return {@code true} if the given authorizable is an automatic member of the group identified by {@code groupId}; {@code false} otherwise.
     * @see AutoMembershipProvider#isMember(Group, Authorizable, boolean) 
     */
    boolean isMember(@NotNull String idpName, @NotNull String groupId, @NotNull Authorizable authorizable) {
        // check global auto-membership mapping first  
        String[] vs = autoMembershipMapping.get(idpName);
        if (vs != null) {
            for (String grId : vs) {
                if (groupId.equals(grId)) {
                    return true;
                }
            }
        }

        // if not defined in global mapping test if any match is found in the auto-membership configurations
        AutoMembershipConfig config = autoMembershipConfigMap.get(idpName);
        if (config != null) {
            return config.getAutoMembership(authorizable).contains(groupId);
        }
        return false;
    }

    boolean isInheritedMember(@NotNull String idpName, @NotNull Group group, @NotNull Authorizable authorizable) throws RepositoryException {
        String groupId = group.getID();
        if (isMember(idpName, groupId, authorizable)) {
            // groupId is listed in auto-membership configuration
            return true;
        }

        // to test for inherited membership collect automembership-ids and loop auto-membership groups
        Set<String> automembershipIds = new HashSet<>(Arrays.asList(autoMembershipMapping.get(idpName)));
        AutoMembershipConfig config = autoMembershipConfigMap.get(idpName);
        if (config != null) {
            automembershipIds.addAll(config.getAutoMembership(authorizable));
        }

        // keep track of processed ids over all 'automembership' ids to avoid repeated evaluation 
        Set<String> processed = new HashSet<>();
        for (String automembershipId : automembershipIds) {
            Authorizable gr = userManager.getAuthorizable(automembershipId);
            if (gr == null || !gr.isGroup()) {
                log.warn("Configured automembership id '{}' is not a valid group", automembershipId);
            } else if (hasInheritedMembership(gr.declaredMemberOf(), groupId, automembershipId, processed, "> ")) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasInheritedMembership(@NotNull Iterator<Group> declared, @NotNull String groupId, 
                                                  @NotNull String memberId, @NotNull Set<String> processed, 
                                                  @NotNull String trace) throws RepositoryException {
        List<Group> groups = new ArrayList<>();
        while (declared.hasNext()) {
            Group gr = declared.next();
            String grId = gr.getID();
            if (groupId.equals(grId)) {
                // the specified groupId defines inherited membership of a configured auto-membership group
                return true;
            }
            if (memberId.equals(grId)) {
                log.error("Cyclic group membership detected for group id {} via {}{}", memberId, trace, grId);
                continue;
            }
            if (processed.add(grId)) {
                // remember group for traversing up the membership hierarchy if it has not already been 
                // processed before (shared membership e.g. for the 'everyone' group)
                groups.add(gr);
            }
        }
        // recursively call to search for inherited membership
        for (Group group : groups) {
            if (hasInheritedMembership(group.declaredMemberOf(), groupId, memberId, processed, String.format("%s %s > ", trace, group.getID()))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the group principal that given authorizable is an automatic member of. This method evaluates both the 
     * global auto-membership settings and {@link AutoMembershipConfig} if they exist for the given IDP name.
     * 
     * @param idpName The name of an IDP
     * @param authorizable The target user/group
     * @param includeInherited Flag indicating if inherited groups should be resolved for global automemberbship groups.                    
     * @return A collection of principals the given authorizable is an automatic member of.
     */
    @NotNull
    Map<Principal,Group> getAutoMembership(@NotNull String idpName, @NotNull Authorizable authorizable, boolean includeInherited) {
        // global auto-membership
        Map<Principal,Group> map = collectGlobalAutoMembershipPrincipals(idpName);
        if (includeInherited) {
            for (Group gr : map.values().toArray(new Group[0])) {
                collectInheritedPrincipals(gr, map);
            }
        }
        // conditional auto-membership
        AutoMembershipConfig config = autoMembershipConfigMap.get(idpName);
        if (config != null) {
            config.getAutoMembership(authorizable).forEach(groupId -> addVerifiedPrincipal(groupId, map, includeInherited));
        }
        return map;
    }

    private Map<Principal, Group> collectGlobalAutoMembershipPrincipals(@NotNull String idpName) {
        Map<Principal, Group> map = new HashMap<>();
        if (!principalMap.containsKey(idpName)) {
            String[] vs = autoMembershipMapping.get(idpName);
            if (vs != null) {
                for (String groupId : vs) {
                    addVerifiedPrincipal(groupId, map, false);
                }
            }
            // only cache the principal instance but not the group (tree might become disconnected)
            principalMap.put(idpName, Set.copyOf(map.keySet()));
        } else {
            // resolve Group objects from cached principals
            principalMap.get(idpName).forEach(groupPrincipal -> {
                Group gr = retrieveGroup(groupPrincipal);
                if (gr != null) {
                    map.put(groupPrincipal, gr);
                }

            });
        }
        return map;
    }

    private static void collectInheritedPrincipals(@NotNull Group group,
                                                   @NotNull Map<Principal, Group> map) {
        try {
            Iterator<Group> groups = group.memberOf();
            while (groups.hasNext()) {
                Group gr = groups.next();
                Principal p = getVerifiedPrincipal(gr);
                if (p != null) {
                    map.put(p, gr);
                }
            }
        } catch (RepositoryException e) {
            log.warn("Error while resolving inherited auto-membership", e);
        }
    }

    private void addVerifiedPrincipal(@NotNull String groupId, @NotNull Map<Principal,Group> builder,
                                      boolean includeInherited) {
        try {
            Authorizable a = userManager.getAuthorizable(groupId);
            if (a == null || !a.isGroup()) {
                log.warn("Configured auto-membership group {} does not exist -> Ignoring", groupId);
                return;
            }
            
            Group group = (Group) a;
            Principal principal = getVerifiedPrincipal(group);
            if (principal != null) {
                builder.put(principal, group);
                if (includeInherited) {
                    collectInheritedPrincipals(group, builder);
                }
            }
        } catch (RepositoryException e) {
            log.debug("Failed to retrieved 'auto-membership' group with id {}", groupId, e);
        }
    }

    @Nullable
    private static Principal getVerifiedPrincipal(@NotNull Group group) throws RepositoryException {
        Principal grPrincipal = group.getPrincipal();
        if (GroupPrincipals.isGroup(grPrincipal)) {
            return grPrincipal;
        } else {
            log.warn("Principal of group {} is not of group type -> Ignoring", group.getID());
            return null;
        }
    }

    @Nullable
    private Group retrieveGroup(@NotNull Principal principal) {
        try {
            Authorizable gr = userManager.getAuthorizable(principal);
            if (gr != null && gr.isGroup()) {
                return (Group) gr;
            } else {
                log.warn("Cannot retrieve group from principal {} -> Ignoring", principal);
            }
        } catch (RepositoryException e) {
            log.debug("Failed to retrieved 'auto-membership' group for principal {}", principal.getName(), e);
        }
        return null;
    }
}
