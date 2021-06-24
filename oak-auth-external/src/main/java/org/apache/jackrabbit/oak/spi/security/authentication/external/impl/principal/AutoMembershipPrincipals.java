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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class AutoMembershipPrincipals {

    private static final Logger log = LoggerFactory.getLogger(AutoMembershipPrincipals.class);

    private final UserManager userManager;
    private final Map<String, String[]> autoMembershipMapping;
    private final Map<String, Set<Principal>> principalMap;

    AutoMembershipPrincipals(@NotNull UserManager userManager, @NotNull Map<String, String[]> autoMembershipMapping) {
        this.userManager = userManager;
        this.autoMembershipMapping = autoMembershipMapping;
        this.principalMap = new ConcurrentHashMap<>(autoMembershipMapping.size());
    }

    boolean isConfiguredPrincipal(@NotNull Principal groupPrincipal) {
        initPrincipalMap();
        String name = groupPrincipal.getName();
        for (Set<Principal> principals : principalMap.values()) {
            if (principals.stream().anyMatch(principal -> name.equals(principal.getName()))) {
                return true;
            }
        }
        return false;
    }

    Set<String> getConfiguredIdpNames(@NotNull Principal groupPrincipal) {
        initPrincipalMap();
        String name = groupPrincipal.getName();
        Set<String> idpNames = new HashSet<>(principalMap.size());
        principalMap.forEach((idpName, principals) -> {
            if (principals.stream().anyMatch(principal -> name.equals(principal.getName()))) {
                idpNames.add(idpName);
            }
        });
        return idpNames;
    }

    private void initPrincipalMap() {
        if (principalMap.isEmpty() && !autoMembershipMapping.isEmpty()) {
            for (String idpName : autoMembershipMapping.keySet()) {
                getPrincipals(idpName);
            }
        }
    }
    
    @NotNull
    Collection<Principal> getPrincipals(@Nullable String idpName) {
        if (idpName == null) {
            return ImmutableSet.of();
        }

        Set<Principal> principals;
        if (!principalMap.containsKey(idpName)) {
            principals = collectAutomembershipPrincipals(idpName);
            principalMap.put(idpName, principals);
        } else {
            principals = principalMap.get(idpName);
        }
        return principals;
    }

    @NotNull
    private Set<Principal> collectAutomembershipPrincipals(@NotNull String idpName) {
        String[] vs = autoMembershipMapping.get(idpName);
        if (vs == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<Principal> builder = ImmutableSet.builder();
        for (String groupId : vs) {
            try {
                Authorizable gr = userManager.getAuthorizable(groupId);
                if (gr != null && gr.isGroup()) {
                    Principal grPrincipal = gr.getPrincipal();
                    if (GroupPrincipals.isGroup(grPrincipal)) {
                        builder.add(grPrincipal);
                    } else {
                        log.warn("Principal of group {} is not of group type -> Ignoring", groupId);
                    }
                } else {
                    log.warn("Configured auto-membership group {} does not exist -> Ignoring", groupId);
                }
            } catch (RepositoryException e) {
                log.debug("Failed to retrieved 'auto-membership' group with id {}", groupId, e);
            }
        }
        return builder.build();
    }
}
