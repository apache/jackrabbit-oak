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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the {@link org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy}
 * interface that respects the configured {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}.
 */
class CugPolicyImpl implements CugPolicy {

    private static final Logger log = LoggerFactory.getLogger(CugPolicyImpl.class);
    private static final String IMMUTABLE_ERR = "Immutable CUG. Use getApplicablePolicies or getPolicies in order to obtain a modifiable policy.";

    private final String oakPath;
    private final NamePathMapper namePathMapper;
    private final PrincipalManager principalManager;
    private final int importBehavior;
    private final CugExclude cugExclude;
    private final boolean immutable;

    private final Map<String,Principal> principals = new LinkedHashMap<>();

    CugPolicyImpl(@NotNull String oakPath, @NotNull NamePathMapper namePathMapper,
                  @NotNull PrincipalManager principalManager, int importBehavior, @NotNull CugExclude cugExclude) {
        this(oakPath, namePathMapper, principalManager, importBehavior, cugExclude, Collections.emptySet(), false);
    }

    CugPolicyImpl(@NotNull String oakPath, @NotNull NamePathMapper namePathMapper,
                  @NotNull PrincipalManager principalManager, int importBehavior,
                  @NotNull CugExclude cugExclude, @NotNull Iterable<Principal> principals, boolean immutable) {
        ImportBehavior.nameFromValue(importBehavior);
        this.oakPath = oakPath;
        this.namePathMapper = namePathMapper;
        this.principalManager = principalManager;
        this.importBehavior = importBehavior;
        this.cugExclude = cugExclude;
        for (Principal principal : principals) {
            this.principals.put(principal.getName(), principal);
        }
        this.immutable = immutable;
    }

    private void checkIsMutable() throws AccessControlException {
        if (immutable) {
            throw new AccessControlException(IMMUTABLE_ERR);
        }
    }

    @NotNull
    @Override
    public Set<Principal> getPrincipals() {
        return Sets.newHashSet(principals.values());
    }

    @Override
    public boolean addPrincipals(@NotNull Principal... principals) throws AccessControlException {
        checkIsMutable();
        boolean modified = false;
        for (Principal principal : principals) {
            if (isValidPrincipal(principal) && !this.principals.containsKey(principal.getName())) {
                this.principals.put(principal.getName(), principal);
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removePrincipals(@NotNull Principal... principals) throws AccessControlException {
        checkIsMutable();
        boolean modified = false;
        for (Principal principal : principals) {
            if (principal != null && this.principals.containsKey(principal.getName())) {
                this.principals.remove(principal.getName());
                modified = true;
            }
        }
        return modified;
    }

    //----------------------------------------< JackrabbitAccessControlList >---
    @Override
    public String getPath() {
        return namePathMapper.getJcrPath(oakPath);
    }

    //--------------------------------------------------------------------------
    Iterable<String> getPrincipalNames() {
        return Sets.newHashSet(principals.keySet());
    }

    //--------------------------------------------------------------------------

    /**
     * Validate the specified {@code principal} taking the configured
     * {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior} into account.
     *
     *
     * @param principal The principal to validate.
     * @return if the principal is considered valid and can be added to the list.
     * @throws AccessControlException If the principal has an invalid name or
     * if {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#ABORT} is
     * configured and this principal is not known to the repository.
     */
    private boolean isValidPrincipal(@Nullable Principal principal) throws AccessControlException {
        if (principal == null) {
            log.debug("Ignoring null principal.");
            return false;
        }

        String name = principal.getName();
        if (Strings.isNullOrEmpty(name)) {
            throw new AccessControlException("Invalid principal " + name);
        }

        if (cugExclude.isExcluded(Collections.singleton(principal))) {
            log.warn("Attempt to add excluded principal {} to CUG.", principal);
            return false;
        }

        boolean isValid = true;
        switch (importBehavior) {
            case ImportBehavior.IGNORE:
                if (!principalManager.hasPrincipal(name)) {
                    log.debug("Ignoring unknown principal {}", name);
                    isValid = false;
                }
                break;
            case ImportBehavior.BESTEFFORT:
                log.debug("Best effort: don't verify existence of principals.");
                break;
            default: //ImportBehavior.ABORT
                if (!principalManager.hasPrincipal(name)) {
                    throw new AccessControlException("Unknown principal " + name);
                }
        }
        return isValid;
    }
}
