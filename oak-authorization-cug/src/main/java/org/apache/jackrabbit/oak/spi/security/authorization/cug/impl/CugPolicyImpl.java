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

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlException;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the {@link org.apache.jackrabbit.oak.spi.security.authorization.cug.CugPolicy}
 * interface that respects the configured {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}.
 */
class CugPolicyImpl implements CugPolicy {

    private static final Logger log = LoggerFactory.getLogger(CugPolicyImpl.class);

    private final String oakPath;
    private final NamePathMapper namePathMapper;
    private final PrincipalManager principalManager;
    private final int importBehavior;

    private final Set<Principal> principals = new HashSet<>();

    CugPolicyImpl(@Nonnull String oakPath, @Nonnull NamePathMapper namePathMapper,
                  @Nonnull PrincipalManager principalManager, int importBehavior) {
        this(oakPath, namePathMapper, principalManager, importBehavior, Collections.<Principal>emptySet());
    }

    CugPolicyImpl(@Nonnull String oakPath, @Nonnull NamePathMapper namePathMapper,
                  @Nonnull PrincipalManager principalManager, int importBehavior,
                  @Nonnull Set<Principal> principals) {
        ImportBehavior.nameFromValue(importBehavior);
        this.oakPath = oakPath;
        this.namePathMapper = namePathMapper;
        this.principalManager = principalManager;
        this.importBehavior = importBehavior;
        this.principals.addAll(principals);
    }

    @Nonnull
    @Override
    public Set<Principal> getPrincipals() {
        return Sets.newHashSet(principals);
    }

    @Override
    public boolean addPrincipals(@Nonnull Principal... principals) throws AccessControlException {
        boolean modified = false;
        for (Principal principal : principals) {
            if (isValidPrincipal(principal)) {
                modified |= this.principals.add(principal);
            }
        }
        return modified;
    }

    @Override
    public boolean removePrincipals(@Nonnull Principal... principals) {
        boolean modified = false;
        for (Principal principal : principals) {
            if (principal != null) {
                modified |= this.principals.remove(principal);
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
        return Iterables.transform(principals, Principal::getName);
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
    private boolean isValidPrincipal(@CheckForNull Principal principal) throws AccessControlException {
        if (principal == null) {
            log.debug("Ignoring null principal.");
            return false;
        }

        String name = principal.getName();
        if (Strings.isNullOrEmpty(name)) {
            throw new AccessControlException("Invalid principal " + name);
        }

        boolean isValid = true;
        switch (importBehavior) {
            case ImportBehavior.ABORT:
                if (!principalManager.hasPrincipal(name)) {
                    throw new AccessControlException("Unknown principal " + name);
                }
                break;
            case ImportBehavior.IGNORE:
                if (!principalManager.hasPrincipal(name)) {
                    log.debug("Ignoring unknown principal " + name);
                    isValid = false;
                }
                break;
            case ImportBehavior.BESTEFFORT:
                log.debug("Best effort: don't verify existence of principals.");
                break;
            default:
                throw new IllegalStateException("Unsupported import behavior " + importBehavior);
        }
        return isValid;
    }
}