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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Objects;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.List;
import java.util.Map;

class ImmutablePrincipalPolicy extends ImmutableACL implements PrincipalAccessControlList {
    
    private final Principal principal;

    private int hashCode;

    public ImmutablePrincipalPolicy(@NotNull Principal principal, @NotNull String oakPath, @NotNull List<? extends PrincipalAccessControlList.Entry> entries, @NotNull RestrictionProvider restrictionProvider, @NotNull NamePathMapper namePathMapper) {
        super(oakPath, entries, restrictionProvider, namePathMapper);
        this.principal = principal;
    }

    public ImmutablePrincipalPolicy(@NotNull PrincipalPolicyImpl accessControlList) {
        super(accessControlList);
        this.principal = accessControlList.getPrincipal();
    }

    //-----------------------------------------< PrincipalAccessControlList >---
    @Override
    public @NotNull Principal getPrincipal() {
        return principal;
    }

    @Override
    public boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges) throws RepositoryException {
        throw new AccessControlException("Immutable PrincipalAccessControlList.");
    }

    @Override
    public boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges, @NotNull Map<String, Value> restrictions, @NotNull Map<String, Value[]> mvRestrictions) throws RepositoryException {
        throw new AccessControlException("Immutable PrincipalAccessControlList.");
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hashCode(principal, getOakPath(), getEntries());
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ImmutablePrincipalPolicy) {
            ImmutablePrincipalPolicy other = (ImmutablePrincipalPolicy) obj;
            return Objects.equal(getOakPath(), other.getOakPath())
                    && principal.equals(other.principal)
                    && getEntries().equals(other.getEntries());
        }
        return false;
    }
}