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
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.security.AccessControlException;
import java.security.Principal;
import java.util.Set;

abstract class AbstractEntry extends ACE implements PrincipalAccessControlList.Entry {

    private final String oakPath;

    private int hashCode;

    AbstractEntry(@Nullable String oakPath, @NotNull Principal principal, @NotNull PrivilegeBits privilegeBits, @NotNull Set<Restriction> restrictions, @NotNull NamePathMapper namePathMapper) throws AccessControlException {
        super(principal, privilegeBits, true, restrictions, namePathMapper);
        this.oakPath = oakPath;
    }

    @Nullable
    String getOakPath() {
        return oakPath;
    }

    @NotNull
    abstract NamePathMapper getNamePathMapper();

    @Override
    @Nullable
    public String getEffectivePath() {
        return (oakPath == null) ? null : getNamePathMapper().getJcrPath(oakPath);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hashCode(oakPath, getPrincipal().getName(), getPrivilegeBits(), Boolean.TRUE, getRestrictions());
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof AbstractEntry) {
            AbstractEntry other = (AbstractEntry) obj;
            return equivalentPath(other.oakPath) && super.equals(obj);
        }
        return false;
    }

    private boolean equivalentPath(@Nullable String otherPath) {
        return (oakPath == null) ? otherPath == null : oakPath.equals(otherPath);
    }
}