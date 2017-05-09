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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlException;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;

/**
 * Default implementation of the {@code JackrabbitAccessControlEntry} interface.
 * It asserts that the basic contract is fulfilled but does perform any additional
 * validation on the principal, the privileges or the specified restrictions.
 */
public abstract class ACE implements JackrabbitAccessControlEntry {

    private final Principal principal;
    private final PrivilegeBits privilegeBits;
    private final boolean isAllow;
    private final Set<Restriction> restrictions;
    private final NamePathMapper namePathMapper;

    private int hashCode;

    public ACE(Principal principal, PrivilegeBits privilegeBits,
               boolean isAllow, Set<Restriction> restrictions, NamePathMapper namePathMapper) throws AccessControlException {
        if (principal == null || privilegeBits == null || privilegeBits.isEmpty()) {
            throw new AccessControlException();
        }

        this.principal = principal;
        this.privilegeBits = privilegeBits;
        this.isAllow = isAllow;
        this.restrictions = (restrictions == null) ? Collections.<Restriction>emptySet() : ImmutableSet.copyOf(restrictions);
        this.namePathMapper = namePathMapper;
    }

    //--------------------------------------------------------------------------
    @Nonnull
    public PrivilegeBits getPrivilegeBits() {
        return privilegeBits;
    }

    @Nonnull
    public Set<Restriction> getRestrictions() {
        return restrictions;
    }

    //-------------------------------------------------< AccessControlEntry >---
    @Nonnull
    @Override
    public Principal getPrincipal() {
        return principal;
    }

    //---------------------------------------< JackrabbitAccessControlEntry >---
    @Override
    public boolean isAllow() {
        return isAllow;
    }

    @Nonnull
    @Override
    public String[] getRestrictionNames() throws RepositoryException {
        return Collections2.transform(restrictions, new Function<Restriction, String>() {
            @Override
            public String apply(Restriction restriction) {
                return getJcrName(restriction);
            }
        }).toArray(new String[restrictions.size()]);
    }

    @CheckForNull
    @Override
    public Value getRestriction(String restrictionName) throws RepositoryException {
        for (Restriction restriction : restrictions) {
            String jcrName = getJcrName(restriction);
            if (jcrName.equals(restrictionName)) {
                if (restriction.getDefinition().getRequiredType().isArray()) {
                    List<Value> values = ValueFactoryImpl.createValues(restriction.getProperty(), namePathMapper);
                    switch (values.size()) {
                        case 1: return values.get(0);
                        default : throw new ValueFormatException("Attempt to retrieve single value from multivalued property");
                    }
                } else {
                    return ValueFactoryImpl.createValue(restriction.getProperty(), namePathMapper);
                }
            }
        }
        return null;
    }

    @CheckForNull
    @Override
    public Value[] getRestrictions(String restrictionName) throws RepositoryException {
        for (Restriction restriction : restrictions) {
            String jcrName = getJcrName(restriction);
            if (jcrName.equals(restrictionName)) {
                List<Value> values = ValueFactoryImpl.createValues(restriction.getProperty(), namePathMapper);
                return values.toArray(new Value[values.size()]);
            }
        }
        return null;
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hashCode(principal.getName(), privilegeBits, isAllow, restrictions);
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ACE) {
            ACE other = (ACE) obj;
            return principal.getName().equals(other.principal.getName())
                    && isAllow == other.isAllow
                    && privilegeBits.equals(other.privilegeBits)
                    && restrictions.equals(other.restrictions);
        }
        return false;
    }

    //------------------------------------------------------------< private >---
    private String getJcrName(Restriction restriction) {
        return namePathMapper.getJcrName(restriction.getDefinition().getName());
    }
}
