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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;

/**
 * ACE... TODO
 */
public class ACE implements JackrabbitAccessControlEntry {

    private final Principal principal;
    private final Set<Privilege> privileges;
    private final boolean isAllow;
    private final Set<Restriction> restrictions;
    private final NamePathMapper namePathMapper;

    private int hashCode;

    public ACE(Principal principal, Privilege[] privileges, boolean isAllow,
               Set<Restriction> restrictions, NamePathMapper namePathMapper) {
        this(principal, ImmutableSet.copyOf(privileges), isAllow, restrictions, namePathMapper);
    }

    public ACE(Principal principal, Set<Privilege> privileges, boolean isAllow,
               Set<Restriction> restrictions, NamePathMapper namePathMapper) {
        this.principal = principal;
        this.privileges = ImmutableSet.copyOf(privileges);
        this.isAllow = isAllow;
        this.restrictions = (restrictions == null) ? Collections.<Restriction>emptySet() : ImmutableSet.copyOf(restrictions);
        this.namePathMapper = namePathMapper;
    }

    public String[] getPrivilegeNames() {
        Collection<String> privNames = Collections2.transform(privileges, new Function<Privilege, String>() {
            @Override
            public String apply(Privilege privilege) {
                return privilege.getName();
            }
        });
        return privNames.toArray(new String[privNames.size()]);
    }

    public Set<Restriction> getRestrictionSet() {
        return restrictions;
    }

    //-------------------------------------------------< AccessControlEntry >---
    @Override
    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public Privilege[] getPrivileges() {
        return privileges.toArray(new Privilege[privileges.size()]);
    }

    //---------------------------------------< JackrabbitAccessControlEntry >---
    @Override
    public boolean isAllow() {
        return isAllow;
    }

    @Override
    public String[] getRestrictionNames() throws RepositoryException {
        return Collections2.transform(restrictions, new Function<Restriction, String>() {
            @Override
            public String apply(Restriction restriction) {
                return namePathMapper.getJcrName(restriction.getName());
            }
        }).toArray(new String[restrictions.size()]);
    }

    @Override
    public Value getRestriction(String restrictionName) throws RepositoryException {
        String oakName = namePathMapper.getOakName(restrictionName);
        for (Restriction restriction : restrictions) {
            if (restriction.getName().equals(oakName)) {
                return ValueFactoryImpl.createValue(restriction.getProperty(), namePathMapper);
            }
        }
        return null;
    }

    //-------------------------------------------------------------< Object >---
    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (hashCode == -1) {
            hashCode = buildHashCode();
        }
        return hashCode;
    }

    /**
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ACE) {
            ACE other = (ACE) obj;
            return principal.equals(other.principal) &&
                   privileges.equals(other.privileges) &&
                   isAllow == other.isAllow &&
                   restrictions.equals(other.restrictions);
        }
        return false;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(principal.getName()).append('-').append(isAllow).append('-');
        sb.append(privileges.toString()).append('-').append(restrictions.toString());
        return sb.toString();
    }

    //------------------------------------------------------------< private >---
    /**
     * Build the hash code.
     *
     * @return the hash code.
     */
    private int buildHashCode() {
        int h = 17;
        h = 37 * h + principal.hashCode();
        h = 37 * h + privileges.hashCode();
        h = 37 * h + Boolean.valueOf(isAllow).hashCode();
        h = 37 * h + restrictions.hashCode();
        return h;
    }
}
