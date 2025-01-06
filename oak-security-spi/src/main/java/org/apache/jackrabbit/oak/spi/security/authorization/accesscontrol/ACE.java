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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Default implementation of the {@code JackrabbitAccessControlEntry} interface.
 * It asserts that the basic contract is fulfilled but does perform any additional
 * validation on the principal, the privileges or the specified restrictions.
 */
@ProviderType
public abstract class ACE implements JackrabbitAccessControlEntry {

    private final Principal principal;
    private final PrivilegeBits privilegeBits;
    private final boolean isAllow;
    private final Set<Restriction> restrictions;
    private final NamePathMapper namePathMapper;
    private final PartialValueFactory valueFactory;

    private int hashCode;

    /**
     * Creates a new access control entry.
     * 
     * @param principal The principal associated with this entry.
     * @param privilegeBits The privilege bits defined for this entry.
     * @param isAllow {@code true} if the entry is granting privileges.
     * @param restrictions A optional set of restrictions.
     * @param namePathMapper The name-path mapper
     * @throws AccessControlException If the given {@code principal} or {@code privilegeBits} are {@code null} or if {@code privilegeBits} are {@link PrivilegeBits#isEmpty() empty}.
     */
    public ACE(@Nullable Principal principal, @Nullable PrivilegeBits privilegeBits,
               boolean isAllow, @Nullable Set<Restriction> restrictions, 
               @NotNull NamePathMapper namePathMapper) throws AccessControlException {
        if (principal == null || privilegeBits == null || privilegeBits.isEmpty()) {
            throw new AccessControlException();
        }

        this.principal = principal;
        this.privilegeBits = privilegeBits;
        this.isAllow = isAllow;
        this.restrictions = (restrictions == null) ? Collections.emptySet() : Collections.unmodifiableSet(restrictions);
        this.namePathMapper = namePathMapper;
        this.valueFactory = new PartialValueFactory(namePathMapper);
    }

    //--------------------------------------------------------------------------
    @NotNull
    public PrivilegeBits getPrivilegeBits() {
        return privilegeBits;
    }

    @NotNull
    public Set<Restriction> getRestrictions() {
        return restrictions;
    }
    
    @NotNull
    protected abstract PrivilegeBitsProvider getPrivilegeBitsProvider();

    //-------------------------------------------------< AccessControlEntry >---
    @NotNull
    @Override
    public Principal getPrincipal() {
        return principal;
    }

    //---------------------------------------< JackrabbitAccessControlEntry >---
    @Override
    public boolean isAllow() {
        return isAllow;
    }

    @NotNull
    @Override
    public String[] getRestrictionNames() {
        return restrictions.stream().map(this::getJcrName).toArray(String[]::new);
    }

    @Nullable
    @Override
    public Value getRestriction(@NotNull String restrictionName) throws RepositoryException {
        for (Restriction restriction : restrictions) {
            String jcrName = getJcrName(restriction);
            if (jcrName.equals(restrictionName)) {
                if (restriction.getDefinition().getRequiredType().isArray()) {
                    List<Value> values = valueFactory.createValues(restriction.getProperty());
                    if (values.size() == 1) {
                        return values.get(0);
                    } else {
                        throw new ValueFormatException("Attempt to retrieve single value from multivalued property");
                    }
                } else {
                    return valueFactory.createValue(restriction.getProperty());
                }
            }
        }
        return null;
    }

    @Nullable
    @Override
    public Value[] getRestrictions(@NotNull String restrictionName) {
        for (Restriction restriction : restrictions) {
            String jcrName = getJcrName(restriction);
            if (jcrName.equals(restrictionName)) {
                List<Value> values = valueFactory.createValues(restriction.getProperty());
                return values.toArray(new Value[0]);
            }
        }
        return null;
    }

    @Override
    public @NotNull PrivilegeCollection getPrivilegeCollection() {
        return new AbstractPrivilegeCollection(privilegeBits) {
            @Override
            public Privilege[] getPrivileges() {
                return ACE.this.getPrivileges();
            }

            @Override
            @NotNull PrivilegeBitsProvider getPrivilegeBitsProvider() {
                return ACE.this.getPrivilegeBitsProvider();
            }

            @Override
            @NotNull NamePathMapper getNamePathMapper() {
                return namePathMapper;
            }
        };
    }

    //-------------------------------------------------------------< Object >---

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hash(principal.getName(), privilegeBits, isAllow, restrictions);
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
