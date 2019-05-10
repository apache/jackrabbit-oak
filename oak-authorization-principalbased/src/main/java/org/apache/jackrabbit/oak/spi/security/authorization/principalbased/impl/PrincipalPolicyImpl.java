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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.security.authorization.PrincipalAccessControlList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;

class PrincipalPolicyImpl extends AbstractAccessControlList implements PrincipalAccessControlList {

    private static final Logger log = LoggerFactory.getLogger(PrincipalPolicyImpl.class);

    private final List<EntryImpl> entries = new ArrayList<>();
    private final Principal principal;

    private final RestrictionProvider restrictionProvider;
    private final PrivilegeManager privilegeManager;
    private final PrivilegeBitsProvider privilegeBitsProvider;

    PrincipalPolicyImpl(@NotNull Principal principal, @NotNull String oakPath, @NotNull MgrProvider mgrProvider) {
        super(oakPath, mgrProvider.getNamePathMapper());
        this.principal = principal;
        this.restrictionProvider = mgrProvider.getRestrictionProvider();
        this.privilegeManager = mgrProvider.getPrivilegeManager();
        this.privilegeBitsProvider = mgrProvider.getPrivilegeBitsProvider();
    }

    boolean addEntry(@NotNull Tree entryTree) throws AccessControlException {
        String oakPath = Strings.emptyToNull(TreeUtil.getString(entryTree, REP_EFFECTIVE_PATH));
        PrivilegeBits bits = privilegeBitsProvider.getBits(entryTree.getProperty(Constants.REP_PRIVILEGES).getValue(Type.NAMES));
        Set<Restriction> restrictions = restrictionProvider.readRestrictions(oakPath, entryTree);
        return addEntry(new EntryImpl(oakPath, bits, restrictions));
    }

    //------------------------------------------< AbstractAccessControlList >---

    @Override
    @NotNull
    public List<EntryImpl> getEntries() {
        return entries;
    }

    @Override
    @NotNull
    public RestrictionProvider getRestrictionProvider() {
        return restrictionProvider;
    }

    //-----------------------------------------< PrincipalAccessControlList >---

    @Override
    @NotNull
    public Principal getPrincipal() {
        return principal;
    }

    @Override
    public boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges) throws RepositoryException {
        return addEntry(effectivePath, privileges, Collections.emptyMap(), Collections.emptyMap());
    }

    @Override
    public boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges, @NotNull Map<String, Value> restrictions, @NotNull Map<String, Value[]> mvRestrictions) throws RepositoryException {
        String oakPath = (effectivePath == null) ? null : getNamePathMapper().getOakPath(effectivePath);
        Set<Restriction> rs = validateRestrictions(oakPath, restrictions, mvRestrictions);
        PrivilegeBits privilegeBits = validatePrivileges(privileges);

        return addEntry(new EntryImpl(oakPath, privilegeBits, rs));
    }


    //----------------------------------------< JackrabbitAccessControlList >---
    @Override
    public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow, @Nullable Map<String, Value> restrictions, @Nullable Map<String, Value[]> mvRestrictions) throws RepositoryException {
        if (!this.principal.equals(principal)) {
            throw new AccessControlException("Principal must be the one associated with the principal based policy.");
        }
        if (!isAllow) {
            throw new AccessControlException("Principal based access control does not support DENY access control entries.");
        }

        String jcrNodePathName = getNamePathMapper().getJcrName(AccessControlConstants.REP_NODE_PATH);
        String path = extractPathFromRestrictions(restrictions, jcrNodePathName);
        Map<String, Value> filteredRestrictions = Maps.filterEntries(restrictions, entry -> !jcrNodePathName.equals(entry.getKey()));

        return addEntry(path, privileges, filteredRestrictions, (mvRestrictions == null) ? Collections.emptyMap() : mvRestrictions);
    }

    @Override
    public void orderBefore(@NotNull AccessControlEntry srcEntry, @Nullable AccessControlEntry destEntry) throws RepositoryException {
        EntryImpl src = validateEntry(srcEntry);
        EntryImpl dest = (destEntry == null) ? null : validateEntry(destEntry);

        if (src.equals(dest)) {
            log.debug("'srcEntry' equals 'destEntry' -> no reordering.");
            return;
        }

        int index = -1;
        if (dest != null) {
            index = entries.indexOf(dest);
            if (index < 0) {
                throw new AccessControlException("Destination entry not contained in this AccessControlList.");
            }
        }

        if (entries.remove(src)) {
            if (index != -1) {
                entries.add(index, src);
            } else {
                entries.add(src);
            }
        } else {
            throw new AccessControlException("Source entry not contained in this AccessControlList");
        }
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) throws RepositoryException {
        validateEntry(ace);

        if (!entries.remove(ace)) {
            throw new AccessControlException("AccessControlEntry " +ace+ " not contained in AccessControlList");
        }
    }

    //--------------------------------------------------------------------------

    @NotNull
    private String getOakName(@NotNull String jcrName) throws RepositoryException {
        return getNamePathMapper().getOakName(jcrName);
    }

    @NotNull
    private Set<Restriction> validateRestrictions(@Nullable String effectiveOakPath, @NotNull Map<String, Value> restrictions, @NotNull  Map<String, Value[]> mvRestrictions) throws RepositoryException {
        for (RestrictionDefinition def : getRestrictionProvider().getSupportedRestrictions(getOakPath())) {
            String jcrName = getNamePathMapper().getJcrName(def.getName());
            if (def.isMandatory()) {
                boolean containsMandatory = (def.getRequiredType().isArray()) ?
                        mvRestrictions.containsKey(jcrName) :
                        restrictions.containsKey(jcrName);
                if (!containsMandatory) {
                    throw new AccessControlException("Mandatory restriction " + jcrName + " is missing.");
                }
            }
        }
        return computeRestrictions(effectiveOakPath, restrictions, mvRestrictions);
    }

    @NotNull
    private Set<Restriction> computeRestrictions(@Nullable String effectiveOakPath, @NotNull Map<String, Value> restrictions, @NotNull Map<String, Value[]> mvRestrictions) throws RepositoryException {
        Set<Restriction> rs;
        if (restrictions.isEmpty() && mvRestrictions.isEmpty()) {
            rs = Collections.emptySet();
        } else {
            RestrictionProvider rp = getRestrictionProvider();
            rs = new HashSet<>();
            for (Map.Entry<String, Value> entry : restrictions.entrySet()) {
                rs.add(rp.createRestriction(effectiveOakPath, getOakName(entry.getKey()), entry.getValue()));
            }
            for (Map.Entry<String, Value[]> entry : mvRestrictions.entrySet()) {
                rs.add(rp.createRestriction(effectiveOakPath, getOakName(entry.getKey()), entry.getValue()));
            }
        }
        return rs;
    }

    @Nullable
    private String extractPathFromRestrictions(@Nullable Map<String, Value> restrictions, @NotNull String jcrName) throws RepositoryException {
        if (restrictions == null || !restrictions.containsKey(jcrName)) {
            throw new AccessControlException("Entries in principal based access control need to have a path specified. Add rep:nodePath restriction or use PrincipalAccessControlList.addEntry(String, Privilege[], Map, Map) instead.");
        }

        // retrieve path from restrictions and filter that restriction entry for further processing
        return Strings.emptyToNull(restrictions.get(jcrName).getString());
    }

    @NotNull
    private PrivilegeBits validatePrivileges(@NotNull Privilege[] privileges) throws RepositoryException {
        if (privileges.length == 0) {
            throw new AccessControlException("Privileges may not be an empty array");
        }
        for (Privilege p : privileges) {
            Privilege pv = privilegeManager.getPrivilege(p.getName());
            if (pv.isAbstract()) {
                throw new AccessControlException("Privilege " + p + " is abstract.");
            }
        }
        return privilegeBitsProvider.getBits(privileges, getNamePathMapper());
    }

    @NotNull
    private static EntryImpl validateEntry(@Nullable AccessControlEntry entry) throws AccessControlException {
        if (entry instanceof EntryImpl) {
            return (EntryImpl) entry;
        } else {
            throw new AccessControlException("Invalid AccessControlEntry " + entry);
        }
    }

    private boolean addEntry(@NotNull EntryImpl entry) {
        if (entries.contains(entry)) {
            return false;
        } else {
            return entries.add(entry);
        }
    }

    //--------------------------------------------------------------< Entry >---

    final class EntryImpl extends ACE implements Entry {

        private final String oakPath;

        private int hashCode;

        private EntryImpl(@Nullable String oakPath, @NotNull PrivilegeBits privilegeBits, @NotNull  Set<Restriction> restrictions) throws AccessControlException {
            super(principal, privilegeBits, true, restrictions, getNamePathMapper());
            this.oakPath = oakPath;
        }

        @Nullable
        String getOakPath() {
            return oakPath;
        }

        @Override
        @Nullable
        public String getEffectivePath() {
            return (oakPath == null) ? null : getNamePathMapper().getJcrPath(oakPath);
        }

        @Override
        public Privilege[] getPrivileges() {
            Set<String> names =  privilegeBitsProvider.getPrivilegeNames(getPrivilegeBits());
            return Utils.privilegesFromOakNames(names, privilegeManager, getNamePathMapper());
        }

        @Override
        public int hashCode() {
            if (hashCode == 0) {
                hashCode = Objects.hashCode(oakPath, principal.getName(), getPrivilegeBits(), Boolean.TRUE, getRestrictions());
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof EntryImpl) {
                EntryImpl other = (EntryImpl) obj;
                return equivalentPath(other.oakPath) && super.equals(obj);
            }
            return false;
        }

        private boolean equivalentPath(@Nullable String otherPath) {
            return (oakPath == null) ? otherPath == null : oakPath.equals(otherPath);
        }
    }
}
