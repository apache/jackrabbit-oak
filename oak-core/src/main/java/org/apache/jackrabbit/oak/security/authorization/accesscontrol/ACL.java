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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

abstract class ACL extends AbstractAccessControlList {

    private static final Logger log = LoggerFactory.getLogger(ACL.class);

    private final List<ACE> entries = new ArrayList<>();

    ACL(@Nullable String oakPath, @Nullable List<ACE> entries, @NotNull NamePathMapper namePathMapper) {
        super(oakPath, namePathMapper);
        if (entries != null) {
            this.entries.addAll(entries);
        }
    }

    abstract @NotNull ACE createACE(@NotNull Principal principal, @NotNull PrivilegeBits privilegeBits, boolean isAllow, @NotNull Set<Restriction> restrictions) throws RepositoryException;
    abstract boolean checkValidPrincipal(@Nullable Principal principal) throws AccessControlException;
    abstract @NotNull PrivilegeManager getPrivilegeManager();
    abstract @NotNull PrivilegeBits getPrivilegeBits(@NotNull Privilege[] privileges);

    //------------------------------------------< AbstractAccessControlList >---
    @NotNull
    @Override
    public List<ACE> getEntries() {
        return entries;
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) throws RepositoryException {
        ACE entry = checkACE(ace);
        if (!entries.remove(entry)) {
            throw new AccessControlException("Cannot remove AccessControlEntry " + ace);
        }
    }

    //----------------------------------------< JackrabbitAccessControlList >---
    @Override
    public boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges,
                            boolean isAllow, @Nullable Map<String, Value> restrictions,
                            @Nullable Map<String, Value[]> mvRestrictions) throws RepositoryException {
        if (privileges == null || privileges.length == 0) {
            throw new AccessControlException("Privileges may not be null nor an empty array");
        }
        for (Privilege p : privileges) {
            Privilege pv = getPrivilegeManager().getPrivilege(p.getName());
            if (pv.isAbstract()) {
                throw new AccessControlException("Privilege " + p + " is abstract.");
            }
        }

        if (!checkValidPrincipal(principal)) {
            return false;
        }

        Set<Restriction> rs = validateRestrictions((restrictions == null) ? Collections.emptyMap() : restrictions, (mvRestrictions == null) ? Collections.emptyMap() : mvRestrictions);

        ACE entry = createACE(principal, getPrivilegeBits(privileges), isAllow, rs);
        if (entries.contains(entry)) {
            log.debug("Entry is already contained in policy -> no modification.");
            return false;
        } else {
            return internalAddEntry(entry);
        }
    }

    @Override
    public void orderBefore(@NotNull AccessControlEntry srcEntry, @Nullable AccessControlEntry destEntry) throws RepositoryException {
        ACE src = checkACE(srcEntry);
        ACE dest = (destEntry == null) ? null : checkACE(destEntry);

        if (src.equals(dest)) {
            log.debug("'srcEntry' equals 'destEntry' -> no reordering required.");
            return;
        }

        int index = (dest == null) ? entries.size() - 1 : entries.indexOf(dest);
        if (index < 0) {
            throw new AccessControlException("'destEntry' not contained in this AccessControlList.");
        } else {
            if (entries.remove(src)) {
                // re-insert the srcEntry at the new position.
                entries.add(index, src);
            } else {
                // src entry not contained in this list.
                throw new AccessControlException("srcEntry not contained in this AccessControlList");
            }
        }
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ACL: ").append(getPath()).append("; ACEs: ");
        for (AccessControlEntry ace : entries) {
            sb.append(ace.toString()).append(';');
        }
        return sb.toString();
    }

    //------------------------------------------------------------< private >---

    /**
     * Check validity of the specified access control entry.
     *
     * @param entry The access control entry to test.
     * @return The validated {@code ACE}.
     * @throws AccessControlException If the specified entry is invalid.
     */
    private static ACE checkACE(AccessControlEntry entry) throws AccessControlException {
        if (!(entry instanceof ACE)) {
            throw new AccessControlException("Invalid access control entry.");
        }
        return (ACE) entry;
    }

    private boolean internalAddEntry(@NotNull ACE entry) throws RepositoryException {
        final String principalName = entry.getPrincipal().getName();
        final Set<Restriction> restrictions = entry.getRestrictions();
        List<ACE> subList = CollectionUtils.toList(Iterables.filter(entries, ace ->
                principalName.equals(requireNonNull(ace).getPrincipal().getName()) && restrictions.equals(ace.getRestrictions())));

        boolean addEntry = true;
        for (ACE existing : subList) {
            PrivilegeBits existingBits = PrivilegeBits.getInstance(existing.getPrivilegeBits());
            PrivilegeBits entryBits = entry.getPrivilegeBits();
            if (entry.isAllow() == existing.isAllow()) {
                if (existingBits.includes(entryBits)) {
                    // no changes
                    return false;
                } else {
                    // merge existing and new ace
                    existingBits.add(entryBits);
                    int index = entries.indexOf(existing);
                    entries.remove(existing);
                    entries.add(index, createACE(existing, existingBits));
                    addEntry = false;
                }
            } else {
                // existing is complementary entry -> clean up redundant
                // privileges defined by the existing entry
                PrivilegeBits updated = PrivilegeBits.getInstance(existingBits).diff(entryBits);
                if (updated.isEmpty()) {
                    // remove the existing entry as the new entry covers all privileges
                    entries.remove(existing);
                } else if (!updated.includes(existingBits)) {
                    // replace the existing entry having it's privileges adjusted
                    int index = entries.indexOf(existing);
                    entries.remove(existing);
                    entries.add(index, createACE(existing, updated));
                } /* else: no collision that requires adjusting the existing entry.*/
            }
        }
        // finally add the new entry at the end of the list
        if (addEntry) {
            entries.add(entry);
        }
        return true;
    }

    private ACE createACE(@NotNull ACE existing, @NotNull PrivilegeBits newPrivilegeBits) throws RepositoryException {
        return createACE(existing.getPrincipal(), newPrivilegeBits, existing.isAllow(), existing.getRestrictions());
    }

    @NotNull
    private Set<Restriction> validateRestrictions(@NotNull Map<String, Value> restrictions, @NotNull Map<String, Value[]> mvRestrictions) throws RepositoryException {
        Iterable<RestrictionDefinition> mandatoryDefs = Iterables.filter(getRestrictionProvider().getSupportedRestrictions(getOakPath()), RestrictionDefinition::isMandatory);
        for (RestrictionDefinition def : mandatoryDefs) {
            String jcrName = getNamePathMapper().getJcrName(def.getName());
            boolean mandatoryPresent;
            if (def.getRequiredType().isArray()) {
                mandatoryPresent = mvRestrictions.containsKey(jcrName);
            } else {
                mandatoryPresent = restrictions.containsKey(jcrName);
            }
            if (!mandatoryPresent) {
                throw new AccessControlException("Mandatory restriction " + jcrName + " is missing.");
            }
        }

        Set<Restriction> rs = new HashSet<>();
        for (Map.Entry<String, Value> restrEntry : restrictions.entrySet()) {
            String oakName = getNamePathMapper().getOakName(restrEntry.getKey());
            rs.add(getRestrictionProvider().createRestriction(getOakPath(), oakName, restrEntry.getValue()));
        }
        for (Map.Entry<String, Value[]> restrEntry : mvRestrictions.entrySet()) {
            String oakName = getNamePathMapper().getOakName(restrEntry.getKey());
            rs.add(getRestrictionProvider().createRestriction(getOakPath(), oakName, restrEntry.getValue()));
        }
        return rs;
    }
}
