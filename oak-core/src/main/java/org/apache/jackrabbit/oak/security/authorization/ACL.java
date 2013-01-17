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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.ACE;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACL... TODO
 */
abstract class ACL extends AbstractAccessControlList {

    private static final Logger log = LoggerFactory.getLogger(ACL.class);

    private final List<JackrabbitAccessControlEntry> entries = new ArrayList<JackrabbitAccessControlEntry>();

    ACL(String oakPath, NamePathMapper namePathMapper) {
        this(oakPath, null, namePathMapper);
    }

    ACL(String oakPath, List<JackrabbitAccessControlEntry> entries, NamePathMapper namePathMapper) {
        super(oakPath, namePathMapper);
        if (entries != null) {
            this.entries.addAll(entries);
        }
    }

    @Nonnull
    @Override
    public List<JackrabbitAccessControlEntry> getEntries() {
        return entries;
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) throws RepositoryException {
        JackrabbitAccessControlEntry entry = checkACE(ace);
        if (!entries.remove(entry)) {
            throw new AccessControlException("Cannot remove AccessControlEntry " + ace);
        }
    }

    //----------------------------------------< JackrabbitAccessControlList >---

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges,
                            boolean isAllow, Map<String, Value> restrictions) throws RepositoryException {
        if (privileges == null || privileges.length == 0) {
            throw new AccessControlException("Privileges may not be null nor an empty array");
        }
        // TODO: check again.
        // NOTE: in contrast to jr2 any further validation and optimization of
        // the entry list is delegated to the commit validator
        Set<Restriction> rs;
        if (restrictions == null) {
            rs = Collections.emptySet();
        } else {
            rs = new HashSet<Restriction>(restrictions.size());
            for (String name : restrictions.keySet()) {
                rs.add(getRestrictionProvider().createRestriction(getOakPath(), name, restrictions.get(name)));
            }
        }
        JackrabbitAccessControlEntry entry = new ACE(principal, privileges, isAllow, rs);
        if (entries.contains(entry)) {
            log.debug("Entry is already contained in policy -> no modification.");
            return false;
        } else {
            return entries.add(entry);
        }
    }

    @Override
    public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws RepositoryException {
        JackrabbitAccessControlEntry src = checkACE(srcEntry);
        JackrabbitAccessControlEntry dest = (destEntry == null) ? null : checkACE(destEntry);

        if (src.equals(dest)) {
            log.debug("'srcEntry' equals 'destEntry' -> no reordering required.");
            return;
        }

        int index = (dest == null) ? entries.size()-1 : entries.indexOf(dest);
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
    /**
     * Returns zero to satisfy the Object equals/hashCode contract.
     * This class is mutable and not meant to be used as a hash key.
     *
     * @return always zero
     */
    @Override
    public int hashCode() {
        return 0;
    }

    /**
     * Returns true if the path and the entries are equal; false otherwise.
     *
     * @param obj Object to test.
     * @return true if the path and the entries are equal; false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ACL) {
            ACL acl = (ACL) obj;
            String path = getOakPath();
            String otherPath = acl.getOakPath();
            return ((path == null) ? otherPath == null : path.equals(otherPath))
                    && entries.equals(acl.entries);
        }
        return false;
    }

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
    private static JackrabbitAccessControlEntry checkACE(AccessControlEntry entry) throws AccessControlException {
        if (!(entry instanceof ACE)) {
            throw new AccessControlException("Invalid access control entry.");
        }
        return (ACE) entry;
    }
}
