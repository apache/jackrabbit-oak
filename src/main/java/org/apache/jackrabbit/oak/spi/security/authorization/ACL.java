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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionDefinition;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ACL... TODO
 */
public class ACL implements JackrabbitAccessControlList {

    private static final Logger log = LoggerFactory.getLogger(ACL.class);

    private final String path;
    private final List<AccessControlEntry> entries;
    private final RestrictionProvider restrictionProvider;
    private final NamePathMapper namePathMapper;

    public ACL(String path, List<AccessControlEntry> entries,
               RestrictionProvider restrictionProvider, NamePathMapper namePathMapper) {
        this.path = path;
        this.entries = (entries == null) ? new ArrayList<AccessControlEntry>() : entries;
        this.restrictionProvider = restrictionProvider;
        this.namePathMapper = namePathMapper;
    }

    //--------------------------------------------------< AccessControlList >---
    @Override
    public AccessControlEntry[] getAccessControlEntries() throws RepositoryException {
        return entries.toArray(new AccessControlEntry[entries.size()]);
    }

    @Override
    public boolean addAccessControlEntry(Principal principal, Privilege[] privileges) throws AccessControlException, RepositoryException {
        return addEntry(principal, privileges, true, Collections.<String, Value>emptyMap());
    }

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) throws AccessControlException, RepositoryException {
        if (!(ace instanceof ACE)) {
            throw new AccessControlException("Invalid AccessControlEntry implementation " + ace.getClass().getName() + '.');
        }
        if (!entries.remove(ace)) {
            throw new AccessControlException("Cannot remove AccessControlEntry " + ace);
        }
    }

    //--------------------------------------< JackrabbitAccessControlPolicy >---
    @Override
    public String getPath() {
        return (path == null) ? null : namePathMapper.getJcrPath(path);
    }

    //----------------------------------------< JackrabbitAccessControlList >---
    @Override
    public String[] getRestrictionNames() throws RepositoryException {
        Set<RestrictionDefinition> supported = restrictionProvider.getSupportedRestrictions(path);
        return Collections2.transform(supported, new Function<RestrictionDefinition, String>() {
            @Override
            public String apply(RestrictionDefinition definition) {
                return namePathMapper.getJcrName(definition.getName());
            }
        }).toArray(new String[supported.size()]);

    }

    @Override
    public int getRestrictionType(String restrictionName) throws RepositoryException {
        String oakName = namePathMapper.getOakName(restrictionName);
        for (RestrictionDefinition definition : restrictionProvider.getSupportedRestrictions(path)) {
            if (definition.getName().equals(oakName)) {
                return definition.getRequiredType();
            }
        }
        return PropertyType.UNDEFINED;
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow) throws AccessControlException, RepositoryException {
        return addEntry(principal, privileges, isAllow, Collections.<String, Value>emptyMap());
    }

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions) throws AccessControlException, RepositoryException {
        // NOTE: validation and any kind of optimization of the entry list is
        // delegated to the commit validator
        Set<Restriction> rs;
        if (restrictions == null) {
            rs = Collections.emptySet();
        } else {
            rs = new HashSet<Restriction>(restrictions.size());
            for (String name : restrictions.keySet()) {
                rs.add(restrictionProvider.createRestriction(path, name, restrictions.get(name)));
            }
        }
        AccessControlEntry entry = new ACE(principal, privileges, isAllow, rs, namePathMapper);
        if (entries.contains(entry)) {
            log.debug("Entry is already contained in policy -> no modification.");
            return false;
        } else {
            return entries.add(entry);
        }
    }

    @Override
    public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws AccessControlException, UnsupportedRepositoryOperationException, RepositoryException {
        if (srcEntry.equals(destEntry)) {
            log.debug("'srcEntry' equals 'destEntry' -> no reordering required.");
            return;
        }

        int index = (destEntry == null) ? entries.size()-1 : entries.indexOf(destEntry);
        if (index < 0) {
            throw new AccessControlException("'destEntry' not contained in this AccessControlList.");
        } else {
            if (entries.remove(srcEntry)) {
                // re-insert the srcEntry at the new position.
                entries.add(index, srcEntry);
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
     * @see Object#hashCode()
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
     * @see Object#equals(Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ACL) {
            ACL acl = (ACL) obj;
            return path.equals(acl.path) && entries.equals(acl.entries);
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ACL: ").append(path).append("; ACEs: ");
        for (AccessControlEntry ace : entries) {
            sb.append(ace.toString()).append(';');
        }
        return sb.toString();
    }
}
