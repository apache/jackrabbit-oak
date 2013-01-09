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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * An implementation of the {@code JackrabbitAccessControlList} interface that only
 * allows for reading. The write methods throw an {@code AccessControlException}.
 */
public class ImmutableACL extends AbstractAccessControlList {

    private final List<AccessControlEntry> entries;

    private int hashCode;

    /**
     * Construct a new {@code UnmodifiableAccessControlList}
     *
     * @param jcrPath
     * @param entries
     * @param restrictionProvider
     */
    public ImmutableACL(String jcrPath, List<? extends AccessControlEntry> entries,
                        RestrictionProvider restrictionProvider) {
        super(jcrPath, restrictionProvider);
        this.entries = (entries == null) ? Collections.<AccessControlEntry>emptyList() : ImmutableList.copyOf(entries);
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public AccessControlEntry[] getAccessControlEntries() throws RepositoryException {
        return entries.toArray(new AccessControlEntry[entries.size()]);
    }

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace)
            throws AccessControlException, RepositoryException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    //----------------------------------------< JackrabbitAccessControlList >---

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges,
                            boolean isAllow, Map<String, Value> restrictions) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    @Override
    public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicy in order to obtain a modifiable ACL.");
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int result = 17;
            result = 37 * result + (getPath() != null ? getPath().hashCode() : 0);
            for (AccessControlEntry entry : entries) {
                result = 37 * result + entry.hashCode();
            }
            hashCode = result;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof JackrabbitAccessControlList) {
            try {
                JackrabbitAccessControlList acl = (JackrabbitAccessControlList) obj;
                return ((jcrPath == null) ? acl.getPath() == null : jcrPath.equals(acl.getPath()))
                        && entries.equals(Arrays.asList(acl.getAccessControlEntries()));
            } catch (RepositoryException e) {
                // failed to retrieve access control entries -> objects are not equal.
            }
        }
        return false;
    }
}