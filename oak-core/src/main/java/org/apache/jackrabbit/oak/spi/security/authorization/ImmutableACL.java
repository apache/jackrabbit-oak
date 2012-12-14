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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * An implementation of the {@code JackrabbitAccessControlList} interface that only
 * allows for reading. The write methods throw an {@code AccessControlException}.
 */
public class ImmutableACL extends ACL {

    private int hashCode;

    /**
     * Construct a new {@code UnmodifiableAccessControlList}
     *
     * @param jcrPath
     * @param entries
     * @param restrictionProvider
     * @param namePathMapper
     */
    public ImmutableACL(String jcrPath, List<ACE> entries,
                        RestrictionProvider restrictionProvider,
                        NamePathMapper namePathMapper) {
        super(jcrPath, getImmutableEntries(entries), restrictionProvider, namePathMapper);
    }

    private static List<ACE> getImmutableEntries(List<ACE> entries) {
        return (entries == null) ? Collections.<ACE>emptyList() : ImmutableList.copyOf(entries);
    }

    //--------------------------------------------------< AccessControlList >---
    /**
     * @see AccessControlList#addAccessControlEntry(java.security.Principal, javax.jcr.security.Privilege[])
     */
    public boolean addAccessControlEntry(Principal principal,
                                         Privilege[] privileges)
            throws AccessControlException, RepositoryException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    /**
     * @see AccessControlList#removeAccessControlEntry(AccessControlEntry)
     */
    public void removeAccessControlEntry(AccessControlEntry ace)
            throws AccessControlException, RepositoryException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    /**
     * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#addEntry(Principal, Privilege[], boolean)
     */
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    /**
     * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#addEntry(Principal, Privilege[], boolean, Map)
     */
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    /**
     * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList#orderBefore(AccessControlEntry, AccessControlEntry)
     */
    public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicy in order to obtain a modifiable ACL.");
    }

    //-------------------------------------------------------------< Object >---
    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            int result = 17;
            result = 37 * result + (getPath() != null ? getPath().hashCode() : 0);
            for (ACE entry : getACEs()) {
                result = 37 * result + entry.hashCode();
            }
            hashCode = result;
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

        if (obj instanceof ImmutableACL) {
            return super.equals(obj);
        }
        return false;
    }
}