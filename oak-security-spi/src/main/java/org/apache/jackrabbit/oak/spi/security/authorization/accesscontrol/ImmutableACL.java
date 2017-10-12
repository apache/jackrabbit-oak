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
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * An implementation of the {@code JackrabbitAccessControlList} interface that only
 * allows for reading. The write methods throw an {@code AccessControlException}.
 */
public class ImmutableACL extends AbstractAccessControlList {

    private final List<JackrabbitAccessControlEntry> entries;
    private final RestrictionProvider restrictionProvider;

    private int hashCode;

    /**
     * Construct a new {@code UnmodifiableAccessControlList}
     *
     * @param oakPath             The Oak path of this policy or {@code null}.
     * @param entries             The access control entries contained in this policy.
     * @param restrictionProvider The restriction provider.
     * @param namePathMapper      The {@link NamePathMapper} used for conversion.
     */
    public ImmutableACL(@Nullable String oakPath,
                        @Nonnull List<? extends JackrabbitAccessControlEntry> entries,
                        @Nonnull RestrictionProvider restrictionProvider,
                        @Nonnull NamePathMapper namePathMapper) {
        super(oakPath, namePathMapper);
        this.entries = ImmutableList.copyOf(entries);
        this.restrictionProvider = restrictionProvider;
    }

    //--------------------------------------------------< AccessControlList >---

    @Override
    public void removeAccessControlEntry(AccessControlEntry ace) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    //----------------------------------------< JackrabbitAccessControlList >---

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges,
                            boolean isAllow, Map<String, Value> restrictions) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    @Override
    public boolean addEntry(Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions, Map<String, Value[]> mvRestrictions) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicies in order to obtain an modifiable ACL.");
    }

    @Override
    public void orderBefore(AccessControlEntry srcEntry, AccessControlEntry destEntry) throws AccessControlException {
        throw new AccessControlException("Immutable ACL. Use AccessControlManager#getPolicy or #getApplicablePolicy in order to obtain a modifiable ACL.");
    }

    //------------------------------------------< AbstractAccessControlList >---
    @Nonnull
    @Override
    public List<JackrabbitAccessControlEntry> getEntries() {
        return entries;
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return restrictionProvider;
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = Objects.hashCode(getOakPath(), entries);
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ImmutableACL) {
            ImmutableACL other = (ImmutableACL) obj;
            return Objects.equal(getOakPath(), other.getOakPath())
                    && entries.equals(other.entries);
        }
        return false;
    }
}