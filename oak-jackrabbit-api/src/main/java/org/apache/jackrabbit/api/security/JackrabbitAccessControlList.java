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
package org.apache.jackrabbit.api.security;

import java.security.Principal;
import java.util.Map;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * <code>JackrabbitAccessControlList</code> is an extension of the <code>AccessControlList</code>.
 * Similar to the latter any modifications made will not take effect, until it is
 * {@link javax.jcr.security.AccessControlManager#setPolicy(String, AccessControlPolicy)
 * written back} and {@link javax.jcr.Session#save() saved}.
 */
@ProviderType
public interface JackrabbitAccessControlList extends JackrabbitAccessControlPolicy, AccessControlList {

    /**
     * Returns the names of the supported restrictions or an empty array
     * if no restrictions are respected.
     *
     * @return the names of the supported restrictions or an empty array.
     * @see #addEntry(Principal, Privilege[], boolean, Map)
     * @throws RepositoryException If an error occurs.
     */
    @NotNull
    String[] getRestrictionNames() throws RepositoryException;

    /**
     * Return the expected {@link javax.jcr.PropertyType property type} of the
     * restriction with the specified <code>restrictionName</code>.
     *
     * @param restrictionName Any of the restriction names retrieved from
     * {@link #getRestrictionNames()}.
     * @return expected {@link javax.jcr.PropertyType property type}.
     * @throws RepositoryException If an error occurs.
     */
    int getRestrictionType(@NotNull String restrictionName) throws RepositoryException;

    /**
     * Returns <code>true</code> if the restriction is multivalued; <code>false</code>
     * otherwise. If an given implementation doesn't support multivalued restrictions,
     * this method always returns <code>false</code>.
     *
     * @param restrictionName Any of the restriction names retrieved from
     * {@link #getRestrictionNames()}.
     * @return <code>true</code> if the restriction is multivalued; <code>false</code>
     * if the restriction with the given name is single value or if the implementation
     * doesn't support multivalued restrictions, this method always returns <code>false</code>.
     * @throws RepositoryException If an error occurs.
     * @see #addEntry(Principal, Privilege[], boolean, Map, Map)
     */
    boolean isMultiValueRestriction(@NotNull String restrictionName) throws RepositoryException;

    /**
     * Returns <code>true</code> if this policy does not yet define any
     * entries.
     *
     * @return If no entries are present.
     */
    boolean isEmpty();

    /**
     * Returns the number of entries or 0 if the policy {@link #isEmpty() is empty}.
     *
     * @return The number of entries present or 0 if the policy {@link #isEmpty() is empty}.
     */
    int size();

    /**
     * Same as {@link #addEntry(Principal, Privilege[], boolean, Map)} using
     * some implementation specific restrictions.
     *
     * @param principal the principal to add the entry for
     * @param privileges the privileges to add
     * @param isAllow if <code>true</code> if this is a positive (allow) entry
     * @return true if this policy has changed by incorporating the given entry;
     * false otherwise.
     * @throws AccessControlException If any of the given parameter is invalid
     * or cannot be handled by the implementation.
     * @throws RepositoryException If another error occurs.
     * @see AccessControlList#addAccessControlEntry(Principal, Privilege[])
     */
    boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges, boolean isAllow)
            throws AccessControlException, RepositoryException;

    /**
     * Adds an access control entry to this policy consisting of the specified
     * <code>principal</code>, the specified <code>privileges</code>, the
     * <code>isAllow</code> flag and an optional map containing additional
     * restrictions.
     * <p>
     * This method returns <code>true</code> if this policy was modified,
     * <code>false</code> otherwise.
     * <p>
     * An <code>AccessControlException</code> is thrown if any of the specified
     * parameters is invalid or if some other access control related exception occurs.
     * 
     * @param principal the principal to add the entry for
     * @param privileges the privileges to add
     * @param isAllow if <code>true</code> if this is a positive (allow) entry
     * @param restrictions A map of additional restrictions used to narrow the
     * effect of the entry to be created. The map must map JCR names to a single
     * {@link javax.jcr.Value} object.
     * @return true if this policy has changed by incorporating the given entry;
     * false otherwise.
     * @throws AccessControlException If any of the given parameter is invalid
     * or cannot be handled by the implementation.
     * @throws RepositoryException If another error occurs.
     * @see AccessControlList#addAccessControlEntry(Principal, Privilege[])
     */
    boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges,
                     boolean isAllow, @Nullable Map<String, Value> restrictions)
            throws AccessControlException, RepositoryException;

    /**
     * Adds an access control entry to this policy consisting of the specified
     * <code>principal</code>, the specified <code>privileges</code>, the
     * <code>isAllow</code> flag and an optional map containing additional
     * restrictions.
     * <p>
     * This method returns <code>true</code> if this policy was modified,
     * <code>false</code> otherwise.
     * <p>
     * An <code>AccessControlException</code> is thrown if any of the specified
     * parameters is invalid or if some other access control related exception occurs.
     *
     * @param principal the principal to add the entry for
     * @param privileges the privileges to add
     * @param isAllow if <code>true</code> if this is a positive (allow) entry
     * @param restrictions A map of additional restrictions used to narrow the
     * effect of the entry to be created. The map must map JCR names to a single
     * {@link javax.jcr.Value} object.
     * @param mvRestrictions A map of additional multivalued restrictions used to narrow the
     * effect of the entry to be created. The map must map JCR names to a
     * {@link javax.jcr.Value} array.
     * @return true if this policy has changed by incorporating the given entry;
     * false otherwise.
     * @throws AccessControlException If any of the given parameter is invalid
     * or cannot be handled by the implementation.
     * @throws RepositoryException If another error occurs.
     * @see AccessControlList#addAccessControlEntry(Principal, Privilege[])
     * @since 2.8
     */
    boolean addEntry(@NotNull Principal principal, @NotNull Privilege[] privileges,
                     boolean isAllow, @Nullable  Map<String, Value> restrictions,
                     @Nullable Map<String, Value[]> mvRestrictions)
            throws AccessControlException, RepositoryException;

    /**
     * If the <code>AccessControlList</code> implementation supports
     * reordering of entries the specified <code>srcEntry</code> is inserted
     * at the position of the specified <code>destEntry</code>.
     * <p>
     * If <code>destEntry</code> is <code>null</code> the entry is moved to the
     * end of the list.
     * <p>
     * If <code>srcEntry</code> and <code>destEntry</code> are the same no
     * changes are made.
     * 
     * @param srcEntry The access control entry to be moved within the list.
     * @param destEntry The entry before which the <code>srcEntry</code> will be moved.
     * @throws AccessControlException If any of the given entries is invalid or
     * cannot be handled by the implementation.
     * @throws UnsupportedRepositoryOperationException If ordering is not supported.
     * @throws RepositoryException If another error occurs.
     */
    void orderBefore(@NotNull AccessControlEntry srcEntry, @Nullable AccessControlEntry destEntry)
            throws AccessControlException, UnsupportedRepositoryOperationException, RepositoryException;
}
