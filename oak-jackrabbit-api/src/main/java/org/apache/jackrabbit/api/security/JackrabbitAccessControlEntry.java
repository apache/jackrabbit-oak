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

import org.apache.jackrabbit.api.security.authorization.PrivilegeCollection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;
import javax.jcr.security.AccessControlEntry;

/**
 * <code>JackrabbitAccessControlEntry</code> is a Jackrabbit specific extension
 * of the <code>AccessControlEntry</code> interface. It represents an single
 * entry of a {@link JackrabbitAccessControlList}.
 */
@ProviderType
public interface JackrabbitAccessControlEntry extends AccessControlEntry {

    /**
     * @return true if this entry adds <code>Privilege</code>s for the principal;
     * false otherwise.
     */
    boolean isAllow();

    /**
     * Return the names of the restrictions present with this access control entry.
     *
     * @return the names of the restrictions
     * @throws RepositoryException if an error occurs.
     */
    @NotNull
    String[] getRestrictionNames() throws RepositoryException;

    /**
     * Return the value of the restriction with the specified name or
     * <code>null</code> if no such restriction exists. In case the restriction
     * with the specified name contains multiple value this method will call
     * {@code ValueFormatException}.
     *
     * @param restrictionName The of the restriction as obtained through
     * {@link #getRestrictionNames()}.
     * @return value of the restriction with the specified name or
     * <code>null</code> if no such restriction exists.
     * @throws ValueFormatException If the restriction with the specified name
     * contains multiple values.
     * @throws RepositoryException if an error occurs.
     * @see #getRestrictions(String)
     */
    @Nullable
    Value getRestriction(@NotNull String restrictionName) throws ValueFormatException, RepositoryException;

    /**
     * Return the values of the restriction with the specified name or
     * <code>null</code> if no such restriction exists. For restrictions that
     * contain just a single value this method is expected to return an array
     * with a single element even if the underlying implementation stored the
     * restriction in single-value JCR property.
     *
     * @param restrictionName The of the restriction as obtained through
     * {@link #getRestrictionNames()}.
     * @return the values of the restriction with the specified name as an array
     * or <code>null</code> if no such restriction exists. The array may contain
     * zero, one or multiple values.
     * @throws RepositoryException if an error occurs.
     * @see #getRestriction(String)
     */
    @Nullable
    Value[] getRestrictions(@NotNull String restrictionName) throws RepositoryException;

    /**
     * Returns a {@link PrivilegeCollection} representing the privileges associated with this entry.
     *
     * @return A {@link PrivilegeCollection} wrapping around the privileges defined for this instance of {@code JackrabbitAccessControlEntry}.
     * @throws RepositoryException If an error occurs.
     * @since Oak 1.42.0
     * @see #getPrivileges() 
     */
    @NotNull
    PrivilegeCollection getPrivilegeCollection() throws RepositoryException;
}
