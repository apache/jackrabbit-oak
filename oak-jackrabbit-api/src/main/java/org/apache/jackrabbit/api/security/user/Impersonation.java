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
package org.apache.jackrabbit.api.security.user;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import java.security.Principal;

/**
 * The <code>Impersonation</code> maintains Principals that are allowed to
 * impersonate. Principals can be added or removed using
 * {@link #grantImpersonation(Principal)} and
 * {@link #revokeImpersonation(Principal)}, respectively.
 *
 * @see User#getImpersonation()
 */
public interface Impersonation {
    
    /**
     * @return An iterator over the <code>Principal</code>s that are allowed
     * to impersonate the <code>User</code> this <code>Impersonation</code>
     * object has been created for.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull
    PrincipalIterator getImpersonators() throws RepositoryException;

    /**
     * @param principal The principal that should be allowed to impersonate
     * the <code>User</code> this <code>Impersonation</code> has been built for.
     * @return true if the specified <code>Principal</code> has not been allowed
     * to impersonate before and if impersonation has been successfully
     * granted to it, false otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean grantImpersonation(@NotNull Principal principal) throws RepositoryException;

    /**
     * @param principal The principal that should no longer be allowed to
     * impersonate.
     * @return If the granted impersonation has been successfully revoked for
     * the given principal; false otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean revokeImpersonation(@NotNull Principal principal) throws RepositoryException;

    /**
     * Test if the given subject (i.e. any of the principals it contains) is
     * allowed to impersonate.
     *
     * @param subject to impersonate.
     * @return true if this <code>Impersonation</code> allows the specified
     * Subject to impersonate.
     * @throws RepositoryException If an error occurs.
     */
    boolean allows(@NotNull Subject subject) throws RepositoryException;
}
