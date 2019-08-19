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

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.PathNotFoundException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import java.security.Principal;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * <code>JackrabbitAccessControlManager</code> provides extensions to the
 * <code>AccessControlManager</code> interface.
 */
@ProviderType
public interface JackrabbitAccessControlManager extends AccessControlManager {

    /**
     * Returns the applicable policies for the specified <code>principal</code>
     * or an empty array if no additional policies can be applied.
     *
     * @param principal A principal known to the editing session.
     * @return array of policies for the specified <code>principal</code>. Note
     * that the policy object returned must reveal the path of the node where
     * they can be applied later on using {@link AccessControlManager#setPolicy(String, javax.jcr.security.AccessControlPolicy)}.
     * @throws AccessDeniedException if the session lacks
     * <code>MODIFY_ACCESS_CONTROL</code> privilege.
     * @throws AccessControlException if the specified principal does not exist
     * or if another access control related exception occurs.
     * @throws UnsupportedRepositoryOperationException if editing access control
     * policies by principal is not supported.
     * @throws RepositoryException if another error occurs.
     * @see JackrabbitAccessControlPolicy#getPath()
     */
    @NotNull
    JackrabbitAccessControlPolicy[] getApplicablePolicies(@NotNull Principal principal) throws AccessDeniedException, AccessControlException, UnsupportedRepositoryOperationException, RepositoryException;

    /**
     * Returns the <code>AccessControlPolicy</code> objects that have been set
     * for the given <code>principal</code> or an empty array if no policy has
     * been set. This method reflects the binding state, including transient
     * policy modifications.
     *
     * @param principal A valid principal.
     * @return The policies defined for the given principal or an empty array.
     * @throws AccessDeniedException if the session lacks
     * <code>READ_ACCESS_CONTROL</code> privilege.
     * @throws AccessControlException  if the specified principal does not exist
     * or if another access control related exception occurs.
     * @throws UnsupportedRepositoryOperationException if editing access control
     * policies by principal is not supported.
     * @throws RepositoryException If another error occurs.
     */
    @NotNull
    JackrabbitAccessControlPolicy[] getPolicies(@NotNull Principal principal) throws AccessDeniedException, AccessControlException, UnsupportedRepositoryOperationException, RepositoryException;

    /**
     * Returns the <code>AccessControlPolicy</code> objects that are in effect
     * for the given <code>Principal</code>s. This may be policies set through
     * this API or some implementation specific (default) policies.
     *
     * @param principals A set of valid principals.
     * @return The policies defined for the given principal or an empty array.
     * @throws AccessDeniedException if the session lacks
     * <code>READ_ACCESS_CONTROL</code> privilege.
     * @throws AccessControlException  if the specified principal does not exist
     * or if another access control related exception occurs.
     * @throws UnsupportedRepositoryOperationException if editing access control
     * policies by principal is not supported.
     * @throws RepositoryException If another error occurs.
     */
    @NotNull
    AccessControlPolicy[] getEffectivePolicies(@NotNull Set<Principal> principals) throws AccessDeniedException, AccessControlException, UnsupportedRepositoryOperationException, RepositoryException;

    /**
     * Returns whether the given set of <code>Principal</code>s has the specified
     * privileges for absolute path <code>absPath</code>, which must be an
     * existing node.
     * <p>
     * Testing an aggregate privilege is equivalent to testing each non
     * aggregate privilege among the set returned by calling
     * <code>Privilege.getAggregatePrivileges()</code> for that privilege.
     * <p>
     * The results reported by the this method reflect the net <i>effect</i> of
     * the currently applied control mechanisms. It does not reflect unsaved
     * access control policies or unsaved access control entries. Changes to
     * access control status caused by these mechanisms only take effect on
     * <code>Session.save()</code> and are only then reflected in the results of
     * the privilege test methods.
     * <p>
     * Since this method allows to view the privileges of principals other
     * than included in the editing session, this method must throw
     * <code>AccessDeniedException</code> if the session lacks
     * <code>READ_ACCESS_CONTROL</code> privilege for the <code>absPath</code>
     * node.
     *
     * @param absPath    an absolute path.
     * @param principals a set of <code>Principal</code>s for which is the
     * given privileges are tested.
     * @param privileges an array of <code>Privilege</code>s.
     * @return <code>true</code> if the session has the specified privileges;
     *         <code>false</code> otherwise.
     * @throws javax.jcr.PathNotFoundException if no node at <code>absPath</code> exists
     * or the session does not have sufficient access to retrieve a node at that location.
     * @throws AccessDeniedException if the session lacks
     * <code>READ_ACCESS_CONTROL</code> privilege for the <code>absPath</code> node.
     * @throws RepositoryException  if another error occurs.
     */
    public boolean hasPrivileges(@Nullable String absPath, @NotNull Set<Principal> principals, @NotNull Privilege[] privileges)
            throws PathNotFoundException, AccessDeniedException, RepositoryException;

    /**
     * Returns the privileges the given set of <code>Principal</code>s has for
     * absolute path <code>absPath</code>, which must be an existing node.
     * <p>
     * The returned privileges are those for which {@link #hasPrivileges} would
     * return <code>true</code>.
     * <p>
     * The results reported by the this method reflect the net <i>effect</i> of
     * the currently applied control mechanisms. It does not reflect unsaved
     * access control policies or unsaved access control entries. Changes to
     * access control status caused by these mechanisms only take effect on
     * <code>Session.save()</code> and are only then reflected in the results of
     * the privilege test methods.
     * <p>
     * Since this method allows to view the privileges of principals other
     * than included in the editing session, this method must throw
     * <code>AccessDeniedException</code> if the session lacks
     * <code>READ_ACCESS_CONTROL</code> privilege for the <code>absPath</code>
     * node.
     * <p>
     * Note that this method does not resolve any group membership, as this is
     * the job of the user manager. nor does it augment the set with the
     * "everyone" principal.
     *
     * @param absPath an absolute path.
     * @param principals a set of <code>Principal</code>s for which is the
     * privileges are retrieved.
     * @return an array of <code>Privilege</code>s.
     * @throws PathNotFoundException if no node at <code>absPath</code> exists
     * or the session does not have sufficient access to retrieve a node at that
     * location.
     * @throws AccessDeniedException if the session lacks <code>READ_ACCESS_CONTROL</code>
     * privilege for the <code>absPath</code> node.
     * @throws RepositoryException  if another error occurs.
     */
    @NotNull
    public Privilege[] getPrivileges(@Nullable String absPath, @NotNull Set<Principal> principals)
            throws PathNotFoundException, AccessDeniedException, RepositoryException;
}