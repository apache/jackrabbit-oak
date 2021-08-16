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
package org.apache.jackrabbit.api.security.authorization;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

/**
 * Extension of the {@code JackrabbitAccessControlList} that is bound to a {@link Principal}.
 * Consequently, all entries returned by {@link #getAccessControlEntries()} will return the same value
 * as {@link #getPrincipal()} and only entries associated with this very principal can be added/removed from this list.
 * In addition this implies that each entry contained within the {@code PrincipalAccessControlList} defines the target
 * object where it will take effect, which can either be an absolute path to a node or {@code null} if the entry takes
 * effect at the repository level.
 * <p>
 * Typically applicable, existing and effective policies of this type of access control list are expected to be obtained
 * through calls of
 * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getApplicablePolicies(Principal)},
 * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getPolicies(Principal)} and
 * {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getEffectivePolicies(Set)}, respectively.
 * <p>
 * Whether or not accessing {@code PrincipalAccessControlList} policies by path is supported is an implementation detail.
 * If it is supported the {@code absPath} parameter specified with {@link javax.jcr.security.AccessControlManager#getApplicablePolicies(String)}
 * and {@link javax.jcr.security.AccessControlManager#getPolicies(String)} will correspond to the path of the policy.
 * <p>
 * As far as the best-effort method {@link javax.jcr.security.AccessControlManager#getEffectivePolicies(String)} is
 * concerned, the {@link Entry#getEffectivePath() effective path} defined with the individual entries will be consulted
 * in order to compute the policies that take effect at a given path.
 *
 * Irrespective on whether access by path is supported or not the {@link JackrabbitAccessControlPolicy#getPath() path}
 * of the policy points to the access controlled node it is bound to and will be used to
 * {@link javax.jcr.security.AccessControlManager#setPolicy(String, AccessControlPolicy) set} and
 * {@link javax.jcr.security.AccessControlManager#removePolicy(String, AccessControlPolicy) remove} the policy.
 * This access controlled node may or may not be associated with an (optional) representation of the associated
 * {@code Principal} inside the repository.
 */
@ProviderType
public interface PrincipalAccessControlList extends JackrabbitAccessControlList {

    /**
     * Returns the {@link Principal} this policy is bound to. It will be the same all entries contained in this list.
     * An attempt to {@link #addAccessControlEntry(Principal, Privilege[]) add an entry} associated with a different
     * principal than the one returned by this method will fail.
     *
     * @return the target principal of this access control list.
     * @see javax.jcr.security.AccessControlEntry#getPrincipal()
     */
    @NotNull
    Principal getPrincipal();

    /**
     * Adds an access control entry to this policy consisting of the specified {@code effectivePath} and the specified
     * {@code privileges} and indicates upon return if the policy was modified.
     * <p>
     * The {@code effectivePath} defines the object where the privileges will take effect. If {@code effectivePath}
     * is an absolute path then the specified object is a {@link javax.jcr.Node}. If it is {@code null} the object
     * is the repository as a whole and the privileges in question are those that are not associated with any particular node
     * (e.g. privilege to register a namespace). Whether or not an absolute path must point to an accessible node is an
     * implementation detail.
     * <p>
     * How the entries are grouped within the list is an implementation detail. An implementation may e.g. combine the
     * specified privileges with those added by a previous call for the same {@code effectivePath} but it will not remove
     * {@code Privilege}s added by a previous call.
     * <p>
     * Modifications to this policy will not take effect until this policy has been written back by calling
     * {@link javax.jcr.security.AccessControlManager#setPolicy(String, javax.jcr.security.AccessControlPolicy)}
     * followed by {@link javax.jcr.Session#save()} to persist the transient modifications.
     * <p>
     * This method is equivalent to calling {@link #addEntry(String, Privilege[], Map, Map)} with empty restriction maps.
     *
     * @param effectivePath An absolute path or {@code null} to indicate where this entry will take effect.
     * @param privileges an array of {@code Privilege}.
     * @return {@code true} if this policy was modify; {@code false} otherwise.
     * @throws AccessControlException if the specified path or any of the privileges is not valid or if some other access
     * control related exception occurs.
     * @throws RepositoryException If another error occurs
     */
    boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges) throws RepositoryException;

    /**
     * Adds an access control entry to this policy consisting of the specified {@code effectivePath}, the specified
     * {@code privileges} as well as the specified single and multivalued restrictions and indicates upon return if the
     * policy was modified.
     * <p>
     * The {@code effectivePath} defines the object where the privileges will take effect. If {@code effectivePath}
     * is an absolute path then the specified object is a {@link javax.jcr.Node}. If it is {@code null} the object
     * is the repository as a whole and the privileges in question are those that are not associated with any particular node
     * (e.g. privilege to register a namespace). Whether or not an absolute path must point to an accessible node is an
     * implementation detail.
     * <p>
     * The names of the supported restrictions can be obtained by calling {@link #getRestrictionNames()}, while
     * {@link #getRestrictionType(String)} and {@link #isMultiValueRestriction(String)} will reveal the expected value
     * type and cardinality.
     * <p>
     * How the entries are grouped within the list is an implementation detail. An implementation may e.g. combine the
     * specified privileges with those added by a previous call for the same {@code effectivePath} but it will not remove
     * a {@code Privilege} or restrictions added by a previous call.
     * <p>
     * Modifications to this policy will not take effect until this policy has been written back by calling
     * {@link javax.jcr.security.AccessControlManager#setPolicy(String, javax.jcr.security.AccessControlPolicy)}
     * followed by {@link javax.jcr.Session#save()} to persist the transient modifications.
     *
     * @param effectivePath An absolute path or {@code null} to indicate where this entry will take effect.
     * @param privileges an array of {@code Privilege}.
     * @param restrictions The single valued restrictions associated with the entry to be created or an empty map.
     * @param mvRestrictions the multi-valued restrictions associated with the entry to be created or an empty map.
     * @return {@code true} if this policy was modify; {@code false} otherwise.
     * @throws AccessControlException if the specified path, any of the privileges or the restrictions are not valid or
     * if some other access control related exception occurs.
     * @throws RepositoryException If another error occurs
     */
    boolean addEntry(@Nullable String effectivePath, @NotNull Privilege[] privileges, @NotNull Map<String, Value> restrictions, @NotNull Map<String, Value[]> mvRestrictions) throws RepositoryException;

    /**
     * Extension of the {@link JackrabbitAccessControlEntry} that additionally defines the target object where this entry
     * will take effect. Due to the fact that the enclosing policy is itself bound to a principal calling
     * {@link Entry#getPrincipal()} is equivalent to {@link PrincipalAccessControlList#getPrincipal()}.
     */
    interface Entry extends JackrabbitAccessControlEntry {

        /**
         * Returns the path of target object where this entry will take effect. If {@code effectivePath} is an absolute
         * path then the specified object is a {@link javax.jcr.Node}. If it is {@code null} the object is the repository
         * as a whole and the privileges in question are those that are not associated with any particular node.
         *
         * @return The effective absolute path of this entry or {@code null}.
         */
        @Nullable
        String getEffectivePath();
    }
}