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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The {@code PrincipalProvider} defines methods to provide access to sources
 * of {@link Principal}s. This allows the security framework share any external
 * sources for authorization and authentication, as may be used by a custom
 * {@link javax.security.auth.spi.LoginModule} for example.
 *
 * A single {@code PrincipalProvider} implementation is expected to exposed
 * principals from one single source. In contrast to the
 * {@link org.apache.jackrabbit.api.security.principal.PrincipalManager}
 * which will expose all known and accessible principals from possibly
 * different sources. See also {@link CompositePrincipalProvider} for a
 * mechanism to combine principals of different providers.
 */
public interface PrincipalProvider {

    /**
     * Returns the principal with the specified name or {@code null} if the
     * principal does not exist.
     *
     * @param principalName the name of the principal to retrieve
     * @return return the requested principal or {@code null}
     */
    @CheckForNull
    Principal getPrincipal(@Nonnull String principalName);

    /**
     * Returns an iterator over all group principals for which the given
     * principal is either direct or indirect member of. Thus for any principal
     * returned in the iterator {@link java.security.acl.Group#isMember(Principal)}
     * must return {@code true}.
     * <p>
     * Example:<br>
     * If Principal is member of Group A, and Group A is member of
     * Group B, this method will return Group A and Group B.
     *
     * @param principal the principal to return it's membership from.
     * @return an iterator returning all groups the given principal is member of.
     * @see java.security.acl.Group#isMember(java.security.Principal)
     */
    @Nonnull
    Set<Group> getGroupMembership(@Nonnull Principal principal);

    /**
     * Tries to resolve the specified {@code userID} to a valid principal and
     * it's group membership. This method returns an empty set if the
     * specified ID cannot be resolved.
     *
     * @param userID A userID.
     * @return The set of principals associated with the specified {@code userID}
     * or an empty set if it cannot be resolved.
     */
    @Nonnull
    Set<? extends Principal> getPrincipals(@Nonnull String userID);

    /**
     * Find the principals that match the specified nameHint and search type.
     *
     * @param nameHint A name hint to use for non-exact matching.
     * @param searchType Limit the search to certain types of principals. Valid
     * values are any of
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_ALL}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_NOT_GROUP}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_GROUP}</li></ul>
     * @return An iterator of principals.
     */
    @Nonnull
    Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType);


    /**
     * Find all principals that match the search type.
     *
     * @param searchType Limit the search to certain types of principals. Valid
     * values are any of
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_ALL}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_NOT_GROUP}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_GROUP}</li></ul>
     * @return An iterator of principals.
     */
    @Nonnull
    Iterator<? extends Principal> findPrincipals(int searchType);
}