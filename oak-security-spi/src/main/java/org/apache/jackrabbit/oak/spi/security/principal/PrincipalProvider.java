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
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

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
@ProviderType
public interface PrincipalProvider {

    /**
     * Returns the principal with the specified name or {@code null} if the
     * principal does not exist.
     *
     * @param principalName the name of the principal to retrieve
     * @return return the requested principal or {@code null}
     */
    @Nullable
    Principal getPrincipal(@NotNull String principalName);

    /**
     * Returns the {@code ItemBasedPrincipal} with the specified {@code principalOakPath}
     * or {@code null} if no principal with that path exists.
     *
     * @param principalOakPath the Oak path of the {@code ItemBasedPrincipal} to retrieve
     * @return return the requested principal or {@code null}
     */
    @Nullable
    default ItemBasedPrincipal getItemBasedPrincipal(@NotNull String principalOakPath) {
        return null;
    }

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
     * @deprecated use {@link #getMembershipPrincipals(Principal)}
     * @param principal the principal to return it's membership from.
     * @return an iterator returning all groups the given principal is member of.
     * @see java.security.acl.Group#isMember(java.security.Principal)
     */
    @NotNull
    default Set<Group> getGroupMembership(@NotNull Principal principal) {
        return Collections.emptySet();
    }

    /**
     * Returns an iterator over all group principals for which the given
     * principal is either direct or indirect member of. Thus for any principal
     * returned in the iterator {@link org.apache.jackrabbit.api.security.principal.GroupPrincipal#isMember(Principal)}
     * must return {@code true}.
     * <p>
     * Example:<br>
     * If Principal is member of Group A, and Group A is member of
     * Group B, this method will return Group A and Group B.
     *
     * @param principal the principal to return it's membership from.
     * @return an iterator returning all groups the given principal is member of.
     * @see org.apache.jackrabbit.api.security.principal.GroupPrincipal#isMember(java.security.Principal)
     */
    @NotNull
    default Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        return GroupPrincipals.transform(getGroupMembership(principal));
    }

    /**
     * Tries to resolve the specified {@code userID} to a valid principal and
     * it's group membership. This method returns an empty set if the
     * specified ID cannot be resolved.
     *
     * @param userID A userID.
     * @return The set of principals associated with the specified {@code userID}
     * or an empty set if it cannot be resolved.
     */
    @NotNull
    Set<? extends Principal> getPrincipals(@NotNull String userID);

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
    @NotNull
    Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType);

    /**
     * Find the principals that match the specified nameHint and search type.
     *
     * @param nameHint A name hint to use for non-exact matching.
     * @param fullText hint to use a full text query for search
     * @param searchType Limit the search to certain types of principals. Valid
     * values are any of
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_ALL}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_NOT_GROUP}</li></ul>
     * <ul><li>{@link org.apache.jackrabbit.api.security.principal.PrincipalManager#SEARCH_TYPE_GROUP}</li></ul>
     * @param offset Offset from where to start returning results. <code>0</code> for no offset.
     * @param limit Maximal number of results to return. -1 for no limit.
     * @return An iterator of principals.
     * @throws IllegalArgumentException if {@code offset} is negative
     */
    @NotNull
    default Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, boolean fullText, int searchType, long offset, long limit) {
        if (offset < 0) {
            throw new IllegalArgumentException(Long.toString(offset));
        }
        Iterator<? extends Principal> principals = findPrincipals(nameHint, searchType);
        Spliterator<? extends Principal> spliterator = Spliterators.spliteratorUnknownSize(principals, 0);
        Stream<? extends Principal> stream = StreamSupport.stream(spliterator, false);
        if (offset > 0) {
            stream = stream.skip(offset);
        }
        if (limit >= 0) {
            stream = stream.limit(limit);
        }
        return stream.iterator();
    }

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
    @NotNull
    Iterator<? extends Principal> findPrincipals(int searchType);
}
