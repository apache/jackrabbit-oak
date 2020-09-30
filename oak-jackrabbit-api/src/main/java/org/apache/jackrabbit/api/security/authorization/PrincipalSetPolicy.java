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

import java.security.Principal;
import java.util.Set;

import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;

import org.jetbrains.annotations.NotNull;

/**
 * Extension of the JCR {@link javax.jcr.security.AccessControlPolicy AccessControlPolicy}
 * intended to grant a set of {@code Principal}s the ability to perform certain
 * actions. The scope of this policy (and thus the affected items) is an
 * implementation detail; it may e.g. take effect on the tree defined by the
 * {@link javax.jcr.Node}, where a given {@code PrincipalSetPolicy} is being
 * applied.
 *
 * <p>The very details on what actions are granted by a given {@code PrincipalSetPolicy}
 * remains an implementation detail. Similarly a given permission model is
 * in charge of defining the interactions and effects different
 * {@link AccessControlPolicy policies} will have if used together in the same
 * repository.</p>
 */
public interface PrincipalSetPolicy extends AccessControlPolicy {

    /**
     * Returns the set of {@code Principal}s that are allowed to perform
     * implementation specific actions on the items affected by this policy.
     *
     * @return The set of {@code Principal}s that are allowed to perform
     * implementation specific actions on the those items where this policy
     * takes effect.
     */
    @NotNull
    Set<Principal> getPrincipals();

    /**
     * Add {@code Principal}s that are allowed to perform some implementation
     * specific actions on those items where this policy takes effect.
     *
     * @param principals The {@code Principal}s that are granted access.
     * @return {@code true} if this policy was modified; {@code false} otherwise.
     * @throws javax.jcr.security.AccessControlException If any of the specified
     * principals is considered invalid or if another access control specific
     * error occurs.
     */
    boolean addPrincipals(@NotNull Principal... principals) throws AccessControlException;

    /**
     * Remove the specified {@code Principal}s for the set of allowed principals
     * thus revoking their ability to perform the implementation specific actions
     * on items where this policy takes effect.
     *
     * @param principals The {@code Principal}s for which access should be revoked.
     * @return {@code true} if this policy was modified; {@code false} otherwise.
     * @throws javax.jcr.security.AccessControlException If any of the specified
     * principals is considered invalid or if another access control specific
     * error occurs.
     */
    boolean removePrincipals(@NotNull Principal... principals) throws AccessControlException;
}