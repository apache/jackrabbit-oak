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
package org.apache.jackrabbit.oak.spi.security.authorization.cug;

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;

/**
 * Denies read access for all principals except for the specified principals.
 */
public interface CugPolicy extends JackrabbitAccessControlPolicy {

    /**
     * Returns the set of {@code Principal}s that are allowed to access the items
     * in the restricted area defined by this policy.
     *
     * @return The set of {@code Principal}s that are allowed to access the
     * restricted area.
     */
    @Nonnull
    Set<Principal> getPrincipals();

    /**
     * Add {@code Principal}s that are allowed to access the restricted area.
     *
     * @param principals The {@code Principal}s that are granted read access.
     * @return {@code true} if this policy was modified; {@code false} otherwise.
     * @throws AccessControlException If any of the specified principals is
     * invalid.
     */
    boolean addPrincipals(@Nonnull Principal... principals) throws AccessControlException;

    /**
     * Remove the specified {@code Principal}s for the set of allowed principals
     * thus revoking their ability to read items in the restricted area defined
     * by this policy.
     *
     * @param principals The {@code Principal}s for which access should be revoked.
     * @return {@code true} if this policy was modified; {@code false} otherwise.
     * @throws  AccessControlException If an error occurs.
     */
    boolean removePrincipals(@Nonnull Principal... principals) throws AccessControlException;

}
