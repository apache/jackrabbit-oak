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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

import java.security.Principal;
import java.util.Set;

/**
 * Interface that allows to define the principals for which principal based access control management and permission
 * evaluation can be executed. For any other principals this module would never take effect.
 */
@ProviderType
public interface Filter {

    /**
     * Reveals if this filter implementation is able to handle the given set of principals.
     *
     * @param principals A set of principals.
     * @return {@code true} if the principals can be dealt with by this filter implementation, {@code false} otherwise.
     */
    boolean canHandle(@NotNull Set<Principal> principals);

    /**
     * Returns the Oak path of the {@code Tree} to which the policy for the given {@code validPrincipal} will be bound.
     * This method can rely on the fact that the given principal has been {@link #canHandle(Set) validated} before and is
     * not expected to validate the principal.
     *
     * @param validPrincipal A valid principal i.e. that has been validated through {@link #canHandle(Set)}.
     * @return The absolute oak path to an exiting {@code Tree}. The policy for the given principal will be bound to that tree.
     * @throws IllegalArgumentException If the specified principal is not validated/valid.
     */
    @NotNull
    String getOakPath(@NotNull Principal validPrincipal);

    /**
     * Retrieves the {@link org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal} for the given {@code oakPath}
     * and returns it if it is considered valid by the {@code Filter} implementation. Otherwise this method returns
     * {@code null}.
     *
     * @param oakPath A non-null Oak path pointing to an {@link org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal}.
     * @return A valid principal or {@code null} if no valid principal can be retrieved/exists for the given path.
     */
    @Nullable
    Principal getValidPrincipal(@NotNull String oakPath);

}
