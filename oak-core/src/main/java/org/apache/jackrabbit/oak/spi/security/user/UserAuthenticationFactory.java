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
package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Provides a user management specific implementation of the
 * {@link org.apache.jackrabbit.oak.spi.security.authentication.Authentication}
 * interface to those {@link javax.security.auth.spi.LoginModule}s that verify
 * a given authentication request by evaluation information exposed by the
 * Jackrabbit user management API.
 */
public interface UserAuthenticationFactory {

    /**
     * Returns an implementation {@link org.apache.jackrabbit.oak.spi.security.authentication.Authentication}
     * for the specified {@code userId}.
     *
     * @param configuration The user configuration.
     * @param root   The {@link org.apache.jackrabbit.oak.api.Root} that provides repository access.
     * @param userId The userId for which a user authentication is provided.
     * @return The authentication object for the given {@code configuration} and
     * {@code userId} or {@code null} if this implementation cannot not handle the
     * specified parameters.
     */
    @CheckForNull
    Authentication getAuthentication(@Nonnull UserConfiguration configuration, @Nonnull Root root, @Nullable String userId);
}
