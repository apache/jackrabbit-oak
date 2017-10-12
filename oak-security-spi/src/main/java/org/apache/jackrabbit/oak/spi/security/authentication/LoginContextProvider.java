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
package org.apache.jackrabbit.oak.spi.security.authentication;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.login.LoginException;

/**
 * Configurable provider taking care of building login contexts for
 * the desired authentication mechanism.
 * <p>
 * This provider defines a single method {@link #getLoginContext(javax.jcr.Credentials, String)}
 * that takes the {@link Credentials credentials} and the workspace name such
 * as passed to {@link org.apache.jackrabbit.oak.api.ContentRepository#login(javax.jcr.Credentials, String)}.
 */
public interface LoginContextProvider {

    /**
     * Returns a new login context instance for handling authentication.
     *
     * @param credentials The {@link Credentials} such as passed to the
     * {@link org.apache.jackrabbit.oak.api.ContentRepository#login(javax.jcr.Credentials, String) login}
     * method of the repository.
     * @param workspaceName The name of the workspace that is being accessed by
     * the login called.
     * @return a new login context
     * @throws LoginException If an error occurs while creating a new context.
     */
    @Nonnull
    LoginContext getLoginContext(Credentials credentials, String workspaceName) throws LoginException;
}