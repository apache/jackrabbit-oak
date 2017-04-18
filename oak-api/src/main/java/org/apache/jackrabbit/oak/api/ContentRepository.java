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
package org.apache.jackrabbit.oak.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

/**
 * Oak content repository. The repository may be local or remote, or a cluster
 * of any size. These deployment details are all hidden behind this interface.
 * <p>
 * All access to the repository happens through authenticated
 * {@link ContentSession} instances acquired through the
 * {@link #login(Credentials, String)} method, which is why that is the only
 * method of this interface.
 * <p>
 * Starting and stopping ContentRepository instances is the responsibility
 * of each particular deployment and not covered by this interface.
 * Repository clients should use a deployment-specific mechanism (JNDI,
 * OSGi service, etc.) to acquire references to ContentRepository instances.
 * <p>
 * This interface is thread-safe.
 */
public interface ContentRepository {

    /**
     * Authenticates a user based on the given credentials or available
     * out-of-band information and, if successful, returns a
     * {@link ContentSession} instance for accessing repository content
     * inside the specified workspace as the authenticated user.
     * <p>
     * TODO clarify workspace handling once multiple workspaces are
     * supported. See OAK-118.
     * <p>
     * The exact type of access credentials is undefined, as this method
     * simply acts as a generic messenger between clients and pluggable
     * login modules that take care of the actual authentication. See
     * the documentation of relevant login modules for the kind of access
     * credentials they expect.
     * <p>
     * The client must explicitly {@link ContentSession#close()} the
     * returned session once it is no longer used. The recommended access
     * pattern is:
     * <pre>
     * ContentRepository repository = ...;
     * ContentSession session = repository.login(...);
     * try {
     *     ...; // Use the session
     * } finally {
     *     session.close();
     * }
     * </pre>
     *
     * @param credentials   access credentials, or {@code null}
     * @param workspaceName The workspace name or {@code null} if the default
     *                      workspace should be used.
     * @return authenticated repository session
     * @throws LoginException           if authentication failed
     * @throws NoSuchWorkspaceException if the specified workspace name is invalid.
     */
    @Nonnull
    ContentSession login(@Nullable Credentials credentials, @Nullable String workspaceName)
            throws LoginException, NoSuchWorkspaceException;

    /**
     * Returns the repository descriptors which contain all or a subset of the descriptors defined in
     * {@link javax.jcr.Repository}.
     *
     * @return the repository descriptors
     */
    @Nonnull
    Descriptors getDescriptors();
}
