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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@code MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final String defaultWorkspaceName;
    private final SecurityProvider securityProvider;
    private final QueryIndexProvider indexProvider;
    private final NodeStore nodeStore;

    /**
     * Creates an content repository instance based on the given, already
     * initialized components.
     *
     * @param nodeStore            the node store this repository is based upon.
     * @param defaultWorkspaceName the default workspace name;
     * @param indexProvider        index provider
     * @param securityProvider     The configured security provider or {@code null} if
     *                             default implementations should be used.
     */
    public ContentRepositoryImpl(NodeStore nodeStore,
                                 String defaultWorkspaceName,
                                 QueryIndexProvider indexProvider,
                                 SecurityProvider securityProvider) {
        this.nodeStore = nodeStore;
        this.defaultWorkspaceName = (defaultWorkspaceName == null) ? DEFAULT_WORKSPACE_NAME : defaultWorkspaceName;
        this.indexProvider = indexProvider != null ? indexProvider : new CompositeQueryIndexProvider();
        this.securityProvider = securityProvider;
    }

    @Nonnull
    @Override
    public ContentSession login(Credentials credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = defaultWorkspaceName;
        }

        // TODO: support multiple workspaces. See OAK-118
        if (!DEFAULT_WORKSPACE_NAME.equals(workspaceName)) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        LoginContextProvider lcProvider = securityProvider.getAuthenticationConfiguration().getLoginContextProvider(nodeStore, indexProvider);
        LoginContext loginContext = lcProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        AccessControlConfiguration acConfiguration = securityProvider.getAccessControlConfiguration();
        return new ContentSessionImpl(loginContext, acConfiguration, workspaceName,
                nodeStore, indexProvider);
    }

}
