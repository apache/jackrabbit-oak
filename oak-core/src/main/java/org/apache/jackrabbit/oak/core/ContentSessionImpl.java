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

import java.io.IOException;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code MicroKernel}-based implementation of the {@link ContentSession} interface.
 */
class ContentSessionImpl implements ContentSession {

    private static final Logger log = LoggerFactory.getLogger(ContentSessionImpl.class);

    private final LoginContext loginContext;
    private final AccessControlProvider accProvider;
    private final String workspaceName;
    private final NodeStore store;
    private final ConflictHandlerProvider conflictHandlerProvider;
    private final QueryIndexProvider indexProvider;

    public ContentSessionImpl(LoginContext loginContext,
            AccessControlProvider accProvider, String workspaceName,
            NodeStore store, ConflictHandlerProvider conflictHandlerProvider,
            QueryIndexProvider indexProvider) {
        this.loginContext = loginContext;
        this.accProvider = accProvider;
        this.workspaceName = workspaceName;
        this.store = store;
        this.conflictHandlerProvider = conflictHandlerProvider;
        this.indexProvider = indexProvider;
    }

    @Nonnull
    @Override
    public AuthInfo getAuthInfo() {
        Set<AuthInfo> infoSet = loginContext.getSubject().getPublicCredentials(AuthInfo.class);
        if (infoSet.isEmpty()) {
            return AuthInfo.EMPTY;
        } else {
            return infoSet.iterator().next();
        }
    }

    @Nonnull
    @Override
    public Root getLatestRoot() {
        RootImpl root = new RootImpl(store, workspaceName, loginContext.getSubject(), accProvider, indexProvider);
        if (conflictHandlerProvider != null) {
            root.setConflictHandler(conflictHandlerProvider.getConflictHandler());
        }
        return root;
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            loginContext.logout();
        } catch (LoginException e) {
            log.error("Error during logout.", e);
        }
    }

    @Override
    public String getWorkspaceName() {
        return workspaceName;
    }

}
