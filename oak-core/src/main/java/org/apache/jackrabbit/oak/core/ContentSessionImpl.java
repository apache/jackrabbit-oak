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
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.SessionQueryEngine;
import org.apache.jackrabbit.oak.plugins.type.BuiltInNodeTypes;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.query.SessionQueryEngineImpl;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code MicroKernel}-based implementation of the {@link ContentSession} interface.
 */
class ContentSessionImpl implements ContentSession {

    private static final Logger log = LoggerFactory.getLogger(ContentSessionImpl.class);

    private final LoginContext loginContext;
    private final String workspaceName;
    private final NodeStore store;
    private final SessionQueryEngine queryEngine;

    private boolean initialised;

    public ContentSessionImpl(LoginContext loginContext, String workspaceName,
                              NodeStore store, QueryEngineImpl queryEngine) {

        this.loginContext = loginContext;
        this.workspaceName = workspaceName;
        this.store = store;
        this.queryEngine = new SessionQueryEngineImpl(this, checkNotNull(queryEngine));
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
        // TODO: improve initial repository/session. See OAK-41
        synchronized (this) {
            if (!initialised) {
                initialised = true;
                BuiltInNodeTypes.register(getLatestRoot());
            }
        }

        return new RootImpl(store, workspaceName, loginContext.getSubject());
    }

    @Override
    public void close() throws IOException {
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

    @Nonnull
    @Override
    public SessionQueryEngine getQueryEngine() {
        return queryEngine;
    }

    @Nonnull
    @Override
    public CoreValueFactory getCoreValueFactory() {
        return store.getValueFactory();
    }
}
