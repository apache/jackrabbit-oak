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
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code NodeStore}-based implementation of the {@link ContentSession} interface.
 */
class ContentSessionImpl implements ContentSession {

    private static final Logger log = LoggerFactory.getLogger(ContentSessionImpl.class);

    /**
     * Session counter. Used to generate unique internal session names.
     */
    private static final AtomicLong SESSION_COUNTER = new AtomicLong();

    private final LoginContext loginContext;
    private final SecurityProvider securityProvider;
    private final String workspaceName;
    private final NodeStore store;
    private final CommitHook hook;
    private final QueryEngineSettings queryEngineSettings;
    private final QueryIndexProvider indexProvider;
    private final Feature classicMove;
    private final String sessionName;

    /**
     * Flag to indicate whether this session is still alive.
     * Only accessed from synchronized methods.
     */
    private boolean live = true;

    public ContentSessionImpl(@NotNull LoginContext loginContext,
                              @NotNull SecurityProvider securityProvider,
                              @NotNull String workspaceName,
                              @NotNull NodeStore store,
                              @NotNull CommitHook hook,
                              QueryEngineSettings queryEngineSettings,
                              @NotNull QueryIndexProvider indexProvider,
                              @Nullable Feature classicMove) {
        this.loginContext = loginContext;
        this.securityProvider = securityProvider;
        this.workspaceName = workspaceName;
        this.store = store;
        this.hook = hook;
        this.queryEngineSettings = queryEngineSettings;
        this.indexProvider = indexProvider;
        this.classicMove = classicMove;
        this.sessionName = "session-" + SESSION_COUNTER.incrementAndGet();
    }

    synchronized void checkLive() {
        Validate.checkState(live, "This session has been closed");
    }

    //-----------------------------------------------------< ContentSession >---
    @NotNull
    @Override
    public AuthInfo getAuthInfo() {
        checkLive();
        return AuthInfoImpl.createFromSubject(loginContext.getSubject());
    }

    @Override
    public String getWorkspaceName() {
        return workspaceName;
    }

    @NotNull
    @Override
    public Root getLatestRoot() {
        checkLive();
        return new MutableRoot(store, hook, workspaceName, loginContext.getSubject(),
                securityProvider, queryEngineSettings, indexProvider, classicMove, this);
    }

    //-----------------------------------------------------------< Closable >---
    @Override
    public synchronized void close() throws IOException {
        live = false;
        try {
            loginContext.logout();
        } catch (LoginException e) {
            log.error("Error during logout.", e);
        }
    }

    @Override
    public String toString() {
        return sessionName;
    }

}
