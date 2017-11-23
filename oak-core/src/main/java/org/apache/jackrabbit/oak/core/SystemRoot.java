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
import javax.annotation.Nullable;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 *  Internal extension of the {@link MutableRoot} to be used
 *  when an usage of the system internal subject is needed.
 */
public class SystemRoot extends MutableRoot {

    private static final LoginContext LOGIN_CONTEXT = new LoginContext() {
        @Override
        public Subject getSubject() {
            return SystemSubject.INSTANCE;
        }
        @Override
        public void login() {
        }
        @Override
        public void logout() {
        }
    };
    
    public static SystemRoot create(@Nonnull NodeStore store, @Nonnull CommitHook hook,
            @Nonnull String workspaceName, @Nonnull SecurityProvider securityProvider,
            @Nonnull QueryIndexProvider indexProvider) {
        return create(store, hook, workspaceName, securityProvider, null, indexProvider);
    }
    
    public static SystemRoot create(@Nonnull NodeStore store, @Nonnull CommitHook hook,
            @Nonnull String workspaceName, @Nonnull SecurityProvider securityProvider,
            @Nullable QueryEngineSettings queryEngineSettings,
            @Nonnull QueryIndexProvider indexProvider) {
        if (queryEngineSettings == null) {
            queryEngineSettings = new QueryEngineSettings();
        }
        return new SystemRoot(store, hook, workspaceName, securityProvider,
                queryEngineSettings, indexProvider);
    }

    private SystemRoot(@Nonnull final NodeStore store, @Nonnull final CommitHook hook,
                      @Nonnull final String workspaceName, @Nonnull final SecurityProvider securityProvider,
                      @Nonnull final QueryEngineSettings queryEngineSettings,
                      @Nonnull final QueryIndexProvider indexProvider) {
        this(store, hook, workspaceName, securityProvider, queryEngineSettings, indexProvider,
                new ContentSessionImpl(
                        LOGIN_CONTEXT, securityProvider, workspaceName,
                        store, hook, queryEngineSettings, indexProvider) {
                    @Nonnull
                    @Override
                    public Root getLatestRoot() {
                        return new SystemRoot(
                                store, hook, workspaceName, securityProvider,
                                queryEngineSettings,
                                indexProvider, this);
                    }
                });
    }
    
    private SystemRoot(@Nonnull NodeStore store, @Nonnull CommitHook hook,
            @Nonnull String workspaceName, @Nonnull SecurityProvider securityProvider,
            @Nullable QueryEngineSettings queryEngineSettings,
            @Nonnull QueryIndexProvider indexProvider,
            @Nonnull ContentSessionImpl session) {
        super(store, hook, workspaceName, SystemSubject.INSTANCE,
                securityProvider, queryEngineSettings, indexProvider, session);
    }
    
}
