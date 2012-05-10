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

import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginContext;
import java.io.IOException;
import java.util.Set;

/**
 * {@link MicroKernel}-based implementation of the {@link ContentSession} interface.
 */
class ContentSessionImpl implements ContentSession {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(ContentSessionImpl.class);

    private final LoginContext loginContext;
    private final String workspaceName;
    private final NodeStore store;
    private final QueryEngine queryEngine;

    public ContentSessionImpl(LoginContext loginContext, String workspaceName,
                              NodeStore store, QueryEngine queryEngine) {
        this.loginContext = loginContext;
        this.workspaceName = workspaceName;
        this.store = store;
        this.queryEngine = queryEngine;
    }

    @Override
    public AuthInfo getAuthInfo() {
        // todo implement properly with extension point or pass it with the constructor...
        Set<SimpleCredentials> creds = loginContext.getSubject().getPublicCredentials(SimpleCredentials.class);
        final SimpleCredentials sc = (creds.isEmpty()) ? new SimpleCredentials(null, new char[0]) : creds.iterator().next();
        return new AuthInfo() {
            @Override
            public String  getUserID() {
                return sc.getUserID();
            }

            @Override
            public String[] getAttributeNames() {
                return sc.getAttributeNames();
            }

            @Override
            public Object getAttribute(String attributeName) {
                return sc.getAttribute(attributeName);
            }
        };
    }

    @Override
    public Root getCurrentRoot() {
        return new RootImpl(store, workspaceName);
    }

    @Override
    public void close() throws IOException {
        // todo implement close
    }

    @Override
    public String getWorkspaceName() {
        return workspaceName;
    }

    @Override
    public QueryEngine getQueryEngine() {
        return queryEngine;
    }

    @Override
    public CoreValueFactory getCoreValueFactory() {
        return store.getValueFactory();
    }

}