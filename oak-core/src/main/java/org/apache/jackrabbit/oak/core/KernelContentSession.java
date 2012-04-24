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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.kernel.KernelRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.SimpleCredentials;
import java.io.IOException;

/**
 * {@link MicroKernel}-based implementation of the {@link ContentSession} interface.
 */
class KernelContentSession implements ContentSession {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(KernelContentSession.class);

    private final SimpleCredentials credentials;
    private final String workspaceName;
    private final KernelNodeStore store;
    private final QueryEngine queryEngine;
    private final CoreValueFactory valueFactory;

    public KernelContentSession(SimpleCredentials credentials, String workspaceName,
            KernelNodeStore store, QueryEngine queryEngine, CoreValueFactory valueFactory) {

        this.credentials = credentials;
        this.workspaceName = workspaceName;
        this.store = store;
        this.queryEngine = queryEngine;
        this.valueFactory = valueFactory;
    }

    @Override
    public AuthInfo getAuthInfo() {
        // todo implement getAuthInfo
        return new AuthInfo() {
            @Override
            public String getUserID() {
                return credentials.getUserID();
            }

            @Override
            public String[] getAttributeNames() {
                return credentials.getAttributeNames();
            }

            @Override
            public Object getAttribute(String attributeName) {
                return credentials.getAttribute(attributeName);
            }
        };
    }
    
    @Override
    public Root getCurrentRoot() {
        return new KernelRoot(store, workspaceName);
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
        return valueFactory;
    }

}