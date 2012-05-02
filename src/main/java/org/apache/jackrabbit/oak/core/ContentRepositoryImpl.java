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
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.mk.index.Indexer;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore2;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.spi.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

/**
 * {@link MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(ContentRepositoryImpl.class);

    // TODO: retrieve default wsp-name from configuration
    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final MicroKernel microKernel;
    private final QueryEngine queryEngine;
    private final NodeStore nodeStore;

    /**
     * Utility constructor that creates a new in-memory repository for use
     * mostly in test cases.
     */
    public ContentRepositoryImpl() {
        this(new MicroKernelImpl());
    }

    private ContentRepositoryImpl(MicroKernel mk) {
        this(mk, getDefaultIndexProvider(mk));
    }

    private static QueryIndexProvider getDefaultIndexProvider(MicroKernel mk) {
        QueryIndexProvider provider = new Indexer(mk);
        provider.init();
        return provider;
    }

    /**
     * Creates an Oak repository instance based on the given, already
     * initialized components.
     *
     * @param mk underlying kernel instance
     * @param indexProvider index provider
     */
    public ContentRepositoryImpl(MicroKernel mk, QueryIndexProvider indexProvider) {
        microKernel = mk;
        nodeStore = new KernelNodeStore2(microKernel);
        queryEngine = new QueryEngineImpl(nodeStore, microKernel, indexProvider);

        // FIXME: workspace setup must be done elsewhere...
        queryEngine.init();
        NodeState root = nodeStore.getRoot();

        NodeState wspNode = root.getChildNode(DEFAULT_WORKSPACE_NAME);

        // FIXME: depends on CoreValue's name mangling
        String ntUnstructured = "nam:nt:unstructured";

        if (wspNode == null) {
            microKernel.commit("/", "+\"" + DEFAULT_WORKSPACE_NAME + "\":{}" + "^\"" + DEFAULT_WORKSPACE_NAME
                    + "/jcr:primaryType\":\"" + ntUnstructured + "\" ", null, null);
        }
    }

    @Override
    public ContentSession login(Object credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = DEFAULT_WORKSPACE_NAME;
        }

        // TODO: add proper implementation
        // TODO  - authentication against configurable spi-authentication
        // TODO  - validation of workspace name (including access rights for the given 'user')

        final SimpleCredentials sc;
        if (credentials == null || credentials instanceof GuestCredentials) {
            sc = new SimpleCredentials("anonymous", new char[0]);
        } else if (credentials instanceof SimpleCredentials) {
            sc = (SimpleCredentials) credentials;
        } else {
            sc = null;
        }

        if (sc == null) {
            throw new LoginException("login failed");
        }

        NodeState wspRoot = nodeStore.getRoot().getChildNode(workspaceName);
        if (wspRoot == null) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        return new ContentSessionImpl(sc, workspaceName, nodeStore, queryEngine);
    }

}
