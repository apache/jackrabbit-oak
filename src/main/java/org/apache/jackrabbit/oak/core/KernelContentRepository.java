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
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.CoreValueFactory;
import org.apache.jackrabbit.oak.kernel.NodeState;
import org.apache.jackrabbit.oak.api.Branch;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
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
public class KernelContentRepository implements ContentRepository {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(KernelContentRepository.class);

    // TODO: retrieve default wsp-name from configuration
    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final MicroKernel microKernel;
    private final KernelNodeStore nodeStore;

    public KernelContentRepository(MicroKernel mk) {
        microKernel = mk;
        nodeStore = new KernelNodeStore(microKernel);

        // FIXME: workspace setup must be done elsewhere...
        NodeState root = nodeStore.getRoot();
        NodeState wspNode = root.getChildNode(DEFAULT_WORKSPACE_NAME);
        if (wspNode == null) {
            Branch branch = nodeStore.branch(root);
            branch.getTree("/").addChild(DEFAULT_WORKSPACE_NAME);
            nodeStore.merge(branch);
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

        CoreValueFactory valueFactory = new CoreValueFactoryImpl(microKernel);

        QueryEngine queryEngine = new QueryEngineImpl(microKernel, valueFactory);
        // TODO set revision!?
        NodeState wspRoot = nodeStore.getRoot().getChildNode(workspaceName);
        if (wspRoot == null) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        return new KernelContentSession(
                sc, workspaceName, nodeStore, wspRoot, queryEngine, valueFactory);
    }

}
