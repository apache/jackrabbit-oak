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

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Connection;
import org.apache.jackrabbit.oak.api.NodeState;
import org.apache.jackrabbit.oak.api.NodeStateEditor;
import org.apache.jackrabbit.oak.api.NodeStore;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.SimpleCredentials;
import java.io.IOException;

/**
 * ConnectionImpl...
 */
public class ConnectionImpl implements Connection {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(ConnectionImpl.class);

    private final SimpleCredentials credentials;
    private final String workspaceName;
    private final NodeStore store;
    private final QueryEngine queryEngine;

    private NodeState root;

    private ConnectionImpl(SimpleCredentials credentials, String workspaceName,
            NodeStore store, NodeState root, QueryEngine queryEngine) {
        this.credentials = credentials;
        this.workspaceName = workspaceName;
        this.store = store;
        this.queryEngine = queryEngine;
        this.root = root;
    }

    static Connection createWorkspaceConnection(SimpleCredentials credentials,
            String workspace, NodeStore store, String revision, QueryEngine queryEngine)
            throws NoSuchWorkspaceException {

        // TODO set revision!?
        NodeState wspRoot = store.getRoot().getChildNode(workspace);
        if (wspRoot == null) {
            throw new NoSuchWorkspaceException(workspace);
        }

        return new ConnectionImpl(credentials, workspace, store, wspRoot, queryEngine);
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
    public NodeState getCurrentRoot() {
        return root;
    }
    
    @Override
    public void refresh() {
        root = workspaceName == null
            ? store.getRoot()
            : store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public NodeState commit(NodeStateEditor editor) throws CommitFailedException {
        try {
            return root = store.merge(editor, editor.getBaseNodeState());
        }
        catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }

    @Override
    public NodeStateEditor getNodeStateEditor(NodeState state) {
        return store.branch(state);
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
    public Connection getRepositoryConnection() {
        return new ConnectionImpl(credentials, null, store, store.getRoot(), queryEngine);
    }

    @Override
    public QueryEngine getQueryEngine() {
        return queryEngine;
    }

}