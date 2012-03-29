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

import org.apache.jackrabbit.mk.model.NodeBuilder;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final SimpleCredentials sc;
    private final String workspaceName;

    private String revision;

    ConnectionImpl(SimpleCredentials sc, String workspaceName, String revision) {
        this.sc = sc;
        this.workspaceName = workspaceName;
        this.revision = revision;
    }

    public String getRevision() {
        return revision;
    }

    @Override
    public AuthInfo getAuthInfo() {
        // todo implement getAuthInfo
        return new AuthInfo() {
            @Override
            public String getUserID() {
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
    public NodeState getCurrentRoot() {
        return null; // todo implement getCurrentRoot
    }

    @Override
    public NodeState commit(NodeState newRoot) throws CommitFailedException {
        return null; // todo implement commit
    }

    @Override
    public NodeBuilder getNodeBuilder(NodeState state) {
        return null; // todo implement getNodeBuilder
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
        return null; // todo implement getRepositoryConnection
    }
}