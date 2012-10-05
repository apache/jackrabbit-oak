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
package org.apache.jackrabbit.oak.spi.security.authentication.callback;

import javax.annotation.CheckForNull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.callback.Callback;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RepositoryCallback... TODO
 */
public class RepositoryCallback implements Callback {

    private static final Logger log = LoggerFactory.getLogger(RepositoryCallback.class);

    private NodeStore nodeStore;
    private String workspaceName;

    @CheckForNull
    public NodeStore getNodeStore() {
        return nodeStore;
    }

    public String getWorkspaceName() {
        return workspaceName;
    }

    @CheckForNull
    public ContentSession getContentSession() {
        if (nodeStore != null) {
            try {
                // TODO rather use Oak or similar setup mechanism
                SecurityProvider sp = new OpenSecurityProvider();
                return new ContentRepositoryImpl(nodeStore, null, sp).login(null, workspaceName);
            } catch (LoginException e) {
                log.warn("Internal error ", e.getMessage());
            } catch (NoSuchWorkspaceException e) {
                log.warn("Internal error ", e.getMessage());
            }
        }
        return null;
    }

    public void setNodeStore(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }
}