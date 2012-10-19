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
import javax.security.auth.callback.Callback;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Callback implementation used to access the repository. It allows to set and
 * get the {@code NodeStore} and the name of the workspace for which the login
 * applies. In addition it provides access to a {@link Root} object based on
 * the given node store and workspace name.
 */
public class RepositoryCallback implements Callback {

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
    public Root getRoot() {
        if (nodeStore != null) {
            return new RootImpl(nodeStore);
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