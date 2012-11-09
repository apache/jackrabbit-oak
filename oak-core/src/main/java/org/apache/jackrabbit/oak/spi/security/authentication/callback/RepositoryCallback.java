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

import java.util.Collections;
import javax.annotation.CheckForNull;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.RootImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Callback implementation used to access the repository. It allows to set and
 * get the {@code NodeStore} and the name of the workspace for which the login
 * applies. In addition it provides access to a {@link Root} object based on
 * the given node store and workspace name.
 */
public class RepositoryCallback implements Callback {

    private NodeStore nodeStore;
    private QueryIndexProvider indexProvider;
    private String workspaceName;

    public String getWorkspaceName() {
        return workspaceName;
    }

    @CheckForNull
    public Root getRoot() {
        if (nodeStore != null) {
            Subject subject = new Subject(true, Collections.singleton(SystemPrincipal.INSTANCE), Collections.<Object>emptySet(), Collections.<Object>emptySet());
            AccessControlConfiguration acConfiguration = new OpenAccessControlConfiguration();
            return new RootImpl(nodeStore, workspaceName, subject, acConfiguration, indexProvider);
        }
        return null;
    }

    public void setNodeStore(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    public void setIndexProvider(QueryIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }
}
