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
package org.apache.jackrabbit.oak.security.authorization;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implementation of the {@code WorkspaceInitializer} interface that creates
 * a property index definitions for {@link #REP_PRINCIPAL_NAME rep:principalName}
 * properties defined with ACE nodes.
 */
class AccessControlInitializer implements WorkspaceInitializer, AccessControlConstants {

    @Nonnull
    @Override
    public NodeState initialize(NodeState workspaceRoot, String workspaceName, QueryIndexProvider indexProvider, CommitHook commitHook) {
        NodeBuilder root = workspaceRoot.builder();

        // property index for rep:principalName stored in ACEs
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(root);
        if (!index.hasChildNode("acPrincipalName")) {
            IndexUtils.createIndexDefinition(index, "acPrincipalName", true, false,
                    ImmutableList.<String>of(REP_PRINCIPAL_NAME),
                    ImmutableList.<String>of(NT_REP_DENY_ACE, NT_REP_GRANT_ACE, NT_REP_ACE));
        }
        return root.getNodeState();
    }
}
