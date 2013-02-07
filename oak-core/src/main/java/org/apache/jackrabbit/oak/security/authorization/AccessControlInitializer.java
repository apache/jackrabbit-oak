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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;

/**
 * AccessControlInitializer... TODO
 */
public class AccessControlInitializer implements RepositoryInitializer, AccessControlConstants {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(AccessControlInitializer.class);

    @Override
    public void initialize(NodeStore store) {
        NodeStoreBranch branch = store.branch();

        NodeBuilder root = branch.getRoot().builder();
        NodeBuilder system = root.child(JCR_SYSTEM);
        if (!system.hasChildNode(REP_PERMISSION_STORE)) {
            system.child(REP_PERMISSION_STORE)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_PERMISSION_STORE);
        }

        // property index for rep:principalName stored in ACEs
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(root);
        IndexUtils.createIndexDefinition(index, "acPrincipalName", true, false,
                ImmutableList.<String>of(REP_PRINCIPAL_NAME),
                ImmutableList.<String>of(NT_REP_DENY_ACE, NT_REP_GRANT_ACE));
        try {
            branch.setRoot(root.getNodeState());
            branch.merge();
        } catch (CommitFailedException e) {
            log.error("Failed to commit access control index definition", e);
            throw new RuntimeException(e);
        }
    }
}