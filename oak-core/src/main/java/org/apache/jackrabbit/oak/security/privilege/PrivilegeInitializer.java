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
package org.apache.jackrabbit.oak.security.privilege;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;

/**
 * {@code RepositoryInitializer} that asserts the existence and node type of
 * the /jcr:system/jcr:privileges node that is used to store privilege definitions.
 * In addition it writes all built-in privilege definitions except jcr:all to
 * the repository.
 */
class PrivilegeInitializer implements RepositoryInitializer, PrivilegeConstants {

    private static final Logger log = LoggerFactory.getLogger(PrivilegeInitializer.class);

    private final RootProvider rootProvider;

    PrivilegeInitializer(@Nonnull RootProvider rootProvider) {
        this.rootProvider = rootProvider;
    }

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        NodeBuilder system = builder.child(JcrConstants.JCR_SYSTEM);
        system.setProperty(JcrConstants.JCR_PRIMARYTYPE, NodeTypeConstants.NT_REP_SYSTEM, Type.NAME);

        if (!system.hasChildNode(REP_PRIVILEGES)) {
            NodeBuilder privileges = system.child(REP_PRIVILEGES);
            privileges.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_PRIVILEGES, Type.NAME);

            // squeeze node state before it is passed to store (OAK-2411)
            NodeState base = squeeze(builder.getNodeState());
            NodeStore store = new MemoryNodeStore(base);
            try {
                Root systemRoot = rootProvider.createSystemRoot(store, null);
                new PrivilegeDefinitionWriter(systemRoot).writeBuiltInDefinitions();
            } catch (RepositoryException e) {
                log.error("Failed to register built-in privileges", e);
                throw new RuntimeException(e);
            }
            NodeState target = store.getRoot();
            target.compareAgainstBaseState(base, new ApplyDiff(builder));
        }
    }
}
