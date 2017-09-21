/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.BUNDLOR;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler.DOCUMENT_NODE_STORE;

public enum BundlingConfigInitializer implements RepositoryInitializer {
    INSTANCE;

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        if (builder.hasChildNode(JCR_SYSTEM)){
            NodeBuilder system = builder.getChildNode(JCR_SYSTEM);

            if (!system.hasChildNode(DOCUMENT_NODE_STORE)){
                NodeBuilder dns = system.child(DOCUMENT_NODE_STORE);
                dns.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);

                NodeState registryState = BundledTypesRegistry.builder()
                        .forType("nt:file", "jcr:content")
                        .build();
                NodeBuilder bundlor = dns.setChildNode(BUNDLOR, registryState);
                bundlor.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
            }
        }

    }
}
