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
package org.apache.jackrabbit.oak.plugins.nodetype.write;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@code InitialContent} implements a {@link RepositoryInitializer} and
 * registers built-in node types when the micro kernel becomes available.
 */
public class InitialContent implements RepositoryInitializer, NodeTypeConstants {

    public static final NodeState INITIAL_CONTENT = createInitialContent();

    private static NodeState createInitialContent() {
        NodeBuilder builder = EMPTY_NODE.builder();
        new InitialContent().initialize(builder);
        return ModifiedNodeState.squeeze(builder.getNodeState());
    }

    @Override
    public void initialize(NodeBuilder builder) {
        builder.setProperty(JCR_PRIMARYTYPE, NT_REP_ROOT, Type.NAME);

        if (!builder.hasChildNode(JCR_SYSTEM)) {
            NodeBuilder system = builder.child(JCR_SYSTEM);
            system.setProperty(JCR_PRIMARYTYPE, NT_REP_SYSTEM, Type.NAME);

            system.child(JCR_VERSIONSTORAGE)
                    .setProperty(JCR_PRIMARYTYPE, VersionConstants.REP_VERSIONSTORAGE, Type.NAME);
            system.child(JCR_NODE_TYPES)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_NODE_TYPES, Type.NAME);
            system.child(VersionConstants.JCR_ACTIVITIES)
                    .setProperty(JCR_PRIMARYTYPE, VersionConstants.REP_ACTIVITIES, Type.NAME);

            Namespaces.setupNamespaces(system);
        }

        if (!builder.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

            IndexUtils.createIndexDefinition(index, "uuid", true, true,
                    ImmutableList.<String>of(JCR_UUID), null);
            NodeBuilder nt = 
                    IndexUtils.createIndexDefinition(index, "nodetype", true, false,
                    ImmutableList.of(JCR_PRIMARYTYPE, JCR_MIXINTYPES), null);
            // the cost of using the property index for "@primaryType is not null" is very high
            nt.setProperty(IndexConstants.ENTRY_COUNT_PROPERTY_NAME, Long.valueOf(Long.MAX_VALUE));
            IndexUtils.createReferenceIndex(index);
        }

        // squeeze node state before it is passed to store (OAK-2411)
        NodeState base = ModifiedNodeState.squeeze(builder.getNodeState());
        NodeStore store = new MemoryNodeStore(base);
        BuiltInNodeTypes.register(new SystemRoot(
                store, new EditorHook(new CompositeEditorProvider(
                        new NamespaceEditorProvider(),
                        new TypeEditorProvider()))));
        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }

}
