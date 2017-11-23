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
package org.apache.jackrabbit.oak;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.squeeze;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;

import com.google.common.collect.ImmutableList;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@code InitialContent} implements a {@link RepositoryInitializer} the creates
 * the initial JCR/Oak repository structure. This includes creating
 *
 * <pre>
 * - the root node
 * - jcr:system node and it subtree
 *      - version storage
 *      - activities
 *      - built-in node types
 *      - built-in namespaces
 * - some basic index definitions required for a functional JCR repository
 * </pre>
 */
public class InitialContent implements RepositoryInitializer, NodeTypeConstants {

    public static final NodeState INITIAL_CONTENT = createInitialContent();

    private static NodeState createInitialContent() {
        NodeBuilder builder = EMPTY_NODE.builder();
        new InitialContent().initialize(builder);
        return squeeze(builder.getNodeState());
    }

    /**
     * Whether to pre-populate the version store with intermediate nodes.
     */
    private boolean prePopulateVS = false;

    @Override
    public void initialize(@Nonnull NodeBuilder builder) {
        builder.setProperty(JCR_PRIMARYTYPE, NT_REP_ROOT, Type.NAME);

        if (!builder.hasChildNode(JCR_SYSTEM)) {
            NodeBuilder system = builder.child(JCR_SYSTEM);
            system.setProperty(JCR_PRIMARYTYPE, NT_REP_SYSTEM, Type.NAME);

            system.child(JCR_VERSIONSTORAGE)
                    .setProperty(JCR_PRIMARYTYPE, REP_VERSIONSTORAGE, Type.NAME);
            system.child(JCR_NODE_TYPES)
                    .setProperty(JCR_PRIMARYTYPE, NT_REP_NODE_TYPES, Type.NAME);
            system.child(VersionConstants.JCR_ACTIVITIES)
                    .setProperty(JCR_PRIMARYTYPE, VersionConstants.REP_ACTIVITIES, Type.NAME);

            Namespaces.setupNamespaces(system);
        }

        NodeBuilder versionStorage = builder.child(JCR_SYSTEM)
                .child(JCR_VERSIONSTORAGE);
        if (prePopulateVS && !isInitialized(versionStorage)) {
            createIntermediateNodes(versionStorage);
        }

        if (!builder.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)) {
            NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

            NodeBuilder uuid = IndexUtils.createIndexDefinition(index, "uuid", true, true,
                    ImmutableList.<String>of(JCR_UUID), null);
            uuid.setProperty("info",
                    "Oak index for UUID lookup (direct lookup of nodes with the mixin 'mix:referenceable').");
            NodeBuilder nodetype = IndexUtils.createIndexDefinition(index, "nodetype", true, false,
                    ImmutableList.of(JCR_PRIMARYTYPE, JCR_MIXINTYPES), null);
            nodetype.setProperty("info", 
                    "Oak index for queries with node type, and possibly path restrictions, " + 
                    "for example \"/jcr:root/content//element(*, mix:language)\".");
            IndexUtils.createReferenceIndex(index);
            
            index.child("counter")
                    .setProperty(JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                    .setProperty(TYPE_PROPERTY_NAME, NodeCounterEditorProvider.TYPE)
                    .setProperty(IndexConstants.ASYNC_PROPERTY_NAME, 
                            IndexConstants.ASYNC_PROPERTY_NAME)
                    .setProperty("info", "Oak index that allows to estimate " + 
                            "how many nodes are stored below a given path, " + 
                            "to decide whether traversing or using an index is faster.");
        }

        // squeeze node state before it is passed to store (OAK-2411)
        NodeState base = squeeze(builder.getNodeState());
        NodeStore store = new MemoryNodeStore(base);
        registerBuiltIn(RootFactory.createSystemRoot(
                store, new EditorHook(new CompositeEditorProvider(
                        new NamespaceEditorProvider(),
                        new TypeEditorProvider())), null, null, null));
        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }

    /**
     * Instructs the initializer to pre-populate the version store with
     * intermediate nodes.
     *
     * @return this instance.
     */
    public InitialContent withPrePopulatedVersionStore() {
        this.prePopulateVS = true;
        return this;
    }
    
    //--------------------------< internal >------------------------------------
    
    private boolean isInitialized(NodeBuilder versionStorage) {
        PropertyState init = versionStorage.getProperty(":initialized");
        return init != null && init.getValue(Type.LONG) > 0;
    }

    private void createIntermediateNodes(NodeBuilder versionStorage) {
        String fmt = "%02x";
        versionStorage.setProperty(":initialized", 1);
        for (int i = 0; i < 0xff; i++) {
            NodeBuilder c = storageChild(versionStorage, String.format(fmt, i));
            for (int j = 0; j < 0xff; j++) {
                storageChild(c, String.format(fmt, j));
            }
        }
    }
    
    private NodeBuilder storageChild(NodeBuilder node, String name) {
        NodeBuilder c = node.child(name);
        if (!c.hasProperty(JCR_PRIMARYTYPE)) {
            c.setProperty(JCR_PRIMARYTYPE, REP_VERSIONSTORAGE, Type.NAME);
        }
        return c;
    }

    /**
     * Registers built in node types using the given {@link Root}.
     *
     * @param root the {@link Root} instance.
     */
    private static void registerBuiltIn(final Root root) {
        try {
            InputStream stream = InitialContent.class.getResourceAsStream("builtin_nodetypes.cnd");
            try {
                NodeTypeRegistry.register(root, stream, "built-in node types");
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to read built-in node types", e);
        }
    }
}
