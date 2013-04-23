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
package org.apache.jackrabbit.oak.plugins.index.nodetype;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.lifecycle.OakInitializer;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.spi.query.Cursors;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertEquals;

/**
 * {@code NodeTypeIndexTest} performs tests on {@link NodeTypeIndex}.
 */
public class NodeTypeIndexTest {

    private KernelNodeStore store;

    @Before
    public void setup() {
        MicroKernel mk = new MicroKernelImpl();
        store = new KernelNodeStore(mk);
        // initialize node types & index definitions
        OakInitializer.initialize(store, new InitialContent(),
                CompositeIndexHookProvider
                        .compose(new ArrayList<IndexHookProvider>()));
    }

    @Test
    public void nodeType() throws Exception {
        NodeStoreBranch branch = store.branch();
        NodeBuilder root = branch.getHead().builder();

        root.removeNode("rep:security"); // interferes with tests
        addFolder(root, "folder-1");
        addFolder(root, "folder-2");
        addFile(root, "file-1");

        branch.setRoot(root.getNodeState());
        branch.merge(new EditorHook(IndexHookManager
                .of(new Property2IndexHookProvider())));

        NodeState rootState = store.getRoot();
        NodeTypeIndex index = new NodeTypeIndex();
        FilterImpl filter;

        filter = createFilter(rootState, JcrConstants.NT_FOLDER);
        assertEquals(2.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/folder-1", "/folder-2");

        filter = createFilter(rootState, JcrConstants.NT_FILE);
        assertEquals(1.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/file-1");

        filter = createFilter(rootState, JcrConstants.NT_HIERARCHYNODE);
        assertEquals(3.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/folder-1", "/folder-2", "/file-1");
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]");
    }

    private void checkCursor(Cursor cursor, String... matches) {
        // make sure the index is actually used
        // and does not traverse
        assertEquals(Cursors.class.getName() + "$PathCursor",
                cursor.getClass().getName());
        Set<String> expected = Sets.newHashSet();
        expected.addAll(Arrays.asList(matches));
        Set<String> actual = Sets.newHashSet();
        while (cursor.hasNext()) {
            actual.add(cursor.next().getPath());
        }
        assertEquals(expected, actual);
    }

    private NodeBuilder addFolder(NodeBuilder node, String name) {
        return addChild(node, name, JcrConstants.NT_FOLDER);
    }

    private NodeBuilder addFile(NodeBuilder node, String name)
            throws IOException {
        NodeBuilder file = addChild(node, name, JcrConstants.NT_FILE);
        NodeBuilder content = addChild(file, JcrConstants.JCR_CONTENT,
                JcrConstants.NT_RESOURCE);
        content.setProperty(JcrConstants.JCR_MIMETYPE, "text/plain");
        Blob blob = store.createBlob(new ByteArrayInputStream("Apache Oak".getBytes()));
        content.setProperty(JcrConstants.JCR_DATA, blob);
        return file;
    }

    private NodeBuilder addChild(NodeBuilder node, String name, String nodeType) {
        return node.child(name).setProperty(
                JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
    }
}
