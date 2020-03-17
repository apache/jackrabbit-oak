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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Cursor;
import org.apache.jackrabbit.oak.plugins.index.Cursors;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * {@code NodeTypeIndexTest} performs tests on {@link NodeTypeIndex}.
 */
public class NodeTypeIndexTest {

    private NodeStore store = new MemoryNodeStore();

    @Before
    public void setup() {
        // initialize node types & index definitions
        OakInitializer.initialize(store, new InitialContent(),
                CompositeIndexEditorProvider
                        .compose(new ArrayList<IndexEditorProvider>()));
    }

    @Test
    public void nodeType() throws Exception {
        NodeBuilder root = store.getRoot().builder();

        // remove "rep:security" as it interferes with tests
        root.getChildNode("rep:security").remove();
        
        // set "entryCount", so the node type index counts the nodes
        // and the approximation is not used
        root.getChildNode("oak:index").getChildNode("nodetype").setProperty("entryCount", -1);

        addFolder(root, "folder-1");
        addFolder(root, "folder-2");
        addFile(root, "file-1");

        store.merge(root, new EditorHook(new IndexUpdateProvider(
                new PropertyIndexEditorProvider())), CommitInfo.EMPTY);

        NodeState rootState = store.getRoot();
        NodeTypeIndex index = new NodeTypeIndex(
                Mounts.defaultMountInfoProvider());
        FilterImpl filter;

        filter = createFilter(rootState, JcrConstants.NT_FOLDER);
        assertEquals(6.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/folder-1", "/folder-2");

        filter = createFilter(rootState, JcrConstants.NT_FILE);
        assertEquals(5.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/file-1");

        filter = createFilter(rootState, JcrConstants.NT_HIERARCHYNODE);
        assertEquals(7.0, index.getCost(filter, rootState), 0.0);
        checkCursor(index.query(filter, rootState), "/folder-1", "/folder-2", "/file-1");
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);        
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

    private static void checkCursor(Cursor cursor, String... matches) {
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

    private static NodeBuilder addFolder(NodeBuilder node, String name) {
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

    private static NodeBuilder addChild(NodeBuilder node, String name, String nodeType) {
        return node.child(name).setProperty(
                JcrConstants.JCR_PRIMARYTYPE, nodeType, Type.NAME);
    }

}
