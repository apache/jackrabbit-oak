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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.OakInitializer;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class TreeStoreTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void convertPathTest() {
        assertEquals("\t", TreeStore.toChildNodeEntry("/"));
        assertEquals("/\tabc", TreeStore.toChildNodeEntry("/abc"));
        assertEquals("/hello\tworld", TreeStore.toChildNodeEntry("/hello/world"));

        assertEquals("/\tabc", TreeStore.toChildNodeEntry("/", "abc"));
        assertEquals("/hello\tworld", TreeStore.toChildNodeEntry("/hello", "world"));
    }

    @Test
    public void convertPathAndReverseTest() {
        // normal case
        String path = "/hello";
        String childNodeName = "world";
        String key = TreeStore.toChildNodeEntry(path, childNodeName);
        assertEquals("/hello\tworld", key);
        String[] parts = TreeStore.toParentAndChildNodeName(key);
        assertEquals(path, parts[0]);
        assertEquals(childNodeName, parts[1]);

        // root node
        path = "/";
        childNodeName = "test";
        key = TreeStore.toChildNodeEntry(path, childNodeName);
        parts = TreeStore.toParentAndChildNodeName(key);
        assertEquals(path, parts[0]);
        assertEquals(childNodeName, parts[1]);

        // failure case
        key = "/";
        try {
            parts = TreeStore.toParentAndChildNodeName(key);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void buildAndIterateTest() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder, null, 1);
        try {
            store.getSession().init();
            store.putNode("/", "{}");
            store.putNode("/content", "{}");
            Iterator<String> it = store.iteratorOverPaths();
            assertEquals("/", it.next());
            assertEquals("/content", it.next());
            assertFalse(it.hasNext());
        } finally {
            store.close();
        }
    }

    @Test
    public void updateStoreTest() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder, null, 1);
        try {
            store.getSession().init();
            store.putNode("/", "{}");
            store.putNode("/content", "{}");
            store.putNode("/content", "{\"x\":1}");
            store.putNode("/toRemove", "{}");
            store.removeNode("/toRemove");
            store.removeNode("/toRemoveNotExisting");
            Iterator<String> it = store.iteratorOverPaths();
            assertEquals("/", it.next());
            assertEquals("/content", it.next());
            assertEquals("{\"x\":1}", store.getSession().get("/content"));
            assertFalse(it.hasNext());
        } finally {
            store.close();
        }
    }

    @Test
    public void tryUpdateReadOnlyPackFile() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder, null, 1);
        try {
            store.getSession().init();
            store.putNode("/", "{}");
            store.putNode("/content", "{}");
            try {
                store.putNode("/new", null);
                fail();
            } catch (IllegalStateException e) {
                // expected
            }
        } finally {
            store.close();
        }
        File packFile = temporaryFolder.newFile();
        FilePacker.pack(testFolder, TreeSession.getFileNameRegex(), packFile, true);
        TreeStore readOnly = new TreeStore("test", packFile, null, 1);
        try {
            assertEquals("{}", store.getSession().get("/content"));
            try {
                readOnly.removeNode("/content");
                fail();
            } catch (IllegalStateException e) {
                // expected
            }
            try {
                readOnly.putNode("/new", "{}");
                fail();
            } catch (IllegalStateException e) {
                // expected
            }
            Iterator<String> it = readOnly.iteratorOverPaths();
            assertEquals("/", it.next());
            assertEquals("/content", it.next());
            assertEquals("{}", store.getSession().get("/content"));
            assertFalse(it.hasNext());
        } finally {
            readOnly.close();
        }
    }

    @Test
    public void includedPathTest() throws IOException {
        File testFolder = temporaryFolder.newFolder();
        TreeStore store = new TreeStore("test", testFolder, null, 1);
        try {
            store.getSession().init();
            store.putNode("/", "{}");
            store.putNode("/content", "{}");
            store.putNode("/content/abc", "{}");
            store.putNode("/jcr:system", "{}");
            store.putNode("/jcr:system/jcr:version", "{}");
            store.putNode("/var/abc", "{}");
            store.putNode("/var/def", "{}");
            store.putNode("/zero", "{}");

            Set<IndexDefinition> defs = inMemoryIndexDefinitions("/content", "/var", "/tmp");
            store.setIndexDefinitions(defs);

            Iterator<String> it = store.iteratorOverPaths();
            assertEquals("/content", it.next());
            assertEquals("/content/abc", it.next());
            assertEquals("/var/abc", it.next());
            assertEquals("/var/def", it.next());
            assertFalse(it.hasNext());
        } finally {
            store.close();
        }
    }

    private static Set<IndexDefinition> inMemoryIndexDefinitions(String... includedPaths) {
        NodeStore store = new MemoryNodeStore();
        EditorHook hook = new EditorHook(
                new CompositeEditorProvider(new NamespaceEditorProvider(), new TypeEditorProvider()));
        OakInitializer.initialize(store, new InitialContent(), hook);

        Set<IndexDefinition> defns = new HashSet<>();
        IndexDefinitionBuilder defnBuilder = new IndexDefinitionBuilder();
        defnBuilder.includedPaths(includedPaths);
        defnBuilder.indexRule("dam:Asset");
        defnBuilder.aggregateRule("dam:Asset");
        IndexDefinition defn = IndexDefinition.newBuilder(store.getRoot(), defnBuilder.build(), "/foo").build();
        defns.add(defn);

        NodeTypeInfoProvider mockNodeTypeInfoProvider = Mockito.mock(NodeTypeInfoProvider.class);
        NodeTypeInfo mockNodeTypeInfo = Mockito.mock(NodeTypeInfo.class, "dam:Asset");
        Mockito.when(mockNodeTypeInfo.getNodeTypeName()).thenReturn("dam:Asset");
        Mockito.when(mockNodeTypeInfoProvider.getNodeTypeInfo("dam:Asset")).thenReturn(mockNodeTypeInfo);

        return defns;
    }

}
