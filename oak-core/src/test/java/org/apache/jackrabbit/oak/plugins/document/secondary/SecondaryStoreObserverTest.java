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

package org.apache.jackrabbit.oak.plugins.document.secondary;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractDocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeStateDiffer;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SecondaryStoreObserverTest {
    private final List<String> empty = Collections.emptyList();

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore primary;
    private NodeStore secondary;

    @Before
    public void setUp() throws IOException {
        primary = builderProvider.newBuilder().getNodeStore();
        secondary = new MemoryNodeStore();
    }

    @Test
    public void basicSetup() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreObserver observer = createBuilder(pathFilter).buildObserver();
        primary.addObserver(observer);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        dump(secondaryRoot(), "/a");
        dump(primary.getRoot(), "/a");
        assertEquals(secondaryRoot().getChildNode("a"),
                primary.getRoot().getChildNode("a"));
    }

    @Test
    public void childNodeAdded() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreObserver observer = createBuilder(pathFilter).buildObserver();
        primary.addObserver(observer);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        nb = primary.getRoot().builder();
        create(nb, "/a/d");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertMetaState(primary.getRoot(), secondaryRoot(), "/a/d");
        assertMetaState(primary.getRoot(), secondaryRoot(), "/a");
    }

    @Test
    public void childNodeChangedAndExclude() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), of("a/b"));
        SecondaryStoreObserver observer = createBuilder(pathFilter).buildObserver();
        primary.addObserver(observer);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        nb = primary.getRoot().builder();
        create(nb, "/a/d", "/a/b/e");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertMetaState(primary.getRoot(), secondaryRoot(), "/a/d");
    }

    @Test
    public void childNodeDeleted() throws Exception{
        PathFilter pathFilter = new PathFilter(of("/a"), empty);
        SecondaryStoreObserver observer = createBuilder(pathFilter).buildObserver();
        primary.addObserver(observer);

        NodeBuilder nb = primary.getRoot().builder();
        create(nb, "/a/b", "/a/c", "/x/y/z");
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        nb = primary.getRoot().builder();
        nb.child("a").child("c").remove();
        primary.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(NodeStateUtils.getNode(secondaryRoot(), "/a/c").exists());
    }

    private NodeState secondaryRoot() {
        return DelegatingDocumentNodeState.wrap(secondary.getRoot(), NodeStateDiffer.DEFAULT_DIFFER);
    }

    private SecondaryStoreBuilder createBuilder(PathFilter pathFilter) {
        return new SecondaryStoreBuilder(secondary).pathFilter(pathFilter);
    }


    private static void assertMetaState(NodeState root1, NodeState root2, String path){
        assertMetaState(documentState(root1, path), documentState(root2, path));
    }

    private static void assertMetaState(AbstractDocumentNodeState a, AbstractDocumentNodeState b){
        assertEquals(a.getLastRevision(), b.getLastRevision());
        assertEquals(a.getRootRevision(), b.getRootRevision());
        assertEquals(a.getPath(), b.getPath());
    }

    static AbstractDocumentNodeState documentState(NodeState root, String path){
        return (AbstractDocumentNodeState) NodeStateUtils.getNode(root, path);
    }

    private static void dump(NodeState root, String path){
        NodeState state = NodeStateUtils.getNode(root, path);
        System.out.println(NodeStateUtils.toString(state));
    }

    static NodeState create(NodeBuilder b, String ... paths){
        for (String path : paths){
            NodeBuilder cb = b;
            for (String pathElement : PathUtils.elements(path)){
                cb = cb.child(pathElement);
            }
        }
        return b.getNodeState();
    }

}