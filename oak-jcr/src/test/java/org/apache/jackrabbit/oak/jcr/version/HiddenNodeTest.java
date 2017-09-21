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
package org.apache.jackrabbit.oak.jcr.version;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Checks if hidden nodes are properly handled on checkin and restore (OAK-1219, OAK-OAK-1226).
 */
public class HiddenNodeTest {

    private NodeStore store;
    private Repository repo;
    private Session session;
    private VersionManager vMgr;

    @Before
    public void before() throws Exception {
        store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        repo = new Jcr(store).createRepository();
        session = repo.login(new SimpleCredentials("admin", "admin".toCharArray()));
        vMgr = session.getWorkspace().getVersionManager();
    }

    @After
    public void after() {
        if (session != null) {
            session.logout();
            session = null;
        }
        repo = dispose(repo);
    }

    @Test
    public void hiddenTree() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        test.addMixin("mix:versionable");
        session.save();

        NodeBuilder builder = store.getRoot().builder();
        builder.child("test").child(":hidden").setProperty("property", "value");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode("jcr:frozenNode");
        assertTrue(state.exists());
        assertFalse(state.hasChildNode(":hidden"));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test");
        assertTrue(state.hasChildNode(":hidden"));
    }

    @Test
    public void hiddenProperty() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        test.addMixin("mix:versionable");
        session.save();

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder testBuilder = builder.getChildNode("test");
        testBuilder.setProperty(":hiddenProperty", "value");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode(VersionConstants.JCR_FROZENNODE);
        assertTrue(state.exists());
        assertFalse(state.hasProperty(":hiddenProperty"));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test");
        assertFalse(state.hasProperty(":hiddenProperty"));
    }

    @Test
    public void hiddenOrderProperty() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        test.addNode("a");
        test.addNode("b");
        test.addNode("c");
        test.addMixin("mix:versionable");
        session.save();

        NodeBuilder testBuilder = store.getRoot().builder().getChildNode("test");
        assertTrue(testBuilder.hasProperty(TreeConstants.OAK_CHILD_ORDER));

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode(VersionConstants.JCR_FROZENNODE);
        assertTrue(state.exists());
        assertTrue(state.hasProperty(TreeConstants.OAK_CHILD_ORDER));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test");
        assertTrue(state.hasProperty(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void hiddenChildNode() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        test.addMixin("mix:versionable");
        test.addNode("child");
        session.save();

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder testBuilder = builder.getChildNode("test").getChildNode("child");
        testBuilder.child(":hidden").setProperty("property", "value");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode("jcr:frozenNode").getChildNode("child");
        assertTrue(state.exists());
        assertFalse(state.hasChildNode(":hidden"));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test").getChildNode("child");
        assertFalse(state.hasChildNode(":hidden"));
    }

    @Test
    public void hiddenChildProperty() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        test.addMixin("mix:versionable");
        test.addNode("child");
        session.save();

        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder testBuilder = builder.getChildNode("test").getChildNode("child");
        testBuilder.setProperty(":hiddenProperty", "value");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode(VersionConstants.JCR_FROZENNODE).getChildNode("child");
        assertTrue(state.exists());
        assertFalse(state.hasProperty(":hiddenProperty"));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test").getChildNode("child");
        assertFalse(state.hasProperty(":hiddenProperty"));
    }

    @Test
    public void hiddenChildOrderProperty() throws Exception {
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        Node child = test.addNode("child");
        child.addNode("a");
        child.addNode("b");
        child.addNode("c");
        test.addMixin("mix:versionable");
        session.save();

        NodeBuilder testBuilder = store.getRoot().builder().getChildNode("test").getChildNode("child");
        assertTrue(testBuilder.hasProperty(TreeConstants.OAK_CHILD_ORDER));

        session.refresh(false);
        Version v1 = vMgr.checkpoint("/test");
        Version v2 = vMgr.checkpoint("/test");

        NodeState state = store.getRoot();
        for (String name : PathUtils.elements(v2.getPath())) {
            state = state.getChildNode(name);
        }
        state = state.getChildNode(VersionConstants.JCR_FROZENNODE).getChildNode("child");
        assertTrue(state.exists());
        assertTrue(state.hasProperty(TreeConstants.OAK_CHILD_ORDER));

        vMgr.restore(v1, true);

        state = store.getRoot().getChildNode("test").getChildNode("child");
        assertTrue(state.hasProperty(TreeConstants.OAK_CHILD_ORDER));
    }
}