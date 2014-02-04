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

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checks if hidden nodes are properly handled on checkin and restore (OAK-1219).
 */
public class HiddenNodeTest {

    private NodeStore store;
    private Repository repo;
    private Session session;
    private VersionManager vMgr;

    @Before
    public void before() throws Exception {
        store = new SegmentNodeStore(new MemoryStore());
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
        store.merge(builder, EmptyHook.INSTANCE, null);

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
}
