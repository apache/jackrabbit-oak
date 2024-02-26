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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test nodes with many child nodes.
 */
public class ManyChildrenIT extends AbstractRepositoryTest {

    public ManyChildrenIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void before() throws RepositoryException {
        //make sure that the repository is initialized
        getRepository();
        Tracker<FeatureToggle> toggleTracker = getWhiteboard().track(FeatureToggle.class);
        for (FeatureToggle ft : toggleTracker.getServices()) {
            if ("FT_COCLEANUP_OAK-10657".equals(ft.getName())) {
                ft.setEnabled(true);
            }
        }
    }

    @Test
    public void sizeTest() throws Exception {
        Session session = getAdminSession();
        int count = 5000;
        String nodeType = "nt:folder";
        Node many = session.getRootNode().addNode("many", nodeType);
        for (int i = 0; i < count; i++) {
            many.addNode("test" + i, nodeType);
        }
        session.save();
        Session session2 = getAdminSession();
        many = session2.getRootNode().getNode("many");
        NodeIterator it = many.getNodes();
        long size = it.getSize();
        assertTrue("size: " + size, size == -1 || size == count);
        for (int i = 0; i < count; i++) {
            assertTrue(it.hasNext());
            it.next();
        }
        assertFalse(it.hasNext());
        session2.logout();
    }

    @Test
    public void addRemoveNodes() throws Exception {
        int numNodes = 101;
        Session writer = getAdminSession();
        Node test = writer.getRootNode().addNode("test", "nt:unstructured");
        for (int i = 0; i < numNodes; i++) {
            test.addNode("node-" + i, "nt:unstructured");
        }
        writer.save();
        for (int i = 0; i < numNodes; i++) {
            if (i % 2 == 0) {
                test.getNode("node-" + i).remove();
            }
        }
        writer.save();
        test.addNode("node-x");
        writer.save();
        assertTrue(test.hasNode("node-x"));
    }

    @Test
    // @Ignore //OAK-10646
    public void orderableAddManyChildrenWithSave() throws Exception {
        int childCount = 1000;
        StringBuilder prefix = new StringBuilder("");
        for (int k = 0; k < 90; k++) {
            prefix.append("0123456789");
        }
        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test", "nt:unstructured");
        session.save();
        for (int k = 0; k < childCount; k++) {
            test.addNode(prefix.toString() + k, "nt:unstructured");
        }
    }

    @Test
    // @Ignore //OAK-10646
    public void moveOrderableWithManyChildren() throws Exception {
        int childCount = 1000;
        StringBuilder prefix = new StringBuilder("");
        for (int k = 0; k < 90; k++) {
            prefix.append("0123456789");
        }
        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test-0", "nt:unstructured");
        session.save();
        for (int k = 0; k < childCount; k++) {
            test.addNode(prefix.toString() + k, "nt:unstructured");
            if (k % 100 == 0) {
                session.save();
            }
        }
        session.save();
        session.move("/test-0", "/test-1");
        session.save();
    }

    @Test
    // @Ignore //OAK-10646
    public void copyOrderableWithManyChildren() throws Exception {
        int childCount = 1000;
        StringBuilder prefix = new StringBuilder("");
        for (int k = 0; k < 90; k++) {
            prefix.append("0123456789");
        }
        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test-0", "nt:unstructured");
        session.save();
        for (int k = 0; k < childCount; k++) {
            test.addNode(prefix.toString() + k, "nt:unstructured");
            if (k % 100 == 0) {
                session.save();
            }
        }
        session.save();
        session.getWorkspace().copy("/test-0", "/test-1");
        session.save();
    }
}
