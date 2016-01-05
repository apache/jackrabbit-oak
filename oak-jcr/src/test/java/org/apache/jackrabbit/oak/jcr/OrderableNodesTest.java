/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.commons.iterator.NodeIterable;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

public class OrderableNodesTest extends AbstractRepositoryTest {

    public OrderableNodesTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testSimpleOrdering() throws RepositoryException {
        doTest("nt:unstructured");
    }

    @Test
    public void orderableFolder() throws Exception {
        // check ordering with node type without a residual properties definition
        new TestContentLoader().loadTestContent(getAdminSession());
        doTest("test:orderableFolder");
    }

    @Test
    public void orderSameNode() throws Exception {
        Session session = getAdminSession();
        Node n = session.getRootNode().addNode("test", "nt:unstructured");
        Node a = n.addNode("a");

        n.orderBefore("a", "a");
    }

    @Test
    public void setPrimaryType() throws Exception {
        new TestContentLoader().loadTestContent(getAdminSession());
        // start with a node without orderable nodes
        Session session = getAdminSession();
        Node root = session.getRootNode().addNode("test", "nt:folder");

        List<String> names = new ArrayList<String>();
        for (int i = 0; i < 100; i++) {
            String name = "node-" + i;
            root.addNode(name, "nt:folder");
            names.add(name);
        }

        root.setPrimaryType("test:orderableFolder");

        // as of now, the child nodes must be stable and orderable
        List<String> expected = getChildNames(root);
        while (!expected.isEmpty()) {
            String name = expected.remove((int) Math.floor(Math.random() * expected.size()));
            root.getNode(name).remove();

            assertEquals(expected, getChildNames(root));
        }

        for (String name : names) {
            root.addNode(name, "nt:folder");
            expected.add(name);
            assertEquals(expected, getChildNames(root));
        }
    }

    /**
     * OAK-612
     */
    @Test
    public void testAddNode() throws Exception {
        new TestContentLoader().loadTestContent(getAdminSession());

        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test", "test:orderableFolder");
        assertTrue(test.getPrimaryNodeType().hasOrderableChildNodes());

        test.addNode("a");
        test.addNode("b");
        session.save();

        NodeIterator it = test.getNodes();
        assertEquals("a", it.nextNode().getName());
        assertEquals("b", it.nextNode().getName());
    }

    private void doTest(String nodeType) throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode().addNode("test", nodeType);

        root.addNode("a");
        root.addNode("b");
        root.addNode("c");

        NodeIterator iterator;

        iterator = root.getNodes();
        assertEquals("a", iterator.nextNode().getName());
        assertEquals("b", iterator.nextNode().getName());
        assertEquals("c", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());

        root.orderBefore("c", "a");
        iterator = root.getNodes();
        assertEquals("c", iterator.nextNode().getName());
        assertEquals("a", iterator.nextNode().getName());
        assertEquals("b", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());

        root.orderBefore("b", "c");
        iterator = root.getNodes();
        assertEquals("b", iterator.nextNode().getName());
        assertEquals("c", iterator.nextNode().getName());
        assertEquals("a", iterator.nextNode().getName());
        assertFalse(iterator.hasNext());

        session.save();
    }

    private static List<String> getChildNames(Node node)
            throws RepositoryException {
        List<String> names = new ArrayList<String>();
        for (Node child : new NodeIterable(node.getNodes())) {
            names.add(child.getName());
        }
        return names;
    }
}
