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

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

public class OrderableNodesTest extends AbstractRepositoryTest {

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

    @Ignore("OAK-612")
    @Test
    public void testAddNode() throws Exception {
        new TestContentLoader().loadTestContent(getAdminSession());

        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test", "test:orderableFolder");
        assertTrue(test.getPrimaryNodeType().hasOrderableChildNodes());

        Node n1 = test.addNode("a");
        Node n2 = test.addNode("b");
        session.save();

        NodeIterator it = test.getNodes();
        assertEquals("a", it.nextNode().getName());
        assertEquals("b", it.nextNode().getName());
    }

    @Ignore("OAK-612")
    @Test
    public void testAddNode2() throws Exception {
        new TestContentLoader().loadTestContent(getAdminSession());

        Session session = getAdminSession();
        Node test = session.getRootNode().addNode("test", "test:orderableFolder");
        Node n1 = test.addNode("a");
        Node n2 = test.addNode("b");
        session.save();

        test.getNode("a").remove();
        test.addNode("a");
        session.save();

        NodeIterator it = test.getNodes();
        assertEquals("b", it.nextNode().getName());
        assertEquals("a", it.nextNode().getName());
    }

    private void doTest(String nodeType) throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode().addNode("test", nodeType);

        root.addNode("a");
        root.addNode("b");
        root.addNode("c");

        NodeIterator iterator;

        root.orderBefore("a", "b");
        root.orderBefore("c", null);
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
}
