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

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import org.apache.jackrabbit.JcrConstants;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CopyTest extends AbstractRepositoryTest {

    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    public CopyTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        ValueFactory valueFactory = session.getValueFactory();
        Node root = session.getRootNode();
        Node foo = root.addNode("foo");
        foo.setProperty("stringProp", "stringVal");
        foo.setProperty("intProp", 42);
        foo.setProperty("mvProp", new Value[]{
                valueFactory.createValue(1),
                valueFactory.createValue(2),
                valueFactory.createValue(3),
        });
        root.addNode("bar");
        root.addNode(TEST_NODE);
        session.save();
    }

    @Test
    public void testCopyNode() throws RepositoryException {
        Session session = getAdminSession();

        Node node = session.getNode(TEST_PATH);
        node.addNode("source").addNode("node");
        node.addNode("target");
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(node.hasNode("source/node"));
        assertTrue(node.hasNode("target/copied"));
    }

    @Ignore("OAK-915") // FIXME
    @Test
    public void testCopyReferenceableNode() throws Exception {
        Session session = getAdminSession();

        Node node = session.getNode(TEST_PATH);
        node.addNode("source").addNode("node").addMixin(JcrConstants.MIX_REFERENCEABLE);
        node.addNode("target");
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(node.hasNode("source/node"));
        assertTrue(node.hasNode("target/copied"));
        Node copy = node.getNode("target/copied");
        assertTrue(copy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(copy.getUUID().equals(node.getNode("source/node").getUUID()));
    }

    @Ignore("OAK-915") // FIXME
    @Test
    public void testCopyReferenceableChildNode() throws Exception {
        Session session = getAdminSession();

        Node node = session.getNode(TEST_PATH);
        node.addNode("source").addNode("node").addNode("child").addMixin(JcrConstants.MIX_REFERENCEABLE);
        node.addNode("target");
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(node.hasNode("source/node"));
        assertTrue(node.hasNode("target/copied"));

        Node childCopy = node.getNode("target/copied/child");
        assertTrue(childCopy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(childCopy.getUUID().equals(node.getNode("source/node/child").getUUID()));
    }
}