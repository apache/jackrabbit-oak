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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.Version;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionIterator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CopyTest extends AbstractRepositoryTest {

    private static final String TEST_NODE = "test_node";
    private static final String TEST_PATH = '/' + TEST_NODE;

    private Node testNode;

    public CopyTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        testNode = root.addNode(TEST_NODE);
        testNode.addNode("source").addNode("node");
        testNode.addNode("target");
        session.save();
    }

    @After
    public void tearDown() throws RepositoryException {
        Session s = testNode.getSession();
        s.removeItem(TEST_PATH);
        s.save();
    }

    @Test
    public void testCopyNode() throws RepositoryException {
        Session session = getAdminSession();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));
    }

    @Test
    public void testCopyReferenceableNode() throws Exception {
        Session session = getAdminSession();

        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_REFERENCEABLE);
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");
        assertTrue(copy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(copy.getUUID().equals(testNode.getNode("source/node").getUUID()));
    }

    @Test
    public void testCopyReferenceableChildNode() throws Exception {
        Session session = getAdminSession();

        session.getNode(TEST_PATH + "/source/node").addNode("child").addMixin(JcrConstants.MIX_REFERENCEABLE);
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node childCopy = testNode.getNode("target/copied/child");
        assertTrue(childCopy.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertFalse(childCopy.getUUID().equals(testNode.getNode("source/node/child").getUUID()));
    }

    @Test
    public void testCopyVersionableNode() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_VERSIONABLE);
        session.save();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");

        assertTrue(copy.isNodeType(JcrConstants.MIX_VERSIONABLE));
        assertFalse(copy.getVersionHistory().isSame(toCopy.getVersionHistory()));
    }

    @Test
    public void testCopyVersionableNodeClearsVersions() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_VERSIONABLE);
        session.save();

        Version v1 = toCopy.checkin();
        toCopy.checkout();
        Version v2 = toCopy.checkin();
        toCopy.checkout();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        Node copy = testNode.getNode("target/copied");

        VersionHistory vh = copy.getVersionHistory();
        Version rootV = vh.getRootVersion();

        assertEquals(0, rootV.getSuccessors().length);
        VersionIterator vItr = vh.getAllVersions();
        while (vItr.hasNext()) {
            if (!rootV.isSame(vItr.nextVersion())) {
                fail("Unexpected version in version history of copied node.");
            }
        }
        try {
            vh.getVersion(v1.getName());
            fail("Unexpected version in version history of copied node.");
        } catch (VersionException e) {
            // success
        }
        try {
            vh.getVersion(v2.getName());
            fail("Unexpected version in version history of copied node.");
        } catch (VersionException e) {
            // success
        }
    }

    @Test
    public void testCopyVersionableNodeCreatesJcrCopiedFrom() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_VERSIONABLE);
        session.save();

        Version baseV = toCopy.getBaseVersion();

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");

        Node copy = testNode.getNode("target/copied");
        VersionHistory copiedVh = copy.getVersionHistory();

        assertTrue(copiedVh.hasProperty(VersionConstants.JCR_COPIED_FROM));
        Property prop = copiedVh.getProperty(VersionConstants.JCR_COPIED_FROM);
        assertEquals(PropertyType.WEAKREFERENCE, prop.getType());
        Node copiedFrom = prop.getNode();
        assertTrue(baseV.isSame(copiedFrom));
    }

    @Test
    public void testCopyLockedNode() throws Exception {
        Session session = getAdminSession();
        Node toCopy = session.getNode(TEST_PATH + "/source/node");
        toCopy.addMixin(JcrConstants.MIX_LOCKABLE);
        session.save();

        session.getWorkspace().getLockManager().lock(toCopy.getPath(), true, true, Long.MAX_VALUE, "my");
        assertTrue(toCopy.isLocked());
        assertTrue(toCopy.hasProperty(JcrConstants.JCR_LOCKISDEEP));
        assertTrue(toCopy.hasProperty(JcrConstants.JCR_LOCKOWNER));

        session.getWorkspace().copy(TEST_PATH + "/source/node", TEST_PATH + "/target/copied");
        assertTrue(testNode.hasNode("source/node"));
        assertTrue(testNode.hasNode("target/copied"));

        Node copy = testNode.getNode("target/copied");
        assertTrue(copy.isNodeType(JcrConstants.MIX_LOCKABLE));
        assertFalse(copy.isLocked());
        assertFalse(copy.hasProperty(JcrConstants.JCR_LOCKISDEEP));
        assertFalse(copy.hasProperty(JcrConstants.JCR_LOCKOWNER));
    }
}