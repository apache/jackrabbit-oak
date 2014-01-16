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

import java.io.InputStreamReader;
import java.io.Reader;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.observation.Event;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitNode;
import org.apache.jackrabbit.commons.cnd.CndImporter;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.api.observation.EventResult;

/**
 * JackrabbitNodeTest: Copied and slightly adjusted from org.apache.jackrabbit.api.JackrabbitNodeTest,
 * which used to create SNS for this test.
 */
public class JackrabbitNodeTest extends AbstractJCRTest {

    static final String SEQ_BEFORE = "abcdefghij";
    static final String SEQ_AFTER =  "abcdefGhij";
    static final int RELPOS = 6;

    static final String TEST_NODETYPES = "org/apache/jackrabbit/oak/jcr/test_mixin_nodetypes.cnd";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        assertTrue(testRootNode.getPrimaryNodeType().hasOrderableChildNodes());
        for (char c : SEQ_BEFORE.toCharArray()) {
            testRootNode.addNode(new String(new char[]{c}));
        }
        superuser.save();

        Reader cnd = new InputStreamReader(getClass().getClassLoader().getResourceAsStream(TEST_NODETYPES));
        CndImporter.registerNodeTypes(cnd, superuser);
        cnd.close();
    }

    public void testRename() throws RepositoryException {
        Node renamedNode = null;
        NodeIterator it = testRootNode.getNodes();
        int pos = 0;
        while (it.hasNext()) {
            Node n = it.nextNode();
            String name = n.getName();
            assertEquals(new String(new char[]{SEQ_BEFORE.charAt(pos)}), name);
            if (pos == RELPOS) {
                JackrabbitNode node = (JackrabbitNode) n;
                node.rename(name.toUpperCase());
                renamedNode = n;
            }
            pos++;
        }

        it = testRootNode.getNodes();
        pos = 0;
        while (it.hasNext()) {
            Node n = it.nextNode();
            String name = n.getName();
            assertEquals(new String(new char[]{SEQ_AFTER.charAt(pos)}), name);
            if (pos == RELPOS) {
                assertTrue(n.isSame(renamedNode));
            }
            pos++;
        }
    }

    public void testRenameEventHandling() throws RepositoryException {
        Session s = getHelper().getSuperuserSession();
        ObservationManager mgr = s.getWorkspace().getObservationManager();
        EventResult result = new EventResult(log);

        try {
            mgr.addEventListener(result, Event.PERSIST|Event.NODE_ADDED|Event.NODE_MOVED|Event.NODE_REMOVED, testRootNode.getPath(), true, null, null, false);

            NodeIterator it = testRootNode.getNodes();

            Node n = it.nextNode();
            String name = n.getName();

            JackrabbitNode node = (JackrabbitNode) n;
            node.rename(name.toUpperCase());
            superuser.save();

            boolean foundMove = false;
            for (Event event : result.getEvents(5000)) {
                if (Event.NODE_MOVED == event.getType()) {
                    foundMove = true;
                    break;
                }
            }

            if (!foundMove) {
                fail("Expected NODE_MOVED event upon renaming a node.");
            }
        } finally {
            mgr.removeEventListener(result);
            s.logout();
        }
    }

    /**
     * @since oak 1.0
     */
    public void testSetNewMixins() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        ((JackrabbitNode) n).setMixins(new String[]{"test:AA", "test:A"});
        superuser.save();

        assertTrue(n.isNodeType("test:AA"));
        assertTrue(n.isNodeType("test:A"));
        assertTrue(n.hasProperty(JcrConstants.JCR_MIXINTYPES));
    }

    /**
     * @since oak 1.0
     */
    public void testSetNewMixins2() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        ((JackrabbitNode) n).setMixins(new String[]{"test:A", "test:AA"});
        superuser.save();

        assertTrue(n.isNodeType("test:A"));
        assertTrue(n.isNodeType("test:AA"));
        assertTrue(n.hasProperty(JcrConstants.JCR_MIXINTYPES));
    }

    /**
     * @since oak 1.0
     */
    public void testSetEmptyMixins() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        n.addMixin("test:AA");
        superuser.save();

        ((JackrabbitNode) n).setMixins(new String[0]);
        superuser.save();

        assertFalse(n.isNodeType("test:AA"));
        assertTrue(n.hasProperty(JcrConstants.JCR_MIXINTYPES));
        assertEquals(0, n.getProperty(JcrConstants.JCR_MIXINTYPES).getValues().length);
    }

    /**
     * @since oak 1.0
     */
    public void testSetRemoveMixins() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        ((JackrabbitNode) n).setMixins(new String[]{"test:A", "test:AA"});
        superuser.save();

        ((JackrabbitNode) n).setMixins(new String[]{"test:A"});
        superuser.save();

        assertTrue(n.isNodeType("test:A"));
        assertFalse(n.isNodeType("test:AA"));
    }

    /**
     * @since oak 1.0
     */
    public void testUpdateMixins() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        ((JackrabbitNode) n).setMixins(new String[]{"test:A", "test:AA"});
        superuser.save();

        assertTrue(n.isNodeType("test:AA"));
        assertTrue(n.isNodeType("test:A"));

        ((JackrabbitNode) n).setMixins(new String[]{"test:A", "test:AA", JcrConstants.MIX_REFERENCEABLE});
        superuser.save();

        assertTrue(n.isNodeType("test:AA"));
        assertTrue(n.isNodeType("test:A"));
        assertTrue(n.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertTrue(n.hasProperty(JcrConstants.JCR_UUID));

        ((JackrabbitNode) n).setMixins(new String[]{JcrConstants.MIX_REFERENCEABLE});
        superuser.save();

        assertFalse(n.isNodeType("test:AA"));
        assertFalse(n.isNodeType("test:A"));
        assertTrue(n.isNodeType(JcrConstants.MIX_REFERENCEABLE));
        assertTrue(n.hasProperty(JcrConstants.JCR_UUID));
    }

    public void testSetMixins() throws RepositoryException {
        // create node with mixin test:AA
        Node n = testRootNode.addNode("foo", "nt:folder");
        n.addMixin("test:AA");
        n.setProperty("test:propAA", "AA");
        n.setProperty("test:propA", "A");
        superuser.save();

        // 'downgrade' from test:AA to test:A
        ((JackrabbitNode) n).setMixins(new String[]{"test:A"});
        superuser.save();

        assertTrue(n.hasProperty("test:propA"));
        assertFalse(n.hasProperty("test:propAA"));

        // 'upgrade' from test:A to test:AA
        ((JackrabbitNode) n).setMixins(new String[]{"test:AA"});
        n.setProperty("test:propAA", "AA");
        superuser.save();

        assertTrue(n.hasProperty("test:propA"));
        assertTrue(n.hasProperty("test:propAA"));

        // replace test:AA with mix:title
        ((JackrabbitNode) n).setMixins(new String[]{"mix:title"});
        n.setProperty("jcr:title", "...");
        n.setProperty("jcr:description", "blah blah");
        superuser.save();

        assertTrue(n.hasProperty("jcr:title"));
        assertTrue(n.hasProperty("jcr:description"));
        assertFalse(n.hasProperty("test:propA"));
        assertFalse(n.hasProperty("test:propAA"));

        // clean up
        n.remove();
        superuser.save();
    }
}
