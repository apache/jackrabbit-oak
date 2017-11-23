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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.api.util.Text;

public class ReadNodeTypeTest extends AbstractEvaluationTest {

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2441">OAK-2441</a>
     */
    public void testNodeGetPrimaryType() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        assertFalse(testSession.propertyExists(path + '/' + JcrConstants.JCR_PRIMARYTYPE));

        Node n = testSession.getNode(path);
        assertFalse(n.hasProperty(JcrConstants.JCR_PRIMARYTYPE));

        NodeType primary = n.getPrimaryNodeType();
        assertEquals(superuser.getNode(path).getPrimaryNodeType().getName(), primary.getName());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2441">OAK-2441</a>
     */
    public void testNodeGetMixinTypes() throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_LOCKABLE);
        superuser.save();

        assertTrue(testSession.propertyExists(path + '/' + JcrConstants.JCR_MIXINTYPES));

        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        assertFalse(testSession.propertyExists(path + '/' + JcrConstants.JCR_MIXINTYPES));
        Node n = testSession.getNode(path);
        assertFalse(n.hasProperty(JcrConstants.JCR_MIXINTYPES));

        int noMixins = superuser.getNode(path).getMixinNodeTypes().length;
        NodeType[] mixins = n.getMixinNodeTypes();
        assertEquals(noMixins, mixins.length);
    }

    /**
     * Verify that transient changes to jcr:mixinTypes are reflected in the
     * API call {@link javax.jcr.Node#getMixinNodeTypes()}.
     */
    public void testNodeGetMixinTypesWithTransientModifications() throws Exception {
        int noMixins = superuser.getNode(path).getMixinNodeTypes().length;

        Node node = superuser.getNode(path);
        node.addMixin(NodeType.MIX_CREATED);

        NodeType[] mixins = node.getMixinNodeTypes();
        assertEquals(noMixins+1, mixins.length);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2488">OAK-2488</a>
     */
    public void testGetPrimaryTypeFromNewNode() throws Exception {
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        testSession.getNode(path).remove();
        Node newNode = testSession.getNode(testRoot).addNode(Text.getName(path));

        if (newNode.hasProperty(JcrConstants.JCR_PRIMARYTYPE)) {
            NodeType primaryType = newNode.getPrimaryNodeType();
            assertEquals(newNode.getProperty(JcrConstants.JCR_PRIMARYTYPE).getString(), primaryType.getName());
        } else {
            try {
                newNode.getPrimaryNodeType();
                fail("Cannot read primary type from transient new node if access to property is not readable.");
            } catch (RepositoryException e) {
                assertTrue(e.getMessage().startsWith("Unable to retrieve primary type for Node"));
            }
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2488">OAK-2488</a>
     */
    public void testGetMixinFromNewNode() throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_LOCKABLE);
        superuser.save();

        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        testSession.getNode(path).remove();
        Node newNode = testSession.getNode(testRoot).addNode(Text.getName(path));

        assertFalse(newNode.hasProperty(JcrConstants.JCR_MIXINTYPES));
        NodeType[] mixins = newNode.getMixinNodeTypes();
        assertEquals(0, mixins.length);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3775">OAK-3775</a>
     */
    public void testIsNodeType() throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_LOCKABLE);
        superuser.save();

        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        Node n = testSession.getNode(path);
        assertFalse(n.hasProperty(JcrConstants.JCR_PRIMARYTYPE));

        assertTrue(n.isNodeType(superuser.getNode(path).getPrimaryNodeType().getName()));
        assertTrue(n.isNodeType(JcrConstants.MIX_LOCKABLE));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3775">OAK-3775</a>
     */
    public void testIsNodeTypeNewNode() throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_LOCKABLE);
        superuser.save();
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        testSession.getNode(path).remove();
        Node newNode = testSession.getNode(testRoot).addNode(Text.getName(path));

        assertTrue(newNode.isNodeType(superuser.getNode(path).getPrimaryNodeType().getName()));
        assertTrue(newNode.isNodeType(testNodeType));
        assertFalse(newNode.isNodeType(JcrConstants.MIX_LOCKABLE));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3775">OAK-3775</a>
     */
    public void testIsNodeTypeAddNewNode() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT));
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        Node newNode = testSession.getNode(path).addNode("child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertTrue(newNode.isNodeType(NodeTypeConstants.NT_OAK_UNSTRUCTURED));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3775">OAK-3775</a>
     */
    public void testIsReferenceable()  throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_REFERENCEABLE);
        superuser.save();
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        Node n = testSession.getNode(path);
        assertTrue(n.isNodeType(JcrConstants.MIX_REFERENCEABLE));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3775">OAK-3775</a>
     */
    public void testIsVersionable()  throws Exception {
        superuser.getNode(path).addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();
        deny(path, privilegesFromName(PrivilegeConstants.REP_READ_PROPERTIES));

        Node n = testSession.getNode(path);
        assertTrue(n.isNodeType(JcrConstants.MIX_VERSIONABLE));
    }
}