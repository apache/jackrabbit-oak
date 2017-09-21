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
import javax.jcr.Property;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Test OPV VERSION
 */
public class OpvVersionTest extends AbstractJCRTest implements VersionConstants {

    private String siblingName;
    private VersionManager versionManager;
    private Node frozen;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node n1 = testRootNode.addNode(nodeName1, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Node n2 = n1.addNode(nodeName2, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        Node n3 = n1.addNode(nodeName3, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        siblingName = nodeName1 + 'b';
        Node n1b = testRootNode.addNode(siblingName, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        assertEquals(OnParentVersionAction.VERSION, n1.getDefinition().getOnParentVersion());
        assertEquals(OnParentVersionAction.VERSION, n2.getDefinition().getOnParentVersion());
        assertEquals(OnParentVersionAction.VERSION, n3.getDefinition().getOnParentVersion());
        assertEquals(OnParentVersionAction.VERSION, n1b.getDefinition().getOnParentVersion());

        testRootNode.addMixin(MIX_VERSIONABLE);
        n1b.addMixin(MIX_VERSIONABLE);
        n2.addMixin(MIX_VERSIONABLE);
        superuser.save();

        versionManager = superuser.getWorkspace().getVersionManager();
        frozen = versionManager.checkpoint(testRoot).getFrozenNode();
    }

    public void testDirectChild() throws Exception {
        // n1 : is not versionable -> copied to frozen node
        assertTrue(frozen.hasNode(nodeName1));
        Node frozenN1 = frozen.getNode(nodeName1);
        assertEquals(NT_FROZENNODE, frozenN1.getPrimaryNodeType().getName());

        assertTrue(frozenN1.hasNode(nodeName2));
        assertTrue(frozenN1.hasNode(nodeName3));

        // n1b is versionable -> only child of 'nt:versionedChild' is created in
        // the frozen node with 'jcr:childVersionHistory' property referring to
        // the version history of n1b
        assertTrue(frozen.hasNode(siblingName));
        Node frozenN1b = frozen.getNode(siblingName);
        assertEquals(NT_VERSIONEDCHILD, frozenN1b.getPrimaryNodeType().getName());
        Property childVh = frozenN1b.getProperty(JCR_CHILD_VERSION_HISTORY);
        assertEquals(versionManager.getVersionHistory(testRoot + '/' + siblingName).getUUID(), childVh.getString());

    }

    public void testChildInSubTree() throws Exception {
        Node frozenN1 = frozen.getNode(nodeName1);

        Node frozenN2 = frozenN1.getNode(nodeName2);
        assertEquals(NT_VERSIONEDCHILD, frozenN2.getPrimaryNodeType().getName());
        Property childVh = frozenN2.getProperty(JCR_CHILD_VERSION_HISTORY);
        assertEquals(versionManager.getVersionHistory(testRoot + '/' + nodeName1 + '/' + nodeName2).getUUID(), childVh.getString());

        Node frozenN3 = frozenN1.getNode(nodeName3);
        assertEquals(NT_FROZENNODE, frozenN3.getPrimaryNodeType().getName());
    }
}