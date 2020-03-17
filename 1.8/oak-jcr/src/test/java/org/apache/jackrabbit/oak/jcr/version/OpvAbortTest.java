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
import javax.jcr.nodetype.NodeDefinitionTemplate;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.VersionException;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Test OPV ABORT
 */
public class OpvAbortTest extends AbstractJCRTest implements VersionConstants {

    private VersionManager vMgr;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        vMgr = superuser.getWorkspace().getVersionManager();

        NodeTypeManager ntMgr = superuser.getWorkspace().getNodeTypeManager();
        NodeDefinitionTemplate def = ntMgr.createNodeDefinitionTemplate();
        def.setOnParentVersion(OnParentVersionAction.ABORT);
        def.setName("child");
        def.setRequiredPrimaryTypeNames(new String[] {NT_BASE});

        NodeTypeTemplate tmpl = ntMgr.createNodeTypeTemplate();
        tmpl.setName("OpvAbortTest");
        tmpl.setMixin(true);
        tmpl.getNodeDefinitionTemplates().add(def);
        ntMgr.registerNodeType(tmpl, true);

        testRootNode.addMixin(MIX_VERSIONABLE);
        superuser.save();
    }

    public void testDirectChild() throws Exception {
        testRootNode.addMixin("OpvAbortTest");
        Node n = testRootNode.addNode("child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        assertEquals(OnParentVersionAction.ABORT, n.getDefinition().getOnParentVersion());
        superuser.save();

        try {
            vMgr.checkpoint(testRootNode.getPath());
            fail();
        } catch (VersionException e) {
            // success
        } finally {
            superuser.refresh(false);
        }
    }

    public void testChildInSubTree() throws Exception {
        Node n = testRootNode.addNode(nodeName1).addNode(nodeName2);
        n.addMixin("OpvAbortTest");

        Node child = n.addNode("child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        superuser.save();

        assertEquals(OnParentVersionAction.ABORT, child.getDefinition().getOnParentVersion());
        try {
            vMgr.checkpoint(testRootNode.getPath());
            fail();
        } catch (VersionException e) {
            // success
        } finally {
            superuser.refresh(false);
        }
    }
}