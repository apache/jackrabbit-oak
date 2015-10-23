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

import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.jcr.version.OnParentVersionAction;
import javax.jcr.version.Version;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * Test OPV IGNORE
 */
public class OpvIgnoreTest extends AbstractJCRTest {

    private VersionManager versionManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // add child nodes that have OPV COPY and thus end up in the frozen node
        testRootNode.addNode(nodeName1, NodeTypeConstants.NT_OAK_UNSTRUCTURED).addNode(nodeName2, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        superuser.save();

        versionManager = superuser.getWorkspace().getVersionManager();
    }

    private void addIgnoredChild(@Nonnull Node node) throws Exception {
        AccessControlManager acMgr = superuser.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, node.getPath());
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ));
        acMgr.setPolicy(acl.getPath(), acl);
        superuser.save();

        Node c = node.getNode(AccessControlConstants.REP_POLICY);
        assertEquals(OnParentVersionAction.IGNORE, c.getDefinition().getOnParentVersion());
    }

    public void testDirectChild() throws Exception {
        addIgnoredChild(testRootNode);

        testRootNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        // enforce the creation of the version (and the frozen node)
        Version version = versionManager.checkpoint(testRoot);
        Node frozen = version.getFrozenNode();

        assertTrue(frozen.hasNode(nodeName1));
        assertTrue(frozen.getNode(nodeName1).hasNode(nodeName2));
        assertFalse(frozen.hasNode(AccessControlConstants.REP_POLICY));
    }

    public void testChildInSubTree() throws Exception {
        addIgnoredChild(testRootNode.getNode(nodeName1));

        testRootNode.addMixin(JcrConstants.MIX_VERSIONABLE);
        superuser.save();

        // enforce the creation of the version (and the frozen node)
        Version version = versionManager.checkpoint(testRoot);
        Node frozen = version.getFrozenNode();

        assertTrue(frozen.hasNode(nodeName1));
        Node frozenChild = frozen.getNode(nodeName1);
        assertTrue(frozenChild.hasNode(nodeName2));
        assertFalse(frozenChild.hasNode(AccessControlConstants.REP_POLICY));
    }
}