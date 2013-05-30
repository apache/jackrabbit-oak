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

import javax.jcr.AccessDeniedException;
import javax.jcr.Workspace;
import javax.jcr.nodetype.NodeTypeManager;
import javax.jcr.nodetype.NodeTypeTemplate;
import javax.jcr.security.AccessControlPolicy;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT;

/**
 * Permission evaluation tests related to
 * {@link org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants#JCR_NODE_TYPE_DEFINITION_MANAGEMENT}
 * privilege.
 */
public class NodeTypeDefinitionManagementTest extends AbstractEvaluationTest {

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        assertHasRepoPrivilege(JCR_NODE_TYPE_DEFINITION_MANAGEMENT, false);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        try {
            for (AccessControlPolicy policy : acMgr.getPolicies(null)) {
                acMgr.removePolicy(null, policy);
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testRegisterNodeType() throws Exception {
        Workspace testWsp = testSession.getWorkspace();
        NodeTypeManager ntm = testWsp.getNodeTypeManager();
        NodeTypeTemplate ntd = ntm.createNodeTypeTemplate();
        ntd.setName("testRegisterNodeType");
        ntd.setMixin(true);

        try {
            ntm.registerNodeType(ntd, true);
            fail("Node type registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
        try {
            ntm.registerNodeType(ntd, false);
            fail("Node type registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }

        NodeTypeTemplate[] ntds = new NodeTypeTemplate[2];
        ntds[0] = ntd;
        ntds[1] = ntm.createNodeTypeTemplate();
        ntds[1].setName("anotherRegisterNodeType");
        ntds[1].setDeclaredSuperTypeNames(new String[] {"nt:file"});
        try {
            ntm.registerNodeTypes(ntds, true);
            fail("Node type registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }

        try {
            ntm.registerNodeTypes(ntds, false);
            fail("Node type registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testModifyNodeTypeWithPrivilege() throws Exception {
        modify(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT.toString(), true);
        assertHasRepoPrivilege(JCR_NODE_TYPE_DEFINITION_MANAGEMENT, true);

        modify(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT.toString(), false);
        assertHasRepoPrivilege(JCR_NODE_TYPE_DEFINITION_MANAGEMENT, false);
    }

    @Test
    public void testRegisterNodeTypeWithPrivilege() throws Exception {
        modify(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT.toString(), true);
        try {
            Workspace testWsp = testSession.getWorkspace();
            NodeTypeManager ntm = testWsp.getNodeTypeManager();
            NodeTypeTemplate ntd = ntm.createNodeTypeTemplate();
            ntd.setName("testRegisterNodeTypeWithPrivilege");
            ntd.setMixin(true);
            ntm.registerNodeType(ntd, true);

            NodeTypeTemplate[] ntds = new NodeTypeTemplate[2];
            ntds[0] = ntd;
            ntds[1] = ntm.createNodeTypeTemplate();
            ntds[1].setName("anotherRegisterNodeTypeWithPrivilege");
            ntds[1].setDeclaredSuperTypeNames(new String[] {"nt:file"});
            ntm.registerNodeTypes(ntds, true);
        } finally {
            modify(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT.toString(), false);
        }
    }

    @Test
    public void testUnRegisterNodeType() throws Exception {
        NodeTypeManager ntm = superuser.getWorkspace().getNodeTypeManager();
        NodeTypeTemplate ntd = ntm.createNodeTypeTemplate();
        ntd.setName("testUnregisterNodeType");
        ntd.setMixin(true);
        ntm.registerNodeType(ntd, true);

        Workspace testWsp = testSession.getWorkspace();
        try {
            try {
                NodeTypeManager testNtm = testWsp.getNodeTypeManager();
                testNtm.unregisterNodeType(ntd.getName());
                fail("Node type unregistration should be denied.");
            } catch (AccessDeniedException e) {
                // success
            }
            try {
                NodeTypeManager testNtm = testWsp.getNodeTypeManager();
                testNtm.unregisterNodeTypes(new String[] {ntd.getName()});
                fail("Node type unregistration should be denied.");
            } catch (AccessDeniedException e) {
                // success
            }
        } finally {
            // NOTE: diff to jr-core where unregisterNt was not supported
            ntm.unregisterNodeType(ntd.getName());
        }
    }
}