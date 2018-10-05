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
import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

public class IndexManagementTest extends AbstractEvaluationTest {

    public void testDefaultSetup() throws RepositoryException {
        assertFalse(testSession.hasPermission(path, Permissions.getString(Permissions.INDEX_DEFINITION_MANAGEMENT)));
    }

    public void testAddOakIndexDefinition() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT));

        Node n = testSession.getNode(path);
        n.addNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        testSession.save();
    }

    public void testAddOakIndexWithoutPermission() throws Exception {
        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));

        Node n = testSession.getNode(path);
        try {
            n.addNode(IndexConstants.INDEX_DEFINITIONS_NAME);
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to add oak:index node.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testAddIndexDefinition() throws Exception {
        superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        superuser.save();

        allow(path, privilegesFromNames(new String[]{PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT}));
        Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        n.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        testSession.save();
    }

    public void testAddIndexDefinitionWithoutPermission() throws Exception {
        superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
            n.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to add index definition node.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testModifyIndexDefinition() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT));
        Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
        n.setProperty("someProperty", "val");
        testSession.save();
    }

    public void testModifyIndexDefinitionWithoutPermission() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
            n.setProperty("someProperty", "val");
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to add index definition property.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testModifyIndexDefinitionWithoutPermission2() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
            n.addNode("customNode");
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to add index definition node.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testModifyIndexDefinitionWithoutPermission3() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        indexDef.setProperty("customProp", "val");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
            n.getProperty("customProp").setValue("val2");
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to modify index definition property.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testModifyIndexDefinitionWithoutPermission4() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        indexDef.setProperty("customProp", "val");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
            n.getProperty("customProp").remove();
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to remove index definition property.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testRemoveIndexDefinition() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT));
        Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
        n.remove();
        testSession.save();
    }

    public void testRemoveIndexDefinitionWithoutPermission() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME).getNode("myIndex");
            n.remove();
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to remove index definition node.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testRemoveOakIndex() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT));
        Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        n.remove();
        testSession.save();
    }

    public void testRemoveOakIndexWithoutPermission() throws Exception {
        Node indexDef = superuser.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        indexDef.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
        superuser.save();

        allow(path, privilegesFromName(PrivilegeConstants.REP_WRITE));
        try {
            Node n = testSession.getNode(path).getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
            n.remove();
            testSession.save();
            fail("AccessDeniedException expected. Test session is not allowed to remove oak:index.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testAddAccessControlToIndexDefinition() throws Exception {
        allow(path, privilegesFromNames(new String[] {PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT}));

        try {
            Node n = testSession.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
            n.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
            AccessControlUtils.addAccessControlEntry(testSession, n.getPath(), testUser.getPrincipal(), new String[] {PrivilegeConstants.JCR_ALL}, true);
            testSession.save();
            fail("Missing rep:modifyAccessControl privilege");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    public void testVersionableIndexDefinition() throws Exception {
        allow(path, privilegesFromNames(new String[] {PrivilegeConstants.REP_INDEX_DEFINITION_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT}));

        try {
            Node n = testSession.getNode(path).addNode(IndexConstants.INDEX_DEFINITIONS_NAME).addNode("myIndex", IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
            n.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "myType");
            n.addMixin(JcrConstants.MIX_VERSIONABLE);
            testSession.save();
            fail("Missing rep:versionManagement privilege");
        } catch (AccessDeniedException e) {
            // success
        }
    }

}
