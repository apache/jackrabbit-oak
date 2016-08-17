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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.util.UUID;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.api.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GroupImportWithPoliciesTest extends AbstractImportTest {

    private String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
            "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
            "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
            "   <sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\" sv:multiple=\"true\"><sv:value>rep:AccessControllable</sv:value></sv:property>" +
            "   <sv:node sv:name=\"rep:policy\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:ACL</sv:value></sv:property><sv:node sv:name=\"allow\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:GrantACE</sv:value></sv:property><sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>administrators</sv:value></sv:property><sv:property sv:name=\"rep:privileges\" sv:type=\"Name\" sv:multiple=\"true\"><sv:value>jcr:read</sv:value></sv:property></sv:node></sv:node>" +
            "</sv:node>";

    private String testUserID = "testUser" + UUID.randomUUID();
    private Session testSession;

    @Override
    public void before() throws Exception {
        super.before();

        User testUser = userMgr.createUser(testUserID, testUserID);
        AccessControlUtils.addAccessControlEntry(adminSession, Text.getRelativeParent(GROUPPATH, 1), testUser.getPrincipal(), new String[]{
                Privilege.JCR_READ,
                Privilege.JCR_WRITE,
                Privilege.JCR_READ_ACCESS_CONTROL,
                Privilege.JCR_MODIFY_ACCESS_CONTROL,
                Privilege.JCR_NODE_TYPE_MANAGEMENT,
                PrivilegeConstants.REP_USER_MANAGEMENT}, true);
        // registering new namespaces curing import requires jcr:namespaceManagement privilege on the repo level (null path)
        AccessControlUtils.addAccessControlEntry(adminSession, null, testUser.getPrincipal(), new String[] {PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT}, true);
        adminSession.save();

        testSession = adminSession.getRepository().login(new SimpleCredentials(testUserID, testUserID.toCharArray()));
    }

    @Override
    public void after() throws Exception {
        try {
            testSession.logout();

            adminSession.refresh(false);
            userMgr.getAuthorizable(testUserID).remove();
            Authorizable g = userMgr.getAuthorizable("g");
            if (g != null) {
                g.remove();
            }
            adminSession.save();
        } finally {
            super.after();
        }
    }

    @Override
    protected String getTargetPath() {
        return GROUPPATH;
    }

    @Override
    protected Session getImportSession() {
        return testSession;
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    @Test
    public void testImportGroupWithPolicies() throws Exception {
        Node target = getTargetNode();
        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        assertTrue(getImportSession().hasPendingChanges());

        Authorizable newGroup = getUserManager().getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(newGroup.isGroup());
        assertEquals("g", newGroup.getPrincipal().getName());
        assertEquals("g", newGroup.getID());

        Node n = getImportSession().getNode(newGroup.getPath());
        assertTrue(n.isNew());
        assertTrue(n.getParent().isSame(target));

        assertEquals("g", n.getName());
        assertEquals("g", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());

        AccessControlList acl = AccessControlUtils.getAccessControlList(getImportSession(), newGroup.getPath());
        assertNotNull(acl);

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        assertEquals(1, aces.length);
        assertEquals("administrators", aces[0].getPrincipal().getName());
        assertEquals(PrivilegeConstants.JCR_READ, aces[0].getPrivileges()[0].getName());

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        getImportSession().save();
    }

}