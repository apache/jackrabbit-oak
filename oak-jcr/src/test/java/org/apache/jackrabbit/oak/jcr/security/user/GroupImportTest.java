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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing group import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 */
public class GroupImportTest extends AbstractImportTest {

    @Override
    protected String getTargetPath() {
        return GROUPPATH;
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    @Test
    public void testImportGroup() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(newGroup.isGroup());
        assertEquals("g", newGroup.getPrincipal().getName());
        assertEquals("g", newGroup.getID());

        Node n = adminSession.getNode(newGroup.getPath());
        assertTrue(n.isNew());
        assertTrue(n.getParent().isSame(target));

        assertEquals("g", n.getName());
        assertEquals("g", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    @Test
    public void testConflictingPrincipalsWithinImport() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "   <sv:value>rep:AuthorizableFolder</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g1\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        try {
            doImport(getTargetPath(), xml);
            adminSession.save();

            fail("Import must detect conflicting principals.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testMultiValuedPrincipalName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value><sv:value>g2</sv:value><sv:value>g</sv:value></sv:property></sv:node>";

        /*
         importing a group with a multi-valued rep:principalName property
         - nonProtected node rep:Group must be created.
         - property rep:principalName must be created regularly without being protected
         - saving changes must fail with ConstraintViolationEx. as the protected
           mandatory property rep:principalName is missing
         */
        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        assertTrue(target.isModified());
        assertTrue(adminSession.hasPendingChanges());

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);

        assertTrue(target.hasNode("g"));
        assertTrue(target.hasProperty("g/rep:principalName"));
        assertFalse(target.getProperty("g/rep:principalName").getDefinition().isProtected());

        // saving changes of the import -> must fail as mandatory prop is missing
        try {
            adminSession.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testIncompleteGroup() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a group without rep:principalName property
         - saving changes must fail with ConstraintViolationEx.
         */
        doImport(getTargetPath(), xml);
        // saving changes of the import -> must fail as mandatory prop is missing
        try {
            adminSession.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        }
    }

    @Test
    public void testImportNewMembers() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "   <sv:value>rep:AuthorizableFolder</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"g\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g1\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Group g = (Group) userMgr.getAuthorizable("g");
        assertNotNull(g);
        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        Node n = adminSession.getNode(g1.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(g));
    }

    @Test
    public void testImportNewMembersReverseOrder() throws Exception {
        // group is imported before the member
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:node sv:name=\"g1\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   </sv:node>" +
                "   <sv:node sv:name=\"g\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Group g = (Group) userMgr.getAuthorizable("g");
        assertNotNull(g);
        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        Node n = adminSession.getNode(g1.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(g));
    }

    @Test
    public void testImportMembers() throws Exception {
        Authorizable admin = checkNotNull(userMgr.getAuthorizable(UserConstants.DEFAULT_ADMIN_ID));
        String uuid = adminSession.getNode(admin.getPath()).getUUID();
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:node sv:name=\"g1\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" + uuid + "</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Group g1 = (Group) userMgr.getAuthorizable("g1");
        assertNotNull(g1);

        // getWeakReferences only works upon save.
        adminSession.save();

        assertTrue(g1.isMember(admin));

        boolean found = false;
        for (Iterator<Group> it = admin.declaredMemberOf(); it.hasNext() && !found; ) {
            found = "g1".equals(it.next().getID());
        }
        assertTrue(found);
    }

    @Test
    public void testImportGroupMembersFromNodes() throws Exception {
        List<String> createdUsers = new LinkedList<String>();
        Node target = getTargetNode();
        try {
            String[] users = {"angi", "adi", "hansi", "lisi", "luzi", "susi", "pipi", "hari", "gabi", "eddi",
                    "debbi", "cati", "admin", "anonymous"};

            for (String user : users) {
                if (userMgr.getAuthorizable(user) == null) {
                    userMgr.createUser(user, user);
                    createdUsers.add(user);
                }
            }
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            doImport(getTargetPath(), getClass().getSimpleName() + "-testImportGroupMembersFromNodes.xml");
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            Authorizable aShrimps = userMgr.getAuthorizable("shrimps");
            assertNotNull("Shrimps authorizable must exist", aShrimps);
            assertTrue("Shrimps authorizable must be a group", aShrimps.isGroup());

            Group gShrimps = (Group) aShrimps;
            for (String user : users) {
                assertTrue(user + " should be member of " + gShrimps, gShrimps.isMember(userMgr.getAuthorizable(user)));
            }


        } finally {
            adminSession.refresh(false);
            for (String user : createdUsers) {
                Authorizable a = userMgr.getAuthorizable(user);
                if (a != null && !a.isGroup()) {
                    a.remove();
                }
            }
            for (NodeIterator it = target.getNodes(); it.hasNext(); ) {
                it.nextNode().remove();
            }
            adminSession.save();
        }
    }

    /**
     * @since OAK 1.0 : Importing new rep:MembershipReferences structure
     */
    @Test
    public void testImportGroupMembersFromOakNodes() throws Exception {
        List<String> createdUsers = new LinkedList<String>();
        Node target = getTargetNode();
        try {
            for (int i=0; i<32; i++) {
                String user = "testUser" + i;
                if (userMgr.getAuthorizable(user) == null) {
                    userMgr.createUser(user, user);
                    createdUsers.add(user);
                }
            }
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            doImport(getTargetPath(), getClass().getSimpleName() + "-testImportGroupMembersFromOakNodes.xml");
            if (!userMgr.isAutoSave()) {
                adminSession.save();
            }

            Authorizable authorizable = userMgr.getAuthorizable("testGroup");
            assertNotNull("testGroup authorizable must exist", authorizable);
            assertTrue("testGroup authorizable must be a group", authorizable.isGroup());
            Group testGroup = (Group) authorizable;
            for (int i=0; i<32; i++) {
                String user = "testUser" + i;
                assertTrue(user + " should be member of " + testGroup, testGroup.isMember(userMgr.getAuthorizable(user)));
            }

            authorizable = userMgr.getAuthorizable("shrimps");
            assertNotNull("shrimps authorizable must exist", authorizable);
            assertTrue("shrimps authorizable must be a group", authorizable.isGroup());
            testGroup = (Group) authorizable;
            for (int i=0; i<32; i++) {
                String user = "testUser" + i;
                assertTrue(user + " should be member of " + testGroup, testGroup.isMember(userMgr.getAuthorizable(user)));
            }


        } finally {
            adminSession.refresh(false);
            for (String user : createdUsers) {
                Authorizable a = userMgr.getAuthorizable(user);
                if (a != null && !a.isGroup()) {
                    a.remove();
                }
            }
            for (NodeIterator it = target.getNodes(); it.hasNext(); ) {
                it.nextNode().remove();
            }
            adminSession.save();
        }
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportGroupWithAuthorizableId() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Authorizable newGroup = userMgr.getAuthorizable("g");
        assertNotNull(newGroup);
        assertTrue(newGroup.isGroup());
        assertEquals("g", newGroup.getID());
        assertTrue(adminSession.propertyExists(newGroup.getPath() + "/rep:authorizableId"));
        assertEquals("g", adminSession.getProperty(newGroup.getPath() + "/rep:authorizableId").getString());

        adminSession.save();
    }

    @Test
    public void testImportNewMembersLateSave() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                "   <sv:value>rep:AuthorizableFolder</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"g1\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>a468b64f-b1df-377c-b325-20d97aaa1ad9</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);
        User user = userMgr.createUser("angi", "pw");
        adminSession.save();

        Group g1 = (Group) userMgr.getAuthorizable("g1");

        // not BEST_EFFORT -> member is not resolved
        assertFalse(g1.isMember(user));
    }
}
