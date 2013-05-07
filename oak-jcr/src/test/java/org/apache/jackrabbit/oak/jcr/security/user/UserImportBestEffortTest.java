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

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Testing {@link ImportBehavior#BESTEFFORT} for user/group import
 */
public class UserImportBestEffortTest extends AbstractImportTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Test
    public void testImportNonExistingMemberBestEffort() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add(UUID.randomUUID().toString()); // random uuid
        invalid.add(getExistingUUID()); // uuid of non-authorizable node

        for (String id : invalid) {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                        "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                        "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                        "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                        "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +id+ "</sv:value></sv:property>" +
                        "</sv:node>" +
                    "</sv:node>";
            try {
                // BESTEFFORT behavior -> must import non-existing members.
                doImport(GROUPPATH, xml);
                Authorizable a = userMgr.getAuthorizable("g1");
                if (a.isGroup()) {
                    // the rep:members property must contain the invalid value
                    boolean found = false;
                    Node grNode = adminSession.getNode(a.getPath());
                    for (Value memberValue : grNode.getProperty(UserConstants.REP_MEMBERS).getValues()) {
                        assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
                        if (id.equals(memberValue.getString())) {
                            found = true;
                            break;
                        }
                    }
                    assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);

                    // declared members must not list the invalid entry.
                    assertNotDeclaredMember((Group) a, id, adminSession);
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                adminSession.refresh(false);
            }
        }
    }

    @Ignore("OAK-414") // FIXME
    @Test
    public void testImportNonExistingMemberBestEffort2() throws Exception {

        String g1Id = "0120a4f9-196a-3f9e-b9f5-23f31f914da7";
        String nonExistingId = "b2f5ff47-4366-31b6-a533-d8dc3614845d"; // groupId of 'g' group.
        if (userMgr.getAuthorizable("g") != null) {
            throw new NotExecutableException();
        }

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + g1Id + "</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +nonExistingId+ "</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        String xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "   <sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + nonExistingId + "</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" + g1Id + "</sv:value></sv:property>" +
                "   </sv:node>";

        // BESTEFFORT behavior -> must import non-existing members.
        doImport(GROUPPATH, xml);
        Authorizable g1 = userMgr.getAuthorizable("g1");
        if (g1.isGroup()) {
            // the rep:members property must contain the invalid value
            boolean found = false;
            Node grNode = adminSession.getNode(g1.getPath());
            for (Value memberValue : grNode.getProperty(UserConstants.REP_MEMBERS).getValues()) {
                assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
                if (nonExistingId.equals(memberValue.getString())) {
                    found = true;
                    break;
                }
            }
            assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);
        } else {
            fail("'g1' was not imported as Group.");
        }

        /*
        now try to import the 'g' group that has a circular group
        membership references.
        expected:
        - group is imported
        - circular membership is ignored
        - g is member of g1
        - g1 isn't member of g
        */
        doImport(GROUPPATH + "/gFolder", xml2);

        Authorizable g = userMgr.getAuthorizable("g");
        assertNotNull(g);
        if (g.isGroup()) {
            assertNotDeclaredMember((Group) g, g1Id, adminSession);
        } else {
            fail("'g' was not imported as Group.");
        }
    }

    @Test
    public void testImportUuidCollisionRemoveExisting() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    /**
     * Same as {@link #testImportUuidCollisionRemoveExisting} with the single
     * difference that the inital import is saved before being overwritten.
     */
    @Test
    public void testImportUuidCollisionRemoveExisting2() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);
        adminSession.save();

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        adminSession.save();
    }

    @Test
    public void testImportUuidCollisionThrow() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(USERPATH, xml);
            doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            fail("UUID collision must be handled according to the uuid behavior.");

        } catch (ItemExistsException e) {
            // success.
        } finally {
            adminSession.refresh(false);
        }
    }

    @Test
    public void testImportImpersonationBestEffort() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"uFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                    "<sv:node sv:name=\"t\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                    "</sv:node>" +
                    "<sv:node sv:name=\"g\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        try {
            doImport(USERPATH, xml);

            Authorizable newUser = userMgr.getAuthorizable("t");
            assertNotNull(newUser);

            Authorizable u2 = userMgr.getAuthorizable("g");
            assertNotNull(u2);

            Subject subj = new Subject();
            subj.getPrincipals().add(u2.getPrincipal());

            Impersonation imp = ((User) newUser).getImpersonation();
            assertTrue(imp.allows(subj));

        } finally {
            superuser.refresh(false);
        }
    }

    @Test
    public void testImportNonExistingImpersonationBestEffort() throws Exception {
        String principalName = "anybody"; // an non-existing princ-name
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property><sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>" +principalName+ "</sv:value></sv:property>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable a = userMgr.getAuthorizable("t");
        assertFalse(a.isGroup());

        boolean found = false;
        PrincipalIterator it = ((User) a).getImpersonation().getImpersonators();
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (principalName.equals(p.getName())) {
                found = true;
                break;
            }
        }
        assertTrue(found);
    }
}