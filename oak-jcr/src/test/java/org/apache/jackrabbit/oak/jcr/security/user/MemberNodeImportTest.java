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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PropertyType;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Testing import behavior if user mgt is configured to store group members
 * in a tree structure.
 */
@Ignore("OAK-414") // TODO: waiting for OAK-482
public class MemberNodeImportTest extends AbstractImportTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // FIXME: create JCR repository with user mgt setup that stores group members in a tree structure (blocked by OAK-482)
    }

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Test
    public void testImportGroupMembersFromNodesBestEffort() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sv:node sv:name=\"s\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:sling=\"http://sling.apache.org/jcr/sling/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"sh\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"shrimps\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property><sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>08429aec-6f09-30db-8c83-1a2a57fc760c</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\">" +
                "<sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>shrimps</sv:value></sv:property><sv:node sv:name=\"rep:members\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"adi\" sv:type=\"WeakReference\"><sv:value>c46335eb-267e-3e1c-9e5b-017acb4cd799</sv:value></sv:property><sv:property sv:name=\"admin\" sv:type=\"WeakReference\"><sv:value>21232f29-7a57-35a7-8389-4a0e4a801fc3</sv:value></sv:property></sv:node><sv:node sv:name=\"angi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"angi\" sv:type=\"WeakReference\"><sv:value>a468b64f-b1df-377c-b325-20d97aaa1ad9</sv:value></sv:property><sv:property sv:name=\"anonymous\" sv:type=\"WeakReference\"><sv:value>294de355-7d9d-30b3-92d8-a1e6aab028cf</sv:value></sv:property><sv:property sv:name=\"cati\" sv:type=\"WeakReference\"><sv:value>f08910b6-41c8-3cb9-a648-1dddd14b132d</sv:value></sv:property></sv:node></sv:node><sv:n" +
                "ode sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"debbi\" sv:type=\"WeakReference\"><sv:value>d53bedf9-ebb8-3117-a8b8-162d32b4bee2</sv:value></sv:property><sv:property sv:name=\"eddi\" sv:type=\"WeakReference\"><sv:value>1795fa1a-3d20-3a64-996e-eaaeb520a01e</sv:value></sv:property><sv:property sv:name=\"gabi\" sv:type=\"WeakReference\"><sv:value>a0d499c7-5105-3663-8611-a32779a57104</sv:value></sv:property><sv:property sv:name=\"hansi\" sv:type=\"WeakReference\"><sv:value>9ea4d671-8ed1-399a-8401-59487a14d00a</sv:value></sv:property></sv:node><sv:node sv:name=\"hari\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"hari\" sv:type=\"WeakReference\"><sv:value>a9bcf1e4-d7b9-3a22-a297-5c812d938889</sv:value></sv:property><sv:property sv:name=\"lisi\" sv:type=\"WeakReference\"><sv:value>dc3a8f16-70d6-3bea-a9b7-b65048a0ac40</sv:value></sv:property></sv:node><sv:node sv:name=\"luzi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"luzi\" sv:type=\"WeakReference\"><sv:value>9ec299fd-3461-3f1a-9749-92a76f2516eb</sv:value></sv:property><sv:property sv:name=\"pipi\" sv:type=" +
                "\"WeakReference\"><sv:value>16d5d24f-5b09-3199-9bd4-e5f57bf11237</sv:value></sv:property><sv:property sv:name=\"susi\" sv:type=\"WeakReference\"><sv:value>536931d8-0dec-318c-b3db-9612bdd004d4</sv:value></sv:property></sv:node></sv:node></sv:node></sv:node></sv:node></sv:node>";

        List<String> createdUsers = new LinkedList<String>();
        Node groupsNode = superuser.getNode(GROUPPATH);
        try {
            String[] users = {"angi", "adi", "hansi", "lisi", "luzi", "susi", "pipi", "hari", "gabi", "eddi",
                    "debbi", "cati", "admin", "anonymous"};

            doImport(groupsNode.getPath(), xml);
            superuser.save();

            for (String user : users) {
                if (userMgr.getAuthorizable(user) == null) {
                    userMgr.createUser(user, user);
                    createdUsers.add(user);
                }
            }
            superuser.save();

            Authorizable aShrimps = userMgr.getAuthorizable("shrimps");
            assertNotNull(aShrimps);
            assertTrue(aShrimps.isGroup());

            Group gShrimps = (Group) aShrimps;
            for (String user : users) {
                assertTrue(user + " should be member of " + gShrimps, gShrimps.isMember(userMgr.getAuthorizable(user)));
            }


        } finally {
            superuser.refresh(false);
            for (String user : createdUsers) {
                Authorizable a = userMgr.getAuthorizable(user);
                if (a != null && !a.isGroup()) {
                    a.remove();
                }
            }
            superuser.save();
            for (NodeIterator it = groupsNode.getNodes(); it.hasNext(); ) {
                it.nextNode().remove();
            }
            if (!userMgr.isAutoSave()) {
                superuser.save();
            }
        }
    }

    @Test
    public void testImportNonExistingMemberBestEffort() throws Exception {
        Node n = testRootNode.addNode(nodeName1, ntUnstructured);
        n.addMixin(mixReferenceable);

        List<String> invalid = new ArrayList<String>();
        invalid.add(UUID.randomUUID().toString()); // random uuid
        invalid.add(n.getUUID()); // uuid of non-authorizable node

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
                    Node grNode = superuser.getNode(a.getPath());
                    for (Value memberValue : grNode.getProperty(UserConstants.REP_MEMBERS).getValues()) {
                        assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
                        if (id.equals(memberValue.getString())) {
                            found = true;
                            break;
                        }
                    }
                    assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);

                    // declared members must not list the invalid entry.
                    assertNotDeclaredMember((Group) a, id, superuser);
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                superuser.refresh(false);
            }
        }
    }

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

        try {
            // BESTEFFORT behavior -> must import non-existing members.
            doImport(GROUPPATH, xml);
            Authorizable g1 = userMgr.getAuthorizable("g1");
            if (g1.isGroup()) {
                // the rep:members property must contain the invalid value
                boolean found = false;
                Node grNode = superuser.getNode(g1.getPath());
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
                assertNotDeclaredMember((Group) g, g1Id, superuser);
            } else {
                fail("'g' was not imported as Group.");
            }

        } finally {
            superuser.refresh(false);
        }
    }
}