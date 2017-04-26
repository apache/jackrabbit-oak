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
import java.util.List;
import java.util.UUID;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Session;
import javax.jcr.Value;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#BESTEFFORT} for group import
 */
public class GroupImportBestEffortTest extends AbstractImportTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    @Override
    protected String getTargetPath() {
        return GROUPPATH;
    }

    @Test
    public void testImportNonExistingMemberBestEffort() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add(UUID.randomUUID().toString()); // random uuid
        invalid.add(getExistingUUID()); // uuid of non-authorizable node

        Session s = getImportSession();
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
                doImport(getTargetPath(), xml);
                Authorizable a = getUserManager().getAuthorizable("g1");
                if (a.isGroup()) {
                    // the rep:members property must contain the invalid value
                    boolean found = false;
                    Node grNode = s.getNode(a.getPath());
                    for (Value memberValue : grNode.getProperty(UserConstants.REP_MEMBERS).getValues()) {
                        assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
                        if (id.equals(memberValue.getString())) {
                            found = true;
                            break;
                        }
                    }
                    assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);

                    // declared members must not list the invalid entry.
                    assertNotDeclaredMember((Group) a, id, s);
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                s.refresh(false);
            }
        }
    }

    @Test
    public void testImportNonExistingMemberBestEffort2() throws Exception {

        String g1Id = "0120a4f9-196a-3f9e-b9f5-23f31f914da7";
        String nonExistingId = "b2f5ff47-4366-31b6-a533-d8dc3614845d"; // groupId of 'g' group.
        if (getUserManager().getAuthorizable("g") != null) {
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

        // BESTEFFORT behavior -> must import non-existing members.
        doImport(getTargetPath(), xml);
        Authorizable g1 = getUserManager().getAuthorizable("g1");
        if (g1.isGroup()) {
            // the rep:members property must contain the invalid value
            boolean found = false;
            Node grNode = getImportSession().getNode(g1.getPath());
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
    }

    @Test
    public void testImportCircularMembership() throws Exception {

        String g1Id = "0120a4f9-196a-3f9e-b9f5-23f31f914da7";
        String nonExistingId = "b2f5ff47-4366-31b6-a533-d8dc3614845d"; // groupId of 'g' group.
        if (getUserManager().getAuthorizable("g") != null) {
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
        doImport(getTargetPath(), xml);

        /*
        now try to import the 'g' group that has a circular group membership references.
        expected:
        - group is imported
        - circular membership is not spotted due to best-effort optimization
        - circular membership is spotted upon resolution of members
        */
        doImport(getTargetPath() + "/gFolder", xml2);

        Group g = getUserManager().getAuthorizable("g", Group.class);
        Group g1 = getUserManager().getAuthorizable("g1", Group.class);

        assertNotNull("'g' was not imported as Group.", g);
        assertNotNull("'g1' was not imported as Group.", g1);

        assertTrue(g1.isDeclaredMember(g));
        assertTrue(g.isDeclaredMember(g1));

        // circular membership created during import -> must be spotted upon member-access
        assertEquals(1, Iterators.size(g1.getMembers()));
        assertEquals(1, Iterators.size(g.getMembers()));
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
        User user = getUserManager().createUser("angi", "pw");
        getImportSession().save();

        Group g1 = (Group) getUserManager().getAuthorizable("g1");
        assertTrue(g1.isMember(user));
    }

}
