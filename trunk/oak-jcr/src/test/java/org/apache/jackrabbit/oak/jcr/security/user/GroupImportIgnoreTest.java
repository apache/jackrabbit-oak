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

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior#IGNORE} for group import
 */
public class GroupImportIgnoreTest extends AbstractImportTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    @Override
    protected String getTargetPath() {
        return GROUPPATH;
    }

    @Test
    public void testImportSelfAsGroupIgnore() throws Exception {
        String invalidId = "0120a4f9-196a-3f9e-b9f5-23f31f914da7"; // uuid of the group itself
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>"+invalidId+"</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +invalidId+ "</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";
        doImport(getTargetPath(), xml);
        // no exception during import -> member must have been ignored though.
        Authorizable a = getUserManager().getAuthorizable("g1");
        if (a.isGroup()) {
            assertNotDeclaredMember((Group) a, invalidId, getImportSession());
        } else {
            fail("'g1' was not imported as Group.");
        }
    }

    @Test
    public void testImportNonExistingMemberIgnore() throws Exception {
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
                // there should be no exception during import,
                // but invalid members must be ignored.
                doImport(getTargetPath(), xml);
                Authorizable a = getUserManager().getAuthorizable("g1");
                if (a.isGroup()) {
                    assertNotDeclaredMember((Group) a, id, getImportSession());
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                getImportSession().refresh(false);
            }
        }
    }

    @Test
    public void testImportCircularMembership() throws Exception {
        String g1Id = "0120a4f9-196a-3f9e-b9f5-23f31f914da7";
        String gId = "b2f5ff47-4366-31b6-a533-d8dc3614845d"; // groupId of 'g' group.
        if (getUserManager().getAuthorizable("g") != null) {
            throw new NotExecutableException();
        }

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                    "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + g1Id + "</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +gId+ "</sv:value></sv:property>" +
                    "</sv:node>" +
                    "<sv:node sv:name=\"g\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + gId + "</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +g1Id+ "</sv:value></sv:property>" +
                    "</sv:node>" +
                "</sv:node>";

        /*
        import groups with cyclic membership with with IGNORE.
        expected:
        - group is imported
        - circular membership is ignored
        - g is member of g1
        - g1 isn't member of g  (circular membership detected)
        */
        doImport(getTargetPath(), xml);

        Group g1 = userMgr.getAuthorizable("g1", Group.class);
        Group g = userMgr.getAuthorizable("g", Group.class);

        boolean b = g1.isDeclaredMember(g);
        assertEquals("Circular membership must be detected", !b, g.isDeclaredMember(g1));

        assertEquals("Circular membership must be detected", b, Iterators.contains(g1.getMembers(), g));
        assertEquals("Circular membership must be detected", !b, Iterators.contains(g.getMembers(), g1));
    }
}
