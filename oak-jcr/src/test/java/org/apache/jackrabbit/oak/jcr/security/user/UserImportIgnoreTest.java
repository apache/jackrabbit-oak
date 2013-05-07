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
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

/**
 * Testing {@link ImportBehavior#IGNORE} for user/group import
 */
public class UserImportIgnoreTest extends AbstractImportTest {

    @Override
    protected String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
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
        try {
            doImport(GROUPPATH, xml);
            // no exception during import -> member must have been ignored though.
            Authorizable a = userMgr.getAuthorizable("g1");
            if (a.isGroup()) {
                assertNotDeclaredMember((Group) a, invalidId, adminSession);
            } else {
                fail("'g1' was not imported as Group.");
            }
        } finally {
            adminSession.refresh(false);
        }
    }

    @Test
    public void testImportInvalidImpersonationIgnore() throws Exception {
        List<String> invalid = new ArrayList<String>();
        invalid.add("anybody"); // an non-existing princ-name
        invalid.add("administrators"); // a group
        invalid.add("t"); // principal of the user itself.

        for (String principalName : invalid) {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property><sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>" +principalName+ "</sv:value></sv:property>" +
                    "</sv:node>";
            Subject subj = new Subject();
            subj.getPrincipals().add(new PrincipalImpl(principalName));

            try {
                doImport(USERPATH, xml);
                // no exception during import: no impersonation must be granted
                // for the invalid principal name
                Authorizable a = userMgr.getAuthorizable("t");
                if (!a.isGroup()) {
                    Impersonation imp = ((User)a).getImpersonation();
                    Subject s = new Subject();
                    s.getPrincipals().add(new PrincipalImpl(principalName));
                    assertFalse(imp.allows(s));
                    for (PrincipalIterator it = imp.getImpersonators(); it.hasNext();) {
                        assertFalse(principalName.equals(it.nextPrincipal().getName()));
                    }
                } else {
                    fail("Importing 't' didn't create a User.");
                }
            } finally {
                adminSession.refresh(false);
            }
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
                doImport(GROUPPATH, xml);
                Authorizable a = userMgr.getAuthorizable("g1");
                if (a.isGroup()) {
                    assertNotDeclaredMember((Group) a, id, adminSession);
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                adminSession.refresh(false);
            }
        }
    }
}