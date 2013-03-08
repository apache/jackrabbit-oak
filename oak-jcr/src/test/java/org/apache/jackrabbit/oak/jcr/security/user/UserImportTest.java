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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * UserImportTest...
 */
public class UserImportTest extends AbstractUserTest {

    private static final String USERPATH = "/rep:security/rep:authorizables/rep:users";
    private static final String GROUPPATH = "/rep:security/rep:authorizables/rep:groups";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // avoid collision with testing a-folders that may have been created
        // with another test (but not removed as user/groups got removed)
        String path = USERPATH + "/t";
        if (superuser.nodeExists(path)) {
            superuser.getNode(path).remove();
        }
        path = GROUPPATH + "/g";
        if (superuser.nodeExists(path)) {
            superuser.getNode(path).remove();
        }
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        //TODO
        try {
            super.tearDown();
        } catch (Exception ignore) {}
    }

    @Test
    public void testImportUser() throws RepositoryException, IOException, SAXException {
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                    "</sv:node>";

        Node parent = superuser.getNode(USERPATH);
            try {
                doImport(USERPATH, xml);

                Authorizable newUser = userMgr.getAuthorizable("t");
                assertNotNull(newUser);
                assertFalse(newUser.isGroup());
                assertEquals("t", newUser.getPrincipal().getName());
                assertEquals("t", newUser.getID());
                assertTrue(((User) newUser).isDisabled());
                assertEquals("disabledUser", ((User) newUser).getDisabledReason());

                Node n = superuser.getNode(newUser.getPath());
                assertTrue(n.isNew());
                assertTrue(n.getParent().isSame(parent));

                assertEquals("t", n.getName());
                assertEquals("t", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());
                assertEquals("{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375", n.getProperty(UserConstants.REP_PASSWORD).getString());
                assertEquals("disabledUser", n.getProperty(UserConstants.REP_DISABLED).getString());

                // saving changes of the import -> must succeed. add mandatory
                // props should have been created.
                superuser.save();

            } finally {
                if (parent.hasNode("t")) {
                    parent.getNode("t").remove();
                    superuser.save();
                }
        }
    }

    public void testImportGroup() throws RepositoryException, IOException, SAXException  {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        Node parent = superuser.getNode(GROUPPATH);
            try {
                doImport(GROUPPATH, xml);

            Authorizable newGroup = userMgr.getAuthorizable("g");
            assertNotNull(newGroup);
            assertTrue(newGroup.isGroup());
            assertEquals("g", newGroup.getPrincipal().getName());
            assertEquals("g", newGroup.getID());

            Node n = superuser.getNode(newGroup.getPath());
            assertTrue(n.isNew());
            assertTrue(n.getParent().isSame(parent));

            assertEquals("g", n.getName());
            assertEquals("g", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());

            // saving changes of the import -> must succeed. add mandatory
            // props should have been created.
            superuser.save();

        } finally {
            if (parent.hasNode("g")) {
                parent.getNode("g").remove();
                superuser.save();
            }
        }
    }

    private void doImport(String parentPath, String xml) throws IOException, SAXException, RepositoryException {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        superuser.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }
}
