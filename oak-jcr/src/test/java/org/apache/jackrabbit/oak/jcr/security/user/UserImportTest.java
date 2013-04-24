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
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * UserImportTest...
 */
public class UserImportTest extends AbstractUserTest {

    private static final String USERPATH = "/rep:security/rep:authorizables/rep:users";
    private static final String GROUPPATH = "/rep:security/rep:authorizables/rep:groups";

    private JackrabbitSession jrSession;

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
        jrSession = (JackrabbitSession) superuser;
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

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(superuser.hasPendingChanges());

            Authorizable newUser = userMgr.getAuthorizable("t");
            assertNotNull(newUser);
            assertFalse(newUser.isGroup());
            assertEquals("t", newUser.getPrincipal().getName());
            assertEquals("t", newUser.getID());
            assertTrue(((User) newUser).isDisabled());
            assertEquals("disabledUser", ((User) newUser).getDisabledReason());

            Node n = superuser.getNode(newUser.getPath());
            assertTrue(n.isNew());
            assertTrue(n.getParent().isSame(target));

            assertEquals("t", n.getName());
            assertEquals("t", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());
            assertEquals("{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375", n.getProperty(UserConstants.REP_PASSWORD).getString());
            assertEquals("disabledUser", n.getProperty(UserConstants.REP_DISABLED).getString());

            // saving changes of the import -> must succeed. add mandatory
            // props should have been created.
            superuser.save();

        } finally {
            superuser.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testImportGroup() throws RepositoryException, IOException, SAXException  {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);

            assertTrue(target.isModified());
            assertTrue(superuser.hasPendingChanges());

            Authorizable newGroup = userMgr.getAuthorizable("g");
            assertNotNull(newGroup);
            assertTrue(newGroup.isGroup());
            assertEquals("g", newGroup.getPrincipal().getName());
            assertEquals("g", newGroup.getID());

            Node n = superuser.getNode(newGroup.getPath());
            assertTrue(n.isNew());
            assertTrue(n.getParent().isSame(target));

            assertEquals("g", n.getName());
            assertEquals("g", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());

            // saving changes of the import -> must succeed. add mandatory
            // props should have been created.
            superuser.save();

        } finally {
            superuser.refresh(false);
            if (target.hasNode("g")) {
                target.getNode("g").remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testImportGroupIntoUsersTree() throws RepositoryException, IOException, SAXException, NotExecutableException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a group below the users-path:
         - nonProtected node rep:Group must be created.
         - protected properties are ignored
         - UserManager.getAuthorizable must return null.
         - saving changes must fail with ConstraintViolationEx.
         */

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(superuser.hasPendingChanges());

            Authorizable newGroup = userMgr.getAuthorizable("g");
            assertNull(newGroup);

            assertTrue(target.hasNode("g"));
            assertFalse(target.hasProperty("g/rep:principalName"));

            // saving changes of the import -> must fail as mandatory prop is missing
            try {
                superuser.save();
                fail("Import must be incomplete. Saving changes must fail.");
            } catch (ConstraintViolationException e) {
                // success
            }

        } finally {
            superuser.refresh(false);
            if (target.hasNode("g")) {
                target.getNode("g").remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testImportAuthorizableId() throws IOException, RepositoryException, SAXException, NotExecutableException {
        // importing an authorizable with an jcr:uuid that doesn't match the
        // hash of the given ID -> getAuthorizable(String id) will not find the
        // authorizable.
        //String calculatedUUID = "e358efa4-89f5-3062-b10d-d7316b65649e";
        String mismatchUUID = "a358efa4-89f5-3062-b10d-d7316b65649e";

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" +mismatchUUID+ "</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property></sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(superuser.hasPendingChanges());

            // node must be present:
            assertTrue(target.hasNode("t"));
            Node n = target.getNode("t");
            assertEquals(mismatchUUID, n.getUUID());

            // but UserManager.getAuthorizable(String) will not find the
            // authorizable
            Authorizable newUser = userMgr.getAuthorizable("t");
            assertNull(newUser);

        } finally {
            superuser.refresh(false);
        }

    }

    @Test
    public void testExistingPrincipal() throws RepositoryException, NotExecutableException, IOException, SAXException {
        Principal existing = null;
        PrincipalIterator principalIterator = jrSession.getPrincipalManager().getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        while (principalIterator.hasNext()) {
            Principal p = principalIterator.nextPrincipal();
            if (userMgr.getAuthorizable(p) != null) {
                existing = p;
                break;
            }
        }
        if (existing == null) {
            throw new NotExecutableException();
        }

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>" + existing.getName() +"</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);
            fail("Import must detect conflicting principals.");
        } catch (SAXException e) {
            // success
        } finally {
            jrSession.refresh(false);
        }
    }

    @Test
    public void testConflictingPrincipalsWithinImport() throws IOException, RepositoryException, NotExecutableException {
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

        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);
            fail("Import must detect conflicting principals.");
        } catch (SAXException e) {
            // success
        } finally {
            jrSession.refresh(false);
        }
    }

    @Test
    public void testMultiValuedPrincipalName() throws RepositoryException, IOException, SAXException, NotExecutableException {
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
        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);

            assertTrue(target.isModified());
            assertTrue(jrSession.hasPendingChanges());

            Authorizable newGroup = userMgr.getAuthorizable("g");
            assertNotNull(newGroup);

            assertTrue(target.hasNode("g"));
            assertTrue(target.hasProperty("g/rep:principalName"));
            assertFalse(target.getProperty("g/rep:principalName").getDefinition().isProtected());

            // saving changes of the import -> must fail as mandatory prop is missing
            try {
                jrSession.save();
                fail("Import must be incomplete. Saving changes must fail.");
            } catch (ConstraintViolationException e) {
                // success
            }

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("g")) {
                target.getNode("g").remove();
                jrSession.save();
            }
        }
    }

    @Test
    public void testPlainTextPassword() throws RepositoryException, IOException, SAXException, NotExecutableException {
        String plainPw = "myPassword";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>"+plainPw+"</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(jrSession.hasPendingChanges());

            Authorizable newUser = userMgr.getAuthorizable("t");
            Node n = superuser.getNode(newUser.getPath());

            String pwValue = n.getProperty(UserConstants.REP_PASSWORD).getString();
            assertFalse(plainPw.equals(pwValue));
            assertTrue(pwValue.toLowerCase().startsWith("{sha"));

        } finally {
            jrSession.refresh(false);
        }
    }

    @Test
    public void testMultiValuedPassword() throws RepositoryException, IOException, SAXException, NotExecutableException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        /*
         importing a user with a multi-valued rep:password property
         - nonProtected node rep:User must be created.
         - property rep:password must be created regularly without being protected
         - saving changes must fail with ConstraintViolationEx. as the protected
           mandatory property rep:password is missing
         */
        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(jrSession.hasPendingChanges());

            Authorizable newUser = userMgr.getAuthorizable("t");
            assertNotNull(newUser);

            assertTrue(target.hasNode("t"));
            assertTrue(target.hasProperty("t/rep:password"));
            assertFalse(target.getProperty("t/rep:password").getDefinition().isProtected());

            // saving changes of the import -> must fail as mandatory prop is missing
            try {
                jrSession.save();
                fail("Import must be incomplete. Saving changes must fail.");
            } catch (ConstraintViolationException e) {
                // success
            }

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                jrSession.save();
            }
        }
    }

    @Test
    public void testIncompleteUser() throws RepositoryException, IOException, SAXException, NotExecutableException {
        List<String> incompleteXml = new ArrayList<String>();
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "</sv:node>");
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>");

        for (String xml : incompleteXml) {
            Node target = superuser.getNode(USERPATH);
            try {
                doImport(USERPATH, xml);
                // saving changes of the import -> must fail as mandatory prop is missing
                try {
                    jrSession.save();
                    fail("Import must be incomplete. Saving changes must fail.");
                } catch (ConstraintViolationException e) {
                    // success
                }
            } finally {
                jrSession.refresh(false);
                if (target.hasNode("t")) {
                    target.getNode("t").remove();
                    jrSession.save();
                }
            }
        }
    }

    @Test
    public void testIncompleteGroup() throws IOException, RepositoryException, SAXException, NotExecutableException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a group without rep:principalName property
         - saving changes must fail with ConstraintViolationEx.
         */
        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);
            // saving changes of the import -> must fail as mandatory prop is missing
            try {
                jrSession.save();
                fail("Import must be incomplete. Saving changes must fail.");
            } catch (ConstraintViolationException e) {
                // success
            }

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("g")) {
                target.getNode("g").remove();
                jrSession.save();
            }
        }
    }

    @Test
    public void testImportWithIntermediatePath() throws IOException, RepositoryException, SAXException, NotExecutableException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"some\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>d5433be9-68d0-4fba-bf96-efc29f461993</sv:value></sv:property>" +
                    "<sv:node sv:name=\"intermediate\">" +
                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>d87354a4-037e-4756-a8fb-deb2eb7c5149</sv:value></sv:property>" +
                        "<sv:node sv:name=\"path\">" +
                        "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                        "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>24263272-b789-4568-957a-3bcaf99dbab3</sv:value></sv:property>" +
                            "<sv:node sv:name=\"t3\">" +
                            "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                            "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0b8854ad-38f0-36c6-9807-928d28195609</sv:value></sv:property>" +
                            "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}4358694eeb098c6708ae914a10562ce722bbbc34</sv:value></sv:property>" +
                            "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t3</sv:value></sv:property>" +
                            "</sv:node>" +
                        "</sv:node>" +
                    "</sv:node>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            assertTrue(target.isModified());
            assertTrue(jrSession.hasPendingChanges());

            Authorizable newUser = userMgr.getAuthorizable("t3");
            assertNotNull(newUser);
            assertFalse(newUser.isGroup());
            assertEquals("t3", newUser.getPrincipal().getName());
            assertEquals("t3", newUser.getID());

            Node n = superuser.getNode(newUser.getPath());
            assertTrue(n.isNew());

            Node parent = n.getParent();
            assertFalse(n.isSame(target));
            assertTrue(parent.isNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
            assertFalse(parent.getDefinition().isProtected());

            assertTrue(target.hasNode("some"));
            assertTrue(target.hasNode("some/intermediate/path"));

        } finally {
            jrSession.refresh(false);
        }
    }

    @Test
    public void testImportNewMembers() throws IOException, RepositoryException, SAXException, NotExecutableException {
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

        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);

            Group g = (Group) userMgr.getAuthorizable("g");
            assertNotNull(g);
            Group g1 = (Group) userMgr.getAuthorizable("g1");
            assertNotNull(g1);

            Node n = jrSession.getNode(g1.getPath());
            assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

            // getWeakReferences only works upon save.
            jrSession.save();

            assertTrue(g1.isMember(g));

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("gFolder")) {
                target.getNode("gFolder").remove();
            }
            jrSession.save();
        }
    }

    @Test
    public void testImportNewMembersReverseOrder() throws IOException, RepositoryException, SAXException, NotExecutableException {
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

        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);

            Group g = (Group) userMgr.getAuthorizable("g");
            assertNotNull(g);
            Group g1 = (Group) userMgr.getAuthorizable("g1");
            assertNotNull(g1);

            Node n = jrSession.getNode(g1.getPath());
            assertTrue(n.hasProperty(UserConstants.REP_MEMBERS) || n.hasNode(UserConstants.NT_REP_MEMBERS));

            // getWeakReferences only works upon save.
            jrSession.save();

            assertTrue(g1.isMember(g));

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("gFolder")) {
                target.getNode("gFolder").remove();
            }
            jrSession.save();
        }
    }

    @Test
    public void testImportMembers() throws RepositoryException, IOException, SAXException, NotExecutableException {
        Authorizable admin = userMgr.getAuthorizable("admin");
        if (admin == null) {
            throw new NotExecutableException();
        }

        String uuid = superuser.getNode(admin.getPath()).getUUID();
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "   <sv:node sv:name=\"g1\">" +
                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +uuid+ "</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);

            Group g1 = (Group) userMgr.getAuthorizable("g1");
            assertNotNull(g1);

            // getWeakReferences only works upon save.
            jrSession.save();

            assertTrue(g1.isMember(admin));

            boolean found = false;
            for (Iterator<Group> it = admin.declaredMemberOf(); it.hasNext() && !found;) {
                found = "g1".equals(it.next().getID());
            }
            assertTrue(found);

        } finally {
            jrSession.refresh(false);
            target.getNode("gFolder").remove();
            jrSession.save();
        }
    }

    @Test
    public void testImportNonExistingMemberIgnore() throws IOException, RepositoryException, SAXException, NotExecutableException {
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
            Node target = superuser.getNode(GROUPPATH);
            try {
                doImport(GROUPPATH, xml /*, UserImporter.ImportBehavior.IGNORE*/);
                // there should be no exception during import,
                // but invalid members must be ignored.
                Authorizable a = userMgr.getAuthorizable("g1");
                if (a.isGroup()) {
                    assertNotDeclaredMember((Group) a, id, superuser);
                } else {
                    fail("'g1' was not imported as Group.");
                }
            } finally {
                jrSession.refresh(false);
            }
        }
    }

    //TODO test UserImporter.ImportBehavior != ignore
//    public void testImportNonExistingMemberAbort() throws IOException, RepositoryException, SAXException, NotExecutableException {
//        Node n = testRootNode.addNode(nodeName1, ntUnstructured);
//        n.addMixin(mixReferenceable);
//
//        List<String> invalid = new ArrayList<String>();
//        invalid.add(UUID.randomUUID().toString()); // random uuid
//        invalid.add(n.getUUID()); // uuid of non-authorizable node
//
//        for (String id : invalid) {
//            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                    "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
//                        "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +id+ "</sv:value></sv:property>" +
//                        "</sv:node>" +
//                    "</sv:node>";
//            NodeImpl target = (NodeImpl) sImpl.getNode(umgr.getGroupsPath());
//            try {
//                doImport(target, xml, UserImporter.ImportBehavior.ABORT);
//                // import behavior ABORT -> should throw.
//                fail("importing invalid members -> must throw.");
//            } catch (SAXException e) {
//                // success as well
//            } finally {
//                sImpl.refresh(false);
//            }
//        }
//    }
//
//    public void testImportNonExistingMemberBestEffort() throws IOException, RepositoryException, SAXException, NotExecutableException {
//        if (umgr.hasMemberSplitSize()) {
//            throw new NotExecutableException();
//        }
//
//        Node n = testRootNode.addNode(nodeName1, ntUnstructured);
//        n.addMixin(mixReferenceable);
//
//        List<String> invalid = new ArrayList<String>();
//        invalid.add(UUID.randomUUID().toString()); // random uuid
//        invalid.add(n.getUUID()); // uuid of non-authorizable node
//
//        for (String id : invalid) {
//            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                    "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
//                        "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0120a4f9-196a-3f9e-b9f5-23f31f914da7</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
//                        "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +id+ "</sv:value></sv:property>" +
//                        "</sv:node>" +
//                    "</sv:node>";
//            NodeImpl target = (NodeImpl) sImpl.getNode(umgr.getGroupsPath());
//            try {
//                // BESTEFFORT behavior -> must import non-existing members.
//                doImport(target, xml, UserImporter.ImportBehavior.BESTEFFORT);
//                Authorizable a = umgr.getAuthorizable("g1");
//                if (a.isGroup()) {
//                    // the rep:members property must contain the invalid value
//                    boolean found = false;
//                    NodeImpl grNode = ((AuthorizableImpl) a).getNode();
//                    for (Value memberValue : grNode.getProperty(UserConstants.P_MEMBERS).getValues()) {
//                        assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
//                        if (id.equals(memberValue.getString())) {
//                            found = true;
//                            break;
//                        }
//                    }
//                    assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);
//
//                    // declared members must not list the invalid entry.
//                    assertNotDeclaredMember((Group) a, id);
//                } else {
//                    fail("'g1' was not imported as Group.");
//                }
//            } finally {
//                sImpl.refresh(false);
//            }
//        }
//    }
//
//    public void testImportNonExistingMemberBestEffort2() throws IOException, RepositoryException, SAXException, NotExecutableException {
//
//        String g1Id = "0120a4f9-196a-3f9e-b9f5-23f31f914da7";
//        String nonExistingId = "b2f5ff47-4366-31b6-a533-d8dc3614845d"; // groupId of 'g' group.
//        if (umgr.getAuthorizable("g") != null || umgr.hasMemberSplitSize()) {
//            throw new NotExecutableException();
//        }
//
//        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
//                "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + g1Id + "</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +nonExistingId+ "</sv:value></sv:property>" +
//                "</sv:node>" +
//                "</sv:node>";
//
//        String xml2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "   <sv:node sv:name=\"g\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                "       <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
//                "       <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + nonExistingId + "</sv:value></sv:property>" +
//                "       <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
//                "       <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" + g1Id + "</sv:value></sv:property>" +
//                "   </sv:node>";
//
//        NodeImpl target = (NodeImpl) sImpl.getNode(umgr.getGroupsPath());
//        try {
//            // BESTEFFORT behavior -> must import non-existing members.
//            doImport(target, xml, UserImporter.ImportBehavior.BESTEFFORT);
//            Authorizable g1 = umgr.getAuthorizable("g1");
//            if (g1.isGroup()) {
//                // the rep:members property must contain the invalid value
//                boolean found = false;
//                NodeImpl grNode = ((AuthorizableImpl) g1).getNode();
//                for (Value memberValue : grNode.getProperty(UserConstants.P_MEMBERS).getValues()) {
//                    assertEquals(PropertyType.WEAKREFERENCE, memberValue.getType());
//                    if (nonExistingId.equals(memberValue.getString())) {
//                        found = true;
//                        break;
//                    }
//                }
//                assertTrue("ImportBehavior.BESTEFFORT must import non-existing members.",found);
//            } else {
//                fail("'g1' was not imported as Group.");
//            }
//
//            /*
//            now try to import the 'g' group that has a circular group
//            membership references.
//            expected:
//            - group is imported
//            - circular membership is ignored
//            - g is member of g1
//            - g1 isn't member of g
//            */
//            target = (NodeImpl) target.getNode("gFolder");
//            doImport(target, xml2, UserImporter.ImportBehavior.BESTEFFORT);
//
//            Authorizable g = umgr.getAuthorizable("g");
//            assertNotNull(g);
//            if (g.isGroup()) {
//                assertNotDeclaredMember((Group) g, g1Id);
//            } else {
//                fail("'g' was not imported as Group.");
//            }
//
//        } finally {
//            sImpl.refresh(false);
//        }
//    }

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
        Node target = superuser.getNode(GROUPPATH);
        try {
            doImport(GROUPPATH, xml);
            // no exception during import -> member must have been ignored though.
            Authorizable a = userMgr.getAuthorizable("g1");
            if (a.isGroup()) {
                assertNotDeclaredMember((Group) a, invalidId, superuser);
            } else {
                fail("'g1' was not imported as Group.");
            }
        } finally {
            jrSession.refresh(false);
        }
    }

    //TODO test UserImporter.ImportBehavior != ignore
//    public void testImportSelfAsGroupAbort() throws Exception {
//
//        String invalidId = "0120a4f9-196a-3f9e-b9f5-23f31f914da7"; // uuid of the group itself
//        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<sv:node sv:name=\"gFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
//                "<sv:node sv:name=\"g1\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>"+invalidId+"</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g1</sv:value></sv:property>" +
//                "   <sv:property sv:name=\"rep:members\" sv:type=\"WeakReference\"><sv:value>" +invalidId+ "</sv:value></sv:property>" +
//                "</sv:node>" +
//                "</sv:node>";
//        NodeImpl target = (NodeImpl) sImpl.getNode(umgr.getGroupsPath());
//        try {
//            doImport(target, xml, UserImporter.ImportBehavior.ABORT);
//            fail("Importing self as group with ImportBehavior.ABORT must fail.");
//        } catch (SAXException e) {
//            // success.
//        }finally {
//            sImpl.refresh(false);
//        }
//    }

    @Test
    public void testImportImpersonation() throws IOException, RepositoryException, SAXException, NotExecutableException {
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

        Node target = superuser.getNode(USERPATH);
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
            jrSession.refresh(false);
        }
    }

    @Test
    public void testImportInvalidImpersonationIgnore() throws IOException, RepositoryException, SAXException, NotExecutableException {
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

            Node target = superuser.getNode(USERPATH);
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
                jrSession.refresh(false);
            }
        }
    }

    //TODO test UserImporter.ImportBehavior != ignore
//    public void testImportInvalidImpersonationAbort() throws IOException, RepositoryException, SAXException, NotExecutableException {
//        List<String> invalid = new ArrayList<String>();
//        invalid.add("anybody"); // an non-existing princ-name
//        invalid.add("administrators"); // a group
//        invalid.add("t"); // principal of the user itself.
//
//        for (String principalName : invalid) {
//            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
//                    "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
//                    "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
//                    "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
//                    "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
//                    "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property><sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>" +principalName+ "</sv:value></sv:property>" +
//                    "</sv:node>";
//            Subject subj = new Subject();
//            subj.getPrincipals().add(new PrincipalImpl(principalName));
//
//            NodeImpl target = (NodeImpl) sImpl.getNode(umgr.getUsersPath());
//            try {
//                doImport(target, xml, UserImporter.ImportBehavior.ABORT);
//                fail("UserImporter.ImportBehavior.ABORT -> importing invalid impersonators must throw.");
//            } catch (SAXException e) {
//                // success
//            } finally {
//                sImpl.refresh(false);
//            }
//        }
//    }

    @Test
    public void testImportUuidCollisionRemoveExisting() throws RepositoryException, IOException, SAXException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            //TODO different IgnoreBehavior needed?
            // re-import should succeed if UUID-behavior is set accordingly
            doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

            // saving changes of the import -> must succeed. add mandatory
            // props should have been created.
            jrSession.save();

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                jrSession.save();
            }
        }
    }

    /**
     * Same as {@link #testImportUuidCollisionRemoveExisting} with the single
     * difference that the inital import is saved before being overwritten.
     *
     * @throws RepositoryException
     * @throws IOException
     * @throws SAXException
     */
    @Test
    public void testImportUuidCollisionRemoveExisting2() throws RepositoryException, IOException, SAXException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);
            jrSession.save();

            //TODO different IgnoreBehavior needed?
            // re-import should succeed if UUID-behavior is set accordingly
            doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

            // saving changes of the import -> must succeed. add mandatory
            // props should have been created.
            jrSession.save();

        } finally {
            jrSession.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                jrSession.save();
            }
        }
    }

    @Test
    public void testImportUuidCollisionThrow() throws RepositoryException, IOException, SAXException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        Node target = superuser.getNode(USERPATH);
        try {
            doImport(USERPATH, xml);

            doImport(USERPATH, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            fail("UUID collision must be handled according to the uuid behavior.");

        } catch (SAXException e) {
            assertTrue(e.getException() instanceof ItemExistsException);
            // success.
        } catch (ItemExistsException e) {
            // success.
        } finally {
            jrSession.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                jrSession.save();
            }
        }
    }

    @Test
    public void testImportGroupMembersFromNodes() throws RepositoryException, IOException, SAXException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sv:node sv:name=\"s\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:sling=\"http://sling.apache.org/jcr/sling/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"sh\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"shrimps\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property><sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>08429aec-6f09-30db-8c83-1a2a57fc760c</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\">" +
                     "<sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>shrimps</sv:value></sv:property><sv:node sv:name=\"rep:members\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"adi\" sv:type=\"WeakReference\"><sv:value>c46335eb-267e-3e1c-9e5b-017acb4cd799</sv:value></sv:property><sv:property sv:name=\"admin\" sv:type=\"WeakReference\"><sv:value>21232f29-7a57-35a7-8389-4a0e4a801fc3</sv:value></sv:property></sv:node><sv:node sv:name=\"angi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"angi\" sv:type=\"WeakReference\"><sv:value>a468b64f-b1df-377c-b325-20d97aaa1ad9</sv:value></sv:property><sv:property sv:name=\"anonymous\" sv:type=\"WeakReference\"><sv:value>294de355-7d9d-30b3-92d8-a1e6aab028cf</sv:value></sv:property><sv:property sv:name=\"cati\" sv:type=\"WeakReference\"><sv:value>f08910b6-41c8-3cb9-a648-1dddd14b132d</sv:value></sv:property></sv:node></sv:node><sv:n" +
                     "ode sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"debbi\" sv:type=\"WeakReference\"><sv:value>d53bedf9-ebb8-3117-a8b8-162d32b4bee2</sv:value></sv:property><sv:property sv:name=\"eddi\" sv:type=\"WeakReference\"><sv:value>1795fa1a-3d20-3a64-996e-eaaeb520a01e</sv:value></sv:property><sv:property sv:name=\"gabi\" sv:type=\"WeakReference\"><sv:value>a0d499c7-5105-3663-8611-a32779a57104</sv:value></sv:property><sv:property sv:name=\"hansi\" sv:type=\"WeakReference\"><sv:value>9ea4d671-8ed1-399a-8401-59487a14d00a</sv:value></sv:property></sv:node><sv:node sv:name=\"hari\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"hari\" sv:type=\"WeakReference\"><sv:value>a9bcf1e4-d7b9-3a22-a297-5c812d938889</sv:value></sv:property><sv:property sv:name=\"lisi\" sv:type=\"WeakReference\"><sv:value>dc3a8f16-70d6-3bea-a9b7-b65048a0ac40</sv:value></sv:property></sv:node><sv:node sv:name=\"luzi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"luzi\" sv:type=\"WeakReference\"><sv:value>9ec299fd-3461-3f1a-9749-92a76f2516eb</sv:value></sv:property><sv:property sv:name=\"pipi\" sv:type=" +
                     "\"WeakReference\"><sv:value>16d5d24f-5b09-3199-9bd4-e5f57bf11237</sv:value></sv:property><sv:property sv:name=\"susi\" sv:type=\"WeakReference\"><sv:value>536931d8-0dec-318c-b3db-9612bdd004d4</sv:value></sv:property></sv:node></sv:node></sv:node></sv:node></sv:node></sv:node>";

        List<String> createdUsers = new LinkedList<String>();
        Node target = superuser.getNode(GROUPPATH);
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
                jrSession.save();
            }

            doImport(GROUPPATH, xml);
            if (!userMgr.isAutoSave()) {
                jrSession.save();
            }

            Authorizable aShrimps = userMgr.getAuthorizable("shrimps");
            assertNotNull(aShrimps);
            assertTrue(aShrimps.isGroup());

            Group gShrimps = (Group) aShrimps;
            for (String user : users) {
                assertTrue(user + " should be member of " + gShrimps, gShrimps.isMember(userMgr.getAuthorizable(user)));
            }


        } finally {
            jrSession.refresh(false);
            for (String user : createdUsers) {
                Authorizable a = userMgr.getAuthorizable(user);
                if (a != null && !a.isGroup()) {
                    a.remove();
                }
            }
            if (!userMgr.isAutoSave()) {
                jrSession.save();
            }
            for (NodeIterator it = target.getNodes(); it.hasNext(); ) {
                it.nextNode().remove();
            }
            if (!userMgr.isAutoSave()) {
                jrSession.save();
            }
        }
    }

    //TODO test UserImporter.ImportBehavior != ignore
//    public void testImportGroupMembersFromNodesBestEffort() throws RepositoryException, IOException, SAXException {
//        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sv:node sv:name=\"s\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:sling=\"http://sling.apache.org/jcr/sling/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"sh\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\"><sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:node sv:name=\"shrimps\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Group</sv:value></sv:property><sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>08429aec-6f09-30db-8c83-1a2a57fc760c</sv:value></sv:property><sv:property sv:name=\"jcr:created\" sv:type=\"Date\">" +
//                     "<sv:value>2010-08-17T18:22:20.086+02:00</sv:value></sv:property><sv:property sv:name=\"jcr:createdBy\" sv:type=\"String\"><sv:value>admin</sv:value></sv:property><sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>shrimps</sv:value></sv:property><sv:node sv:name=\"rep:members\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"adi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"adi\" sv:type=\"WeakReference\"><sv:value>c46335eb-267e-3e1c-9e5b-017acb4cd799</sv:value></sv:property><sv:property sv:name=\"admin\" sv:type=\"WeakReference\"><sv:value>21232f29-7a57-35a7-8389-4a0e4a801fc3</sv:value></sv:property></sv:node><sv:node sv:name=\"angi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"angi\" sv:type=\"WeakReference\"><sv:value>a468b64f-b1df-377c-b325-20d97aaa1ad9</sv:value></sv:property><sv:property sv:name=\"anonymous\" sv:type=\"WeakReference\"><sv:value>294de355-7d9d-30b3-92d8-a1e6aab028cf</sv:value></sv:property><sv:property sv:name=\"cati\" sv:type=\"WeakReference\"><sv:value>f08910b6-41c8-3cb9-a648-1dddd14b132d</sv:value></sv:property></sv:node></sv:node><sv:n" +
//                     "ode sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:node sv:name=\"debbi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"debbi\" sv:type=\"WeakReference\"><sv:value>d53bedf9-ebb8-3117-a8b8-162d32b4bee2</sv:value></sv:property><sv:property sv:name=\"eddi\" sv:type=\"WeakReference\"><sv:value>1795fa1a-3d20-3a64-996e-eaaeb520a01e</sv:value></sv:property><sv:property sv:name=\"gabi\" sv:type=\"WeakReference\"><sv:value>a0d499c7-5105-3663-8611-a32779a57104</sv:value></sv:property><sv:property sv:name=\"hansi\" sv:type=\"WeakReference\"><sv:value>9ea4d671-8ed1-399a-8401-59487a14d00a</sv:value></sv:property></sv:node><sv:node sv:name=\"hari\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"hari\" sv:type=\"WeakReference\"><sv:value>a9bcf1e4-d7b9-3a22-a297-5c812d938889</sv:value></sv:property><sv:property sv:name=\"lisi\" sv:type=\"WeakReference\"><sv:value>dc3a8f16-70d6-3bea-a9b7-b65048a0ac40</sv:value></sv:property></sv:node><sv:node sv:name=\"luzi\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:Members</sv:value></sv:property><sv:property sv:name=\"luzi\" sv:type=\"WeakReference\"><sv:value>9ec299fd-3461-3f1a-9749-92a76f2516eb</sv:value></sv:property><sv:property sv:name=\"pipi\" sv:type=" +
//                     "\"WeakReference\"><sv:value>16d5d24f-5b09-3199-9bd4-e5f57bf11237</sv:value></sv:property><sv:property sv:name=\"susi\" sv:type=\"WeakReference\"><sv:value>536931d8-0dec-318c-b3db-9612bdd004d4</sv:value></sv:property></sv:node></sv:node></sv:node></sv:node></sv:node></sv:node>";
//
//        List<String> createdUsers = new LinkedList<String>();
//        NodeImpl groupsNode = (NodeImpl) sImpl.getNode(umgr.getGroupsPath());
//        try {
//            String[] users = {"angi", "adi", "hansi", "lisi", "luzi", "susi", "pipi", "hari", "gabi", "eddi",
//                              "debbi", "cati", "admin", "anonymous"};
//
//            doImport(groupsNode, xml, UserImporter.ImportBehavior.BESTEFFORT);
//            if (!umgr.isAutoSave()) {
//                sImpl.save();
//            }
//
//            for (String user : users) {
//                if (umgr.getAuthorizable(user) == null) {
//                    umgr.createUser(user, user);
//                    createdUsers.add(user);
//                }
//            }
//            if (!umgr.isAutoSave()) {
//                sImpl.save();
//            }
//
//            Authorizable aShrimps = umgr.getAuthorizable("shrimps");
//            assertNotNull(aShrimps);
//            assertTrue(aShrimps.isGroup());
//
//            Group gShrimps = (Group) aShrimps;
//            for (String user : users) {
//                assertTrue(user + " should be member of " + gShrimps, gShrimps.isMember(umgr.getAuthorizable(user)));
//            }
//
//
//        } finally {
//            sImpl.refresh(false);
//            for (String user : createdUsers) {
//                Authorizable a = umgr.getAuthorizable(user);
//                if (a != null && !a.isGroup()) {
//                    a.remove();
//                }
//            }
//            if (!umgr.isAutoSave()) {
//                sImpl.save();
//            }
//            for (NodeIterator it = groupsNode.getNodes(); it.hasNext(); ) {
//                it.nextNode().remove();
//            }
//            if (!umgr.isAutoSave()) {
//                sImpl.save();
//            }
//        }
//    }

    private void doImport(String parentPath, String xml) throws IOException, SAXException, RepositoryException {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        superuser.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }

    private void doImport(String parentPath, String xml, int importUUIDBehavior) throws IOException, SAXException, RepositoryException {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        superuser.importXML(parentPath, in, importUUIDBehavior);
    }

    private static void assertNotDeclaredMember(Group gr, String potentialID, Session session ) throws RepositoryException {
        // declared members must not list the invalid entry.
        Iterator<Authorizable> it = gr.getDeclaredMembers();
        while (it.hasNext()) {
            Authorizable member = it.next();
            assertFalse(potentialID.equals(session.getNode(member.getPath()).getIdentifier()));
        }
    }
}
