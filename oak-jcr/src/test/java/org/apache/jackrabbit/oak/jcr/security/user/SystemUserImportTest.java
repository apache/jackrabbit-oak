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
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.ItemExistsException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing system user import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 */
public class SystemUserImportTest extends AbstractImportTest {

    @Override
    public void before() throws Exception {
        super.before();

        adminSession.getNode(USERPATH).addNode(UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH, UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        adminSession.save();
    }

    @Override
    public void after() throws Exception {
        try {
            getTargetNode().remove();
            adminSession.save();
        } finally {
            super.after();
        }
    }

    @Override
    protected String getTargetPath() {
        return USERPATH + '/' + UserConstants.DEFAULT_SYSTEM_RELATIVE_PATH;
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    @Test
    public void testImportUser() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        Session s = getImportSession();
        assertTrue(target.isModified());
        assertTrue(s.hasPendingChanges());

        Authorizable newUser = getUserManager().getAuthorizable("t");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertTrue(((User) newUser).isSystemUser());

        assertEquals("t", newUser.getPrincipal().getName());
        assertEquals("t", newUser.getID());
        assertTrue(((User) newUser).isDisabled());
        assertEquals("disabledUser", ((User) newUser).getDisabledReason());

        Node n = s.getNode(newUser.getPath());
        assertTrue(n.isNew());
        assertTrue(n.getParent().isSame(target));

        assertEquals("t", n.getName());
        assertEquals("t", n.getProperty(UserConstants.REP_PRINCIPAL_NAME).getString());
        assertEquals("disabledUser", n.getProperty(UserConstants.REP_DISABLED).getString());

        assertFalse(n.hasProperty(UserConstants.REP_PASSWORD));

        // saving changes of the import -> must succeed. all mandatory props should have been created.
        s.save();
    }

    /**
     * @since OAK 1.0 : constraintviolation is no longer detected during import
     *        but only upon save.
     */
    @Test
    public void testImportIntoNonSystemPath() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        /*
         importing a system user below the regular users-path:
         - saving changes must fail with ConstraintViolationEx.
         */
        Session s = getImportSession();
        Node target = s.getNode(USERPATH);
        doImport(USERPATH, xml);

        assertTrue(target.isModified());
        assertTrue(s.hasPendingChanges());

        Authorizable user = getUserManager().getAuthorizable("t");
        assertNotNull(user);
        assertTrue(target.hasNode("t"));
        assertTrue(target.hasProperty("t/rep:principalName"));

        // saving changes of the import -> must fail
        try {
            s.save();
            fail("Import must be incomplete. Saving changes must fail.");
        } catch (ConstraintViolationException e) {
            // success
        } finally {
            s.refresh(false);
            if (target.hasNode("t")) {
                target.getNode("t").remove();
                target.save();
            }
        }
    }

    @Test
    public void testImportUuidMismatch() throws Exception {
        // importing an authorizable with an jcr:uuid that doesn't match the
        // hash of the given ID -> getAuthorizable(String id) will not find the
        // authorizable.
        //String calculatedUUID = "e358efa4-89f5-3062-b10d-d7316b65649e";
        String mismatchUUID = "a358efa4-89f5-3062-b10d-d7316b65649e";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>" + mismatchUUID + "</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property></sv:node>";

        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        assertTrue(target.isModified());
        assertTrue(getImportSession().hasPendingChanges());

        // node must be present:
        assertTrue(target.hasNode("t"));
        Node n = target.getNode("t");
        assertEquals(mismatchUUID, n.getUUID());

        // but UserManager.getAuthorizable(String) will not find the
        // authorizable
        Authorizable newUser = getUserManager().getAuthorizable("t");
        assertNull(newUser);
    }

    @Test
    public void testExistingPrincipal() throws Exception {
        Principal existing = null;

        Session s = getImportSession();
        PrincipalIterator principalIterator = ((JackrabbitSession) s).getPrincipalManager().getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        while (principalIterator.hasNext()) {
            Principal p = principalIterator.nextPrincipal();
            if (getUserManager().getAuthorizable(p) != null) {
                existing = p;
                break;
            }
        }
        if (existing == null) {
            throw new NotExecutableException();
        }

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>" + existing.getName() + "</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(getTargetPath(), xml);
            s.save();

            fail("Import must detect conflicting principals.");
        } catch (RepositoryException e) {
            // success
        }
    }

    @Test
    public void testPassword() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Node target = getTargetNode();
        assertFalse(target.hasProperty(UserConstants.REP_PASSWORD));

        Authorizable user = getUserManager().getAuthorizable("t");
        assertNotNull(user);
        assertFalse(user.isGroup());
        assertTrue(((User)user).isSystemUser());

        getImportSession().save();
    }

    /**
     * @since OAK 1.0 : password property is not longer mandatory -> multivalued
     *        property will just be ignored (instead of throwing ConstraintViolationException
     *        upon save).
     */
    @Test
    public void testMultiValuedPassword() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        /*
         importing a user with a multi-valued rep:password property
         - nonProtected node rep:SystemUser must be created.
         - property rep:password must be created regularly without being protected
         */
        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        assertTrue(target.isModified());
        assertTrue(getImportSession().hasPendingChanges());

        Authorizable newUser = getUserManager().getAuthorizable("t");
        assertNotNull(newUser);

        assertTrue(target.hasNode("t"));
        assertTrue(target.hasProperty("t/rep:password"));
        assertFalse(target.getProperty("t/rep:password").getDefinition().isProtected());
    }

    @Test
    public void testIncompleteUser() throws Exception {
        List<String> incompleteXml = new ArrayList<String>();
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "</sv:node>");
        incompleteXml.add("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>");

        Session s = getImportSession();
        for (String xml : incompleteXml) {
            Node target = s.getNode(getTargetPath());
            try {
                doImport(getTargetPath(), xml);
                // saving changes of the import -> must fail as mandatory prop is missing
                try {
                    s.save();
                    fail("Import must be incomplete. Saving changes must fail.");
                } catch (ConstraintViolationException e) {
                    // success
                }
            } finally {
                s.refresh(false);
                if (target.hasNode("t")) {
                    target.getNode("t").remove();
                    s.save();
                }
            }
        }
    }

    /**
     * @since OAK 1.0 : importing User without password must succeed.
     */
    @Test
    public void testUserWithoutPassword() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Authorizable user = getUserManager().getAuthorizable("t");
        assertNotNull(user);
        assertFalse(user.isGroup());
        assertFalse(getImportSession().propertyExists(user.getPath() + "/rep:password"));
    }

    @Test
    public void testImportWithIntermediatePath() throws Exception {
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
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0b8854ad-38f0-36c6-9807-928d28195609</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}4358694eeb098c6708ae914a10562ce722bbbc34</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t3</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>" +
                "</sv:node>" +
                "</sv:node>";

        Node target = getTargetNode();
        doImport(getTargetPath(), xml);

        assertTrue(target.isModified());
        assertTrue(getImportSession().hasPendingChanges());

        Authorizable newUser = getUserManager().getAuthorizable("t3");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t3", newUser.getPrincipal().getName());
        assertEquals("t3", newUser.getID());

        Node n = getImportSession().getNode(newUser.getPath());
        assertTrue(n.isNew());

        Node parent = n.getParent();
        assertFalse(n.isSame(target));
        assertTrue(parent.isNodeType(UserConstants.NT_REP_AUTHORIZABLE_FOLDER));
        assertFalse(parent.getDefinition().isProtected());

        assertTrue(target.hasNode("some"));
        assertTrue(target.hasNode("some/intermediate/path"));
    }

    @Test
    public void testImportImpersonation() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"uFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"t\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:impersonators\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"g\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>b2f5ff47-4366-31b6-a533-d8dc3614845d</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>g</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Authorizable newUser = getUserManager().getAuthorizable("t");
        assertNotNull(newUser);

        Authorizable u2 = getUserManager().getAuthorizable("g");
        assertNotNull(u2);

        Subject subj = new Subject();
        subj.getPrincipals().add(u2.getPrincipal());

        Impersonation imp = ((User) newUser).getImpersonation();
        assertTrue(imp.allows(subj));
    }

    @Test
    public void testImportUuidCollisionRemoveExisting() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"r\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>4b43b0ae-e356-34cd-95b9-10189b3dc231</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        getImportSession().save();
    }

    /**
     * Same as {@link #testImportUuidCollisionRemoveExisting} with the single
     * difference that the initial import is saved before being overwritten.
     *
     * @throws Exception
     */
    @Test
    public void testImportUuidCollisionRemoveExisting2() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"r\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>4b43b0ae-e356-34cd-95b9-10189b3dc231</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        doImport(getTargetPath(), xml);
        getImportSession().save();

        // re-import should succeed if UUID-behavior is set accordingly
        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        // saving changes of the import -> must succeed. add mandatory
        // props should have been created.
        getImportSession().save();
    }

    @Test
    public void testImportUuidCollisionThrow() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        try {
            doImport(getTargetPath(), xml);
            doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            fail("UUID collision must be handled according to the uuid behavior.");

        } catch (ItemExistsException e) {
            // success.
        }
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithAuthorizableId() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Session s = getImportSession();
        Authorizable newUser = getUserManager().getAuthorizable("t");
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(s.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", s.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        s.save();
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdDifferentFromNodeName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Session s = getImportSession();
        Authorizable newUser = getUserManager().getAuthorizable("t");

        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(s.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", s.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        s.save();
    }

    /**
     * Same as {@link #testImportUserWithIdDifferentFromNodeName} but with
     * different order of properties.
     *
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdDifferentFromNodeName2() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "</sv:node>";
        doImport(getTargetPath(), xml);

        Authorizable newUser = getUserManager().getAuthorizable("t");

        Session s = getImportSession();
        assertNotNull(newUser);
        assertFalse(newUser.isGroup());
        assertEquals("t", newUser.getID());
        assertTrue(s.propertyExists(newUser.getPath() + "/rep:authorizableId"));
        assertEquals("t", s.getProperty(newUser.getPath() + "/rep:authorizableId").getString());
        s.save();
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithExistingId() throws Exception {
        String existingId = "admin";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t_diff\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + existingId + "</sv:value></sv:property>" +
                "</sv:node>";
        try {
            doImport(getTargetPath(), xml);
            fail("Reuse of existing ID must be detected.");
        } catch (AuthorizableExistsException e) {
            // success
        }
    }

    /**
     * @since OAK 1.0 : Importing rep:authorizableId
     */
    @Test
    public void testImportUserWithIdCollision() throws Exception {
        String collidingId = "t";
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"uFolder\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AuthorizableFolder</sv:value></sv:property>" +
                "<sv:node sv:name=\"t1\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + collidingId + "</sv:value></sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"t2\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:SystemUser</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0f826a89-cf68-3399-85f4-cf320c1a5842</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>" + collidingId + "</sv:value></sv:property>" +
                "</sv:node>" +
                "</sv:node>";
        try {
            doImport(getTargetPath(), xml);
            fail("Reuse of existing ID must be detected.");
        } catch (AuthorizableExistsException e) {
            // success
        }
    }
}
