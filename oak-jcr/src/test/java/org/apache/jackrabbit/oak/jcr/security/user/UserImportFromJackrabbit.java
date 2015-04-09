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

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.core.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.api.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests to verify the behavior of importing user content packages exported
 * from Jackrabbit 2.x and their behavior upon import into an Oak repository.
 *
 * @since Oak 1.2
 * @see <a href="https://issues.apache.org/jira/browse/OAK-2245">OAK-2245</a>
 */
public class UserImportFromJackrabbit extends AbstractImportTest {

    private String uid = "t";
    private String randomNodeName = "f5aj6fp7q9834jof";
    private String intermediatePath = "foo/bar/test";

    private Session importSession;

    @Override
    public void before() throws Exception {
        super.before();

        importSession = getImportSession();

    }

    @Override
    protected String getTargetPath() {
        return USERPATH;
    }

    @Override
    protected String getImportBehavior() {
        return null;
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testImportCreatesAuthorizableId() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\""+uid+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml);

        Authorizable newUser = getUserManager().getAuthorizable(uid);
        assertEquals(uid, newUser.getID());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(uid, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must succeed
        importSession.save();
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorReplace() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl("t"), getTargetPath() + "/foo/bar/test");
        String initialPath = u.getPath();
        importSession.save();

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\""+uid+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // replace should retain path
        assertEquals(initialPath, newUser.getPath());
        assertFalse(getTargetPath().equals(Text.getRelativeParent(newUser.getPath(), 1)));

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(uid, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must succeed
        importSession.save();
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorRemove() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String initialPath = u.getPath();
        importSession.save();

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // IMPORT_UUID_COLLISION_REMOVE_EXISTING should result in the user to
        // be imported a the new path
        assertEquals(getTargetPath(), Text.getRelativeParent(newUser.getPath(), 1));
        assertFalse(initialPath.equals(newUser.getPath()));

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(uid, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must succeed
        importSession.save();
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorReplaceFromRenamed() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String initialPath = u.getPath();
        String movedPath = Text.getRelativeParent(initialPath, 1) + '/' + randomNodeName;
        importSession.move(initialPath, movedPath);
        importSession.save();

        // import 'correct' jr2 package which contains the encoded ID in the node name
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\""+uid+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // replace should update the path
        assertEquals("user path", Text.getRelativeParent(initialPath, 1) + '/' + uid, newUser.getPath());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, uid, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, uid, newUser.getID());

        // saving changes of the import must succeed.
        importSession.save();
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorReplaceFromRenamed2() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String initialPath = u.getPath();
        String movedPath = Text.getRelativeParent(initialPath, 1) + '/' + randomNodeName;
        importSession.move(initialPath, movedPath);
        importSession.save();

        // we need to include the new node name in the sysview import, so that the importer uses the correct name.
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"" + randomNodeName + "\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // replace should retain path
        assertEquals("user path", movedPath, newUser.getPath());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, randomNodeName, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes must fail -> the original authorizable has been replaced
        // and got the ID set as specified by the Jackrabbit XML. Since the latter
        // specifies the modified authorizable ID, which doesn't represent the
        // correct ID as hashed in the jcr:uuid, the CommitHook will detect
        // the mismatch, which for the diff looks like a modified ID.
        try {
            importSession.save();
            fail("Importing an authorizable with mismatch between authorizableId and uuid must fail.");
        } catch (ConstraintViolationException e) {
            // success
            assertTrue(e.getMessage().contains("OakConstraint0022"));
        }
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorReplaceFromRenamed3() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String originalPath = u.getPath();
        importSession.save();

        // we need to include the new node name in the sysview import, so that the importer uses the correct name.
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"" + randomNodeName + "\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // replace should change the original path
        String expectedPath = Text.getRelativeParent(originalPath, 1) + '/' + randomNodeName;
        assertEquals("user path", expectedPath, newUser.getPath());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, randomNodeName, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must fail as the authorizable ID
        // has been modified (it no longer represents the correct ID due to the
        // modified node name in combination with the fact that in JR 2.x
        // the node name MUST contain the id as there is no rep:authorizableId.
        try {
            importSession.save();
            fail("Importing an authorizable with mismatch between authorizableId and uuid must fail.");
        } catch (ConstraintViolationException e) {
            // success
            assertTrue(e.getMessage().contains("OakConstraint0021"));
        }
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorRemoveFromRenamed() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String initialPath = u.getPath();
        String movedPath = Text.getRelativeParent(initialPath, 1) + '/' + randomNodeName;
        importSession.move(initialPath, movedPath);
        importSession.save();

        // import 'correct' jr2 package which contains the encoded ID in the node name
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\""+uid+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // IMPORT_UUID_COLLISION_REMOVE_EXISTING should import the user at the new path
        assertEquals("user path", getTargetPath() + '/' + uid, newUser.getPath());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, uid, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, uid, newUser.getID());

        // saving changes of the import must succeed.
        importSession.save();
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorRemoveFromRenamed2() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String initialPath = u.getPath();
        String movedPath = Text.getRelativeParent(initialPath, 1) + '/' + randomNodeName;
        importSession.move(initialPath, movedPath);
        importSession.save();

        // we need to include the new node name in the sysview import, so that the importer uses the correct name.
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"" + randomNodeName + "\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // IMPORT_UUID_COLLISION_REMOVE_EXISTING should import the user at the new path
        assertEquals("user path", getTargetPath() + '/' + randomNodeName, newUser.getPath());

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, randomNodeName, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must fail as original user has been
        // removed and the JR 2.x the node name doesn't contain the correct id,
        // which is detected during save as it looks like the id had been modified.
        try {
            importSession.save();
            fail("Importing an authorizable with mismatch between authorizableId and uuid must fail.");
        } catch (ConstraintViolationException e) {
            // success
            assertTrue(e.getMessage().contains("OakConstraint0021"));
        }
    }

    /**
     * @since Oak 1.2
     */
    @Test
    public void testUUIDBehaviorRemoveFromRenamed3() throws Exception {
        // create authorizable
        User u = getUserManager().createUser(uid, null, new PrincipalImpl(uid), intermediatePath);
        String originalPath = u.getPath();
        importSession.save();

        // we need to include the new node name in the sysview import, so that the importer uses the correct name.
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"" + randomNodeName + "\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:disabled\" sv:type=\"String\"><sv:value>disabledUser</sv:value></sv:property>" +
                "</sv:node>";

        doImport(getTargetPath(), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING);

        Authorizable newUser = getUserManager().getAuthorizable(uid);

        // replace should change the original path
        String expectedPath = getTargetPath() + '/' + randomNodeName;
        assertEquals("user path", expectedPath, newUser.getPath());
        assertFalse((Text.getRelativeParent(originalPath,1) + '/' + randomNodeName).equals(newUser.getPath()));

        Node n = importSession.getNode(newUser.getPath());
        assertTrue(n.hasProperty(UserConstants.REP_AUTHORIZABLE_ID));
        assertEquals(UserConstants.REP_AUTHORIZABLE_ID, randomNodeName, n.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString());

        // saving changes of the import -> must fail as the authorizable ID
        // has been modified (it no longer represents the correct ID due to the
        // fact that in JR 2.x the node name MUST contain the id.
        try {
            importSession.save();
            fail("Importing an authorizable with mismatch between authorizableId and uuid must fail.");
        } catch (ConstraintViolationException e) {
            // success
            assertTrue(e.getMessage().contains("OakConstraint0021"));
        }
    }
}