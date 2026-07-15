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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.CredentialExpiredException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing user import with default {@link org.apache.jackrabbit.oak.spi.xml.ImportBehavior}
 * and initial-pw-change option enabled
 */
public class UserImportInitialPwChangeTest extends AbstractImportTest {

    @Override
    protected @NotNull String getTargetPath() {
        return USERPATH;
    }

    @Override
    protected @NotNull String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    protected @NotNull ConfigurationParameters getConfigurationParameters() {
        HashMap<String, Object> userParams = new HashMap<String, Object>() {{
            put(UserConstants.PARAM_PASSWORD_INITIAL_CHANGE, true);
        }};
        return ConfigurationParameters.of(UserConfiguration.NAME, ConfigurationParameters.of(userParams));
    }

    @Test
    public void testImportUserWithoutPwdNode() throws Exception {
        // import user
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>"+
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable authorizable = getUserManager().getAuthorizable("t");
        Node userNode = getImportSession().getNode(authorizable.getPath());
        assertFalse(userNode.hasNode(UserConstants.REP_PWD));
        getImportSession().save();

        try {
            getImportSession().getRepository().login(new SimpleCredentials("t", "t".toCharArray())).logout();
            fail("must be prompted for initial pw change!");
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof CredentialExpiredException);
        }
    }

    @Test
    public void testImportExistingUserWithoutPwdNode() throws Exception {
        User user = getUserManager().createUser("t", "t");
        getImportSession().save();
        String userPath = user.getPath();
        String uuid = getImportSession().getProperty(PathUtils.concat(userPath, JcrConstants.JCR_UUID)).getString();

        // import user
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\""+PathUtils.getName(userPath)+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>"+uuid+"</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>"+
                "   <sv:node sv:name=\"profile\">" +
                "      <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>"+ JcrConstants.NT_UNSTRUCTURED +"</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(PathUtils.getParentPath(userPath), xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING);

        Authorizable authorizable = getUserManager().getAuthorizable("t");
        Node userNode = getImportSession().getNode(authorizable.getPath());
        assertFalse(userNode.hasNode(UserConstants.REP_PWD));
        getImportSession().save();

        try {
            getImportSession().getRepository().login(new SimpleCredentials("t", "t".toCharArray())).logout();
            fail("must be prompted for initial pw change!");
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof CredentialExpiredException);
        }
    }

    @Test
    public void testImportUserWithPwdNodeMissingLastModified() throws Exception {
        // import user
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>"+
                "   <sv:node sv:name=\"" + UserConstants.REP_PWD + "\">" +
                "      <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>"+ UserConstants.NT_REP_PASSWORD +"</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable authorizable = getUserManager().getAuthorizable("t");
        Node userNode = getImportSession().getNode(authorizable.getPath());
        assertTrue(userNode.hasNode(UserConstants.REP_PWD));
        Node pwdNode = userNode.getNode(UserConstants.REP_PWD);
        assertFalse(pwdNode.hasProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED));
        getImportSession().save();

        try {
            getImportSession().getRepository().login(new SimpleCredentials("t", "t".toCharArray())).logout();
            fail("must be prompted for initial pw change!");
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof CredentialExpiredException);
        }
    }

    @Test
    public void testImportUserWithPwdNodeWithLastModified() throws Exception {
        // import user
        long now = System.currentTimeMillis();
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
                "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:password\" sv:type=\"String\"><sv:value>{sha1}8efd86fb78a56a5145ed7739dcb00c78581c5375</sv:value></sv:property>" +
                "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>"+
                "   <sv:node sv:name=\"" + UserConstants.REP_PWD + "\">" +
                "      <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>"+ UserConstants.NT_REP_PASSWORD +"</sv:value></sv:property>" +
                "      <sv:property sv:name=\"" + UserConstants.REP_PASSWORD_LAST_MODIFIED + "\" sv:type=\"Long\"><sv:value>"+now+"</sv:value></sv:property>" +
                "   </sv:node>" +
                "</sv:node>";

        doImport(USERPATH, xml);

        Authorizable authorizable = getUserManager().getAuthorizable("t");
        Node userNode = getImportSession().getNode(authorizable.getPath());
        assertTrue(userNode.hasNode(UserConstants.REP_PWD));
        Node pwdNode = userNode.getNode(UserConstants.REP_PWD);
        assertTrue(pwdNode.hasProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED));
        assertEquals(now, pwdNode.getProperty(UserConstants.REP_PASSWORD_LAST_MODIFIED).getLong());
        getImportSession().save();

        // login must succeed
        getImportSession().getRepository().login(new SimpleCredentials("t", "t".toCharArray())).logout();
    }
}
