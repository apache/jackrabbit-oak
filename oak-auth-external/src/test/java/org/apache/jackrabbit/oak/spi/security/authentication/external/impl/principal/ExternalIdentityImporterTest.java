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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import javax.annotation.Nonnull;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test XML import of external users/groups with protection of external identity
 * properties turned on.
 */
public class ExternalIdentityImporterTest {

    public static final String XML_EXTERNAL_USER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
            "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
            "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrinc</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:externalId\" sv:type=\"String\"><sv:value>idp;ext-t</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:lastSynced\" sv:type=\"Date\"><sv:value>2016-05-03T10:03:08.061+02:00</sv:value></sv:property>" +
            "</sv:node>";

    public static final String XML_EXTERNAL_USER_WITH_PRINCIPAL_NAMES = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<sv:node sv:name=\"t\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
            "   <sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:User</sv:value></sv:property>" +
            "   <sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>e358efa4-89f5-3062-b10d-d7316b65649e</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:authorizableId\" sv:type=\"String\"><sv:value>t</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>tPrinc</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:externalId\" sv:type=\"String\"><sv:value>idp;ext-t</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:externalPrincipalNames\" sv:type=\"String\"><sv:value>grPrinc</sv:value><sv:value>gr2Princ</sv:value></sv:property>" +
            "   <sv:property sv:name=\"rep:lastSynced\" sv:type=\"Date\"><sv:value>2016-05-03T10:03:08.061+02:00</sv:value></sv:property>" +
            "</sv:node>";

    private Repository repo;
    private SecurityProvider securityProvider;
    private JackrabbitSession session;

    @Before
    public void before() throws Exception {
        securityProvider = TestSecurityProvider.newTestSecurityProvider(getConfigurationParameters(),
                new ExternalPrincipalConfiguration());
        QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setFailTraversal(true);

        Jcr jcr = new Jcr();
        jcr.with(securityProvider);
        jcr.with(queryEngineSettings);
        repo = jcr.createRepository();
    }

    @After
    public void after() throws Exception {
        try {
            if (session != null) {
                session.logout();
            }
        } finally {
            if (repo instanceof JackrabbitRepository) {
                ((JackrabbitRepository) repo).shutdown();
            }
        }
    }

    @Nonnull
    ConfigurationParameters getConfigurationParameters() {
        return ConfigurationParameters.EMPTY;
    }

    JackrabbitSession createSession(boolean isSystem) throws Exception {
        if (isSystem) {
            session = (JackrabbitSession) Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<Session>() {
                @Override
                public Session run() throws RepositoryException {
                    return repo.login(null, null);
                }
            });
        } else {
            session = (JackrabbitSession) repo.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));
        }
        return session;
    }

    Node doImport(Session importSession, String parentPath, String xml) throws Exception {
        InputStream in;
        if (xml.charAt(0) == '<') {
            in = new ByteArrayInputStream(xml.getBytes());
        } else {
            in = getClass().getResourceAsStream(xml);
        }
        try {
            importSession.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
            return importSession.getNode(parentPath);
        } finally {
            in.close();
        }
    }

    static void assertHasProperties(@Nonnull Node node, @Nonnull String... propertyNames) throws Exception {
        for (String pN : propertyNames) {
            assertTrue(node.hasProperty(pN));
        }
    }

    static void assertNotHasProperties(@Nonnull Node node, @Nonnull String... propertyNames) throws Exception {
        for (String pN : propertyNames) {
            assertFalse(node.hasProperty(pN));
        }
    }

    @Test
    public void importExternalUser() throws Exception {
        Node parent = doImport(createSession(false), UserConstants.DEFAULT_USER_PATH, XML_EXTERNAL_USER);
        assertHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_ID, ExternalIdentityConstants.REP_LAST_SYNCED);
        assertNotHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
    }

    @Test
    public void importExternalUserAsSystem() throws Exception {
        Node parent = doImport(createSession(true), UserConstants.DEFAULT_USER_PATH, XML_EXTERNAL_USER);
        assertHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_ID, ExternalIdentityConstants.REP_LAST_SYNCED);
        assertNotHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
    }

    @Test
    public void importExternalUserWithPrincipalNames() throws Exception {
        Node parent = doImport(createSession(false), UserConstants.DEFAULT_USER_PATH, XML_EXTERNAL_USER_WITH_PRINCIPAL_NAMES);
        assertHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_ID);
        assertNotHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_LAST_SYNCED, ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
    }

    @Test
    public void importExternalUserWithPrincipalNamesAsSystem() throws Exception {
        Node parent = doImport(createSession(true), UserConstants.DEFAULT_USER_PATH, XML_EXTERNAL_USER_WITH_PRINCIPAL_NAMES);
        assertHasProperties(parent.getNode("t"), ExternalIdentityConstants.REP_EXTERNAL_ID, ExternalIdentityConstants.REP_LAST_SYNCED, ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES);
    }
}