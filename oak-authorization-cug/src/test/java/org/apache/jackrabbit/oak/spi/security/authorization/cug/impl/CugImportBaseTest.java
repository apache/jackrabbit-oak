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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Set;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlException;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConstants.MIX_REP_CUG_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConstants.NT_REP_CUG_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConstants.REP_CUG_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.cug.impl.CugConstants.REP_PRINCIPAL_NAMES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class CugImportBaseTest {

    private static final String TEST_NODE_NAME = "testNode";
    private static final String TEST_NODE_PATH = "/testNode";
    static final String TEST_GROUP_PRINCIPAL_NAME = "testPrincipal";

    static final String XML_CUG_POLICY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                    "<sv:value>" + TEST_GROUP_PRINCIPAL_NAME + "</sv:value>" +
                    "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                "</sv:property>" +
            "</sv:node>";

    static final String XML_CHILD_WITH_CUG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"child\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>oak:Unstructured</sv:value></sv:property>" +
                "<sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\"><sv:value>rep:CugMixin</sv:value></sv:property>" +
                "<sv:node sv:name=\"rep:cugPolicy\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                    "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                    "<sv:value>" + TEST_GROUP_PRINCIPAL_NAME + "</sv:value>" +
                    "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_NESTED_CUG_POLICY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                    "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"rep:cugPolicy\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                    "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                            "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private Repository repo;
    private Session adminSession;
    private Group testGroup;

    @Before
    public void before() throws Exception {
        ConfigurationParameters config = getConfigurationParameters();
        SecurityProvider securityProvider = CugSecurityProvider.newTestSecurityProvider(config);
        QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setFailTraversal(true);

        Jcr jcr = new Jcr();
        jcr.with(securityProvider);
        jcr.with(queryEngineSettings);
        repo = jcr.createRepository();
        adminSession = repo.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));

        adminSession.getRootNode().addNode(TEST_NODE_NAME, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        adminSession.save();
    }

    @After
    public void after() throws Exception {
        try {
            getImportSession().refresh(false);
            adminSession.refresh(false);

            adminSession.getNode(TEST_NODE_PATH).remove();
            if (testGroup != null) {
                testGroup.remove();
            }
            adminSession.save();
        } finally {
            adminSession.logout();
            if (repo instanceof JackrabbitRepository) {
                ((JackrabbitRepository) repo).shutdown();
            }
            repo = null;
        }
    }

    @NotNull
    private ConfigurationParameters getConfigurationParameters() {
        String importBehavior = getImportBehavior();
        if (importBehavior != null) {
            ConfigurationParameters params = ConfigurationParameters.of(
                    ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior(),
                    CugConstants.PARAM_CUG_SUPPORTED_PATHS, new String[] {TEST_NODE_PATH});
            return ConfigurationParameters.of(AuthorizationConfiguration.NAME, params);
        } else {
            return ConfigurationParameters.EMPTY;
        }
    }

    abstract String getImportBehavior();

    String getTargetPath() {
        return TEST_NODE_PATH;
    }

    Session getImportSession() {
        return adminSession;
    }

    Node getTargetNode() throws RepositoryException {
        return getImportSession().getNode(getTargetPath());
    }

    void doImport(String parentPath, String xml) throws Exception {
        doImport(getImportSession(), parentPath, xml);
    }

    private void doImport(Session importSession, String parentPath, String xml) throws Exception {
        InputStream in;
        if (xml.charAt(0) == '<') {
            in = new ByteArrayInputStream(xml.getBytes());
        } else {
            in = getClass().getResourceAsStream(xml);
        }
        try {
            importSession.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
        } finally {
            in.close();
        }
    }

    static void assertPrincipalNames(@NotNull Set<String> expectedPrincipalNames, @NotNull Value[] principalNames) {
        assertEquals(expectedPrincipalNames.size(), principalNames.length);
        Set<String> result = CollectionUtils.toSet(Iterables.transform(Set.of(principalNames), principalName -> {
            try {
                return principalName.getString();
            } catch (RepositoryException e) {
                throw new IllegalStateException(e);
            }
        }));
        assertEquals(expectedPrincipalNames, result);
    }

    @Test
    public void testCugValidPrincipals() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        Node targetNode = getTargetNode();
        targetNode.addMixin(MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_CUG_POLICY);
        adminSession.save();

        assertTrue(targetNode.hasNode(REP_CUG_POLICY));
        Node n = targetNode.getNode(REP_CUG_POLICY);
        assertEquals(NT_REP_CUG_POLICY, n.getPrimaryNodeType().getName());

        assertTrue(n.hasProperty(REP_PRINCIPAL_NAMES));
        Property p = n.getProperty(REP_PRINCIPAL_NAMES);
        ValueFactory vf = adminSession.getValueFactory();
        assertArrayEquals(new Value[] {vf.createValue(TEST_GROUP_PRINCIPAL_NAME), vf.createValue(EveryonePrincipal.NAME)}, p.getValues());
    }

    @Test(expected = AccessControlException.class)
    public void testCugValidPrincipalsNoMixin() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        doImport(getTargetPath(), XML_CUG_POLICY);
        try {
            adminSession.save();
        } catch (AccessControlException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof CommitFailedException);
            assertTrue(((CommitFailedException) cause).isAccessControlViolation());
            assertEquals(22, ((CommitFailedException) cause).getCode());
            throw e;
        }

    }

    @Test
    public void testNodeWithCugValidPrincipals() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        doImport(getTargetPath(), XML_CHILD_WITH_CUG);
        adminSession.save();

        Node targetNode = getTargetNode();
        assertTrue(targetNode.hasNode("child"));

        Node child = getTargetNode().getNode("child");
        assertTrue(child.hasNode(REP_CUG_POLICY));
    }

    @Test
    public void testCugWithoutPrincipalNames() throws Exception {
        String xmlCugPolicyWithoutPrincipals = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "</sv:node>";
        doImport(getTargetPath(), xmlCugPolicyWithoutPrincipals);

        assertFalse(getTargetNode().hasNode(REP_CUG_POLICY));
        getImportSession().save();
    }

    @Test
    public void testCugWithEmptyPrincipalNames() throws Exception {
        String xmlCugPolicyEmptyPrincipals = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\"></sv:property>" +
            "</sv:node>";

        getTargetNode().addMixin(MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), xmlCugPolicyEmptyPrincipals);
        getImportSession().save();

        String propPath = getTargetPath() + "/" + REP_CUG_POLICY + "/" + REP_PRINCIPAL_NAMES;
        assertTrue(getImportSession().propertyExists(propPath));
        assertArrayEquals(new Value[0], getImportSession().getProperty(propPath).getValues());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNestedCug() throws Exception {
        doImport(getTargetPath(), XML_NESTED_CUG_POLICY);
    }

    @Test
    public void testNestedCugWithMixin() throws Exception {
        getTargetNode().addMixin(MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_NESTED_CUG_POLICY);

        assertTrue(getTargetNode().hasNode(REP_CUG_POLICY));

        Node cugPolicy = getTargetNode().getNode(REP_CUG_POLICY);
        assertTrue(cugPolicy.hasProperty(REP_PRINCIPAL_NAMES));
        assertFalse(cugPolicy.hasNode(REP_CUG_POLICY));
    }

    @Test
    public void testNestedCugSave() throws Exception {
        getTargetNode().addMixin(MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_NESTED_CUG_POLICY);
        getImportSession().save();

        assertTrue(getTargetNode().hasNode(REP_CUG_POLICY));

        Node cugPolicy = getTargetNode().getNode(REP_CUG_POLICY);
        assertTrue(cugPolicy.hasProperty(REP_PRINCIPAL_NAMES));
        assertFalse(cugPolicy.hasNode(REP_CUG_POLICY));
    }

    @Test(expected = ConstraintViolationException.class)
    public void testCugWithInvalidName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"someOtherNode\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                    "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                "</sv:property>" +
            "</sv:node>";

        getTargetNode().addMixin(MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), xml);

        // save must fail
        getImportSession().save();
    }

    @Test
    public void testCugAtUnsupportedPath() throws Exception {
        doImport("/", XML_CHILD_WITH_CUG);
        getImportSession().save();

        assertTrue(getImportSession().getRootNode().hasNode("child"));
        assertFalse(getImportSession().getRootNode().hasNode("child/rep:cugPolicy"));
    }
}
