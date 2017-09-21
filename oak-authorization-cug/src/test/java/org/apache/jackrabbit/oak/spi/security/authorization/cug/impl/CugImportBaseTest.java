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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.security.AccessControlException;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.CommitFailedException;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CugImportBaseTest {

    static final String TEST_NODE_NAME = "testNode";
    static final String TEST_NODE_PATH = "/testNode";
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

    static final String XML_NESTED_CUG_POLICY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
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
        SecurityProvider securityProvider = new CugSecurityProvider(config);
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

    @Nonnull
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
        doImport(parentPath, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }

    void doImport(String parentPath, String xml, int importUUIDBehavior) throws Exception {
        doImport(getImportSession(), parentPath, xml, importUUIDBehavior);
    }

    void doImport(Session importSession, String parentPath, String xml, int importUUIDBehavior) throws Exception {
        InputStream in;
        if (xml.charAt(0) == '<') {
            in = new ByteArrayInputStream(xml.getBytes());
        } else {
            in = getClass().getResourceAsStream(xml);
        }
        try {
            importSession.importXML(parentPath, in, importUUIDBehavior);
        } finally {
            in.close();
        }
    }

    static void assertPrincipalNames(@Nonnull Set<String> expectedPrincipalNames, @Nonnull Value[] principalNames) {
        assertEquals(expectedPrincipalNames.size(), principalNames.length);
        Set<String> result = ImmutableSet.copyOf(Iterables.transform(ImmutableSet.copyOf(principalNames), new Function<Value, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Value principalName) {
                try {
                    return (principalName == null) ? null : principalName.getString();
                } catch (RepositoryException e) {
                    throw new IllegalStateException(e);
                }
            }
        }));
        assertEquals(expectedPrincipalNames, result);
    }

    @Test
    public void testCugValidPrincipals() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        Node targetNode = getTargetNode();
        targetNode.addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_CUG_POLICY);
        adminSession.save();
    }

    @Test
    public void testCugValidPrincipalsNoMixin() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        doImport(getTargetPath(), XML_CUG_POLICY);
        try {
            adminSession.save();
            fail();
        } catch (AccessControlException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof CommitFailedException);
            assertTrue(((CommitFailedException) cause).isAccessControlViolation());
            assertEquals(22, ((CommitFailedException) cause).getCode());
        }

    }

    @Test
    public void testNodeWithCugValidPrincipals() throws Exception {
        testGroup = ((JackrabbitSession) adminSession).getUserManager().createGroup(new PrincipalImpl(TEST_GROUP_PRINCIPAL_NAME));
        adminSession.save();

        doImport(getTargetPath(), XML_CHILD_WITH_CUG);
        adminSession.save();
    }

    @Test
    public void testCugWithoutPrincipalNames() throws Exception {
        String xmlCugPolicyWithoutPrincipals = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "</sv:node>";
        doImport(getTargetPath(), xmlCugPolicyWithoutPrincipals);

        assertFalse(getTargetNode().hasNode(CugConstants.REP_CUG_POLICY));
        getImportSession().save();
    }

    @Test
    public void testCugWithEmptyPrincipalNames() throws Exception {
        String xmlCugPolicyEmptyPrincipals = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:cugPolicy\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\"></sv:property>" +
            "</sv:node>";

        getTargetNode().addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), xmlCugPolicyEmptyPrincipals);
        getImportSession().save();

        String propPath = getTargetPath() + "/" + CugConstants.REP_CUG_POLICY + "/" + CugConstants.REP_PRINCIPAL_NAMES;
        assertTrue(getImportSession().propertyExists(propPath));
        assertArrayEquals(new Value[0], getImportSession().getProperty(propPath).getValues());
    }

    @Test
    public void testNestedCug() throws Exception {
        try {
            doImport(getTargetPath(), XML_NESTED_CUG_POLICY);
            fail();
        } catch (ConstraintViolationException e) {
            // success
        } finally {
            getImportSession().refresh(false);
        }
    }

    @Test
    public void testNestedCugWithMixin() throws Exception {
        getTargetNode().addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_NESTED_CUG_POLICY);

        assertTrue(getTargetNode().hasNode(CugConstants.REP_CUG_POLICY));

        Node cugPolicy = getTargetNode().getNode(CugConstants.REP_CUG_POLICY);
        assertTrue(cugPolicy.hasProperty(CugConstants.REP_PRINCIPAL_NAMES));
        assertFalse(cugPolicy.hasNode(CugConstants.REP_CUG_POLICY));
    }

    @Test
    public void testNestedCugSave() throws Exception {
        getTargetNode().addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), XML_NESTED_CUG_POLICY);

        assertTrue(getTargetNode().hasNode(CugConstants.REP_CUG_POLICY));

        Node cugPolicy = getTargetNode().getNode(CugConstants.REP_CUG_POLICY);
        assertTrue(cugPolicy.hasProperty(CugConstants.REP_PRINCIPAL_NAMES));
        assertFalse(cugPolicy.hasNode(CugConstants.REP_CUG_POLICY));
    }

    @Test
    public void testCugWithInvalidName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"someOtherNode\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:CugPolicy</sv:value></sv:property>" +
                "<sv:property sv:name=\"rep:principalNames\" sv:type=\"String\" sv:multiple=\"true\">" +
                    "<sv:value>" + EveryonePrincipal.NAME + "</sv:value>" +
                "</sv:property>" +
            "</sv:node>";

        getTargetNode().addMixin(CugConstants.MIX_REP_CUG_MIXIN);
        doImport(getTargetPath(), xml);

        try {
            getImportSession().save();
            fail();
        } catch (ConstraintViolationException e) {
            // success
        } finally {
            getImportSession().refresh(false);
        }
    }

    @Test
    public void testCugAtUnsupportedPath() throws Exception {
        doImport("/", XML_CHILD_WITH_CUG);

        getImportSession().save();

        assertTrue(getImportSession().getRootNode().hasNode("child"));
        assertFalse(getImportSession().getRootNode().hasNode("child/rep:cugPolicy"));
    }
}