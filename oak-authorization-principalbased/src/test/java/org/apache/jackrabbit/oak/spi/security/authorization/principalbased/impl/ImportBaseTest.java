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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.nodetype.ConstraintViolationException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.security.Principal;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.MIX_REP_PRINCIPAL_BASED_MIXIN;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_ENTRY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.NT_REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_EFFECTIVE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_RESTRICTIONS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class ImportBaseTest extends AbstractPrincipalBasedTest {

    private Repository repo;
    private JackrabbitSession adminSession;

    private String testPath;
    private String uid;
    private Principal testPrincipal;
    private String testPrincipalName;

    @Before
    public void before() throws Exception {
        super.before();

        Jcr jcr = new Jcr();
        jcr.with(getSecurityProvider());
        jcr.with(getQueryEngineSettings());
        repo = jcr.createRepository();
        adminSession = (JackrabbitSession) repo.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));

        User u = getUserManager().createSystemUser("testSystemUser" + UUID.randomUUID(), getNamePathMapper().getJcrPath(INTERMEDIATE_PATH));
        adminSession.save();
        uid = u.getID();
        testPath = u.getPath();
        testPrincipal = u.getPrincipal();
        testPrincipalName = testPrincipal.getName();
    }

    @After
    public void after() throws Exception {
        try {
            adminSession.refresh(false);
            User u = getUserManager().getAuthorizable(uid, User.class);
            if (u != null) {
                u.remove();
            }
            adminSession.removeItem(SUPPORTED_PATH);
            adminSession.save();
        } finally {
            adminSession.logout();
            if (repo instanceof JackrabbitRepository) {
                ((JackrabbitRepository) repo).shutdown();
            }
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME,
                ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior())
        );
    }

    abstract String getImportBehavior();

    UserManager getUserManager() throws RepositoryException {
        return adminSession.getUserManager();
    }

    JackrabbitSession getSession() {
        return adminSession;
    }

    JackrabbitAccessControlManager getAccessControlManager() throws RepositoryException {
        return (JackrabbitAccessControlManager) adminSession.getAccessControlManager();
    }

    void doImport(String parentPath, String xml) throws Exception {
        doImport(adminSession, parentPath, xml, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
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

    @Test(expected = ConstraintViolationException.class)
    public void testPolicyWithoutPrincipalName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                "</sv:node>";
        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);

        assertTrue(adminSession.getNode(testPath).hasNode(REP_PRINCIPAL_POLICY));
        adminSession.save();
    }

    @Test
    public void testEmptyPolicyMissingMixinType() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                "</sv:node>";
        doImport(testPath, xml);

        assertTrue(adminSession.getNode(testPath).isNodeType(MIX_REP_PRINCIPAL_BASED_MIXIN));
        assertTrue(adminSession.getNode(testPath).hasNode(REP_PRINCIPAL_POLICY));
        adminSession.save();
    }

    @Test
    public void testEmptyPolicy() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                "</sv:node>";
        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager());
        assertTrue(policy.isEmpty());
        adminSession.save();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmptyPolicyWithInvalidNodeName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\"someOtherNode\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                "</sv:node>";

        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);
        adminSession.save();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmptyPolicyPrincipalNameTypeMismatch() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"Name\"><sv:value>" + getTestUser().getPrincipal().getName() + "</sv:value></sv:property>" +
                "</sv:node>";
        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmptyPolicyPrincipalNameMultiple() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\" sv:multiple=\"true\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                "</sv:node>";
        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNestedPolicy() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                     "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                         "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                         "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>" + testPrincipalName + "</sv:value></sv:property>" +
                    "</sv:node>" +
                "</sv:node>";

        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEmptyPolicyWithInvalidPrincipalName() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+getTestUser().getPrincipal().getName()+"</sv:value></sv:property>" +
                "</sv:node>";

        adminSession.getNode(testPath).addMixin(MIX_REP_PRINCIPAL_BASED_MIXIN);
        doImport(testPath, xml);
        //adminSession.save();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEntryWithMissingEffectivePath() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEntryWithEffectivePathTypeMismatch() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"String\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                        "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
    }

    @Test(expected = RepositoryException.class)
    public void testEntryWithEffectivePathMV() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\" sv:multiple=\"true\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                        "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEntryWithMissingPrivileges() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
    }

    @Test(expected = ConstraintViolationException.class)
    public void testEntryWithPrivilegesTypeMismatch() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"String\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
    }

    @Test
    public void testEntryWithPrivilegesSingleValue() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
        adminSession.save();

        Node policyNode = adminSession.getNode(PathUtils.concat(testPath, REP_PRINCIPAL_POLICY));
        Node entry = Iterators.<Node>getOnlyElement(policyNode.getNodes());
        assertTrue(entry.isNodeType(NT_REP_PRINCIPAL_ENTRY));
        assertTrue(entry.getProperty(REP_PRIVILEGES).isMultiple());
    }

    @Test
    public void testTwoIdenticalEntries() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                        "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                    "<sv:node sv:name=\"entry1\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                        "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
        adminSession.save();

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager());
        assertEquals(1, policy.size());
    }

    @Test
    public void testTwoDifferentEntries() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                    "<sv:node sv:name=\"entry1\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value></sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_NAMESPACE_MANAGEMENT+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
        adminSession.save();

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager());
        assertEquals(2, policy.size());
        List<PrincipalPolicyImpl.EntryImpl> entries = policy.getEntries();
        assertEquals("/content", entries.get(0).getEffectivePath());
        assertNull(entries.get(1).getEffectivePath());
    }

    @Test
    public void testEffectivePathInRestriction() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                        "<sv:node sv:name=\""+REP_RESTRICTIONS+"\">" +
                            "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_RESTRICTIONS+"</sv:value></sv:property>" +
                            "<sv:property sv:name=\""+ AccessControlConstants.REP_NODE_PATH+"\" sv:type=\"String\"><sv:value>/content</sv:value></sv:property>" +
                         "</sv:node>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
        adminSession.save();

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager());
        assertEquals(1, policy.size());
        PrincipalPolicyImpl.EntryImpl entry = policy.getEntries().get(0);
        assertEquals("/content", entry.getOakPath());
        assertTrue(entry.getRestrictions().isEmpty());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUnsupportedPath() throws Exception {
        // move user node outside of supported path.
        String unsupportedPath = PathUtils.concat(PathUtils.getAncestorPath(testPath, 2), PathUtils.getName(testPath));
        adminSession.move(testPath, unsupportedPath);
        adminSession.save();

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>";

        // import will leave incomplete policy
        doImport(unsupportedPath, xml);
        adminSession.save();
    }

    @Test
    public void testEntryWithRestriction() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<sv:node sv:name=\""+REP_PRINCIPAL_POLICY+"\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                    "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_POLICY+"</sv:value></sv:property>" +
                    "<sv:property sv:name=\""+REP_PRINCIPAL_NAME+"\" sv:type=\"String\"><sv:value>"+testPrincipalName+"</sv:value></sv:property>" +
                    "<sv:node sv:name=\"entry0\">" +
                        "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_PRINCIPAL_ENTRY+"</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_EFFECTIVE_PATH+"\" sv:type=\"Path\"><sv:value>/content</sv:value></sv:property>" +
                        "<sv:property sv:name=\""+REP_PRIVILEGES+"\" sv:type=\"Name\" sv:multiple=\"true\">" +
                            "<sv:value>"+JCR_READ+"</sv:value>" +
                        "</sv:property>" +
                        "<sv:node sv:name=\""+REP_RESTRICTIONS+"\">" +
                            "<sv:property sv:name=\""+JCR_PRIMARYTYPE+"\" sv:type=\"Name\"><sv:value>"+NT_REP_RESTRICTIONS+"</sv:value></sv:property>" +
                            "<sv:property sv:name=\""+ REP_GLOB+"\" sv:type=\"String\"><sv:value>*</sv:value></sv:property>" +
                         "</sv:node>" +
                    "</sv:node>" +
                "</sv:node>";
        doImport(testPath, xml);
        adminSession.save();

        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, getAccessControlManager());
        assertEquals(1, policy.size());
        PrincipalPolicyImpl.EntryImpl entry = policy.getEntries().get(0);
        assertEquals("*", entry.getRestriction(REP_GLOB).getString());
    }
}
