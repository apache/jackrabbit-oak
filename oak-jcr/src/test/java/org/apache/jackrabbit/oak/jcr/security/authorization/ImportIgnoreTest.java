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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertEquals;

public class ImportIgnoreTest {

    private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:policy\" " +
            "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
            "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
            "<sv:value>rep:ACL</sv:value>" +
            "</sv:property>" +
            "<sv:node sv:name=\"allow\">" +
            "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
            "<sv:value>rep:GrantACE</sv:value>" +
            "</sv:property>" +
            "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
            "<sv:value>unknownprincipal</sv:value>" +
            "</sv:property>" +
            "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
            "<sv:value>jcr:write</sv:value>" +
            "</sv:property>" +
            "</sv:node>" +
            "</sv:node>";

    private Repository repo;
    protected Session adminSession;
    protected Node target;

    @Before
    public void before() throws Exception {
        String importBehavior = getImportBehavior();
        SecurityProvider securityProvider;
        if (importBehavior != null) {
            Map<String, String> params = new HashMap<String, String>();
            params.put(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, getImportBehavior());
            ConfigurationParameters config = ConfigurationParameters.of(AuthorizationConfiguration.NAME, ConfigurationParameters.of(params));

            securityProvider = new SecurityProviderBuilder().with(config).build();
        } else {
            securityProvider = new SecurityProviderBuilder().build();
        }

        QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setFailTraversal(true);

        Jcr jcr = new Jcr();
        jcr.with(securityProvider);
        jcr.with(queryEngineSettings);
        repo = jcr.createRepository();
        adminSession = repo.login(new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray()));

        target = adminSession.getRootNode().addNode("nodeName1");
        target.addMixin("rep:AccessControllable");
        adminSession.save();
    }

    @After
    public void after() throws Exception {
        if (adminSession != null) {
            adminSession.refresh(false);
            adminSession.logout();
        }
        repo = dispose(repo);
    }

    protected String getImportBehavior() {
        return ImportBehavior.NAME_IGNORE;
    }

    protected void runImport() throws RepositoryException, IOException {
        String path = target.getPath();

        InputStream in = new ByteArrayInputStream(XML.getBytes("UTF-8"));
        adminSession.importXML(target.getPath(), in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);

    }

    @Test
    public void testImportUnknownPrincipal() throws Exception {
        try {
            runImport();

            AccessControlManager acMgr = adminSession.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(target.getPath());

            assertEquals(1, policies.length);
            assertEquals(0, ((AccessControlList) policies[0]).getAccessControlEntries().length);
        } finally {
            adminSession.refresh(false);
        }
    }
}
