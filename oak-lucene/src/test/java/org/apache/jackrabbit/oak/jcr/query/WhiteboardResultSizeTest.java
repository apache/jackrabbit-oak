/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.query;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.LuceneOakRepositoryStub;
import org.apache.jackrabbit.oak.query.SessionQuerySettingsProvider;
import org.apache.jackrabbit.oak.query.SessionQuerySettingsProviderService;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.test.RepositoryStub;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.security.Privilege;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Performs the same queries as {@link ResultSizeTest#testResultSize()} and expects the same results, with the only
 * differences being the registration of a {@link org.apache.jackrabbit.oak.query.SessionQuerySettingsProvider} service
 * in the whiteboard, and the runtime reconfiguration of the service before each login, in place of the calls to
 * {@code System.setProperty("oak.fastQuerySize", true/false)}.
 */
public class WhiteboardResultSizeTest {

    /**
     * Non-static inner class extending RepositoryStub which is intended to be created directly by a test
     * and not by the TCK RepositoryHelper, purely so that it can mirror the behavior of
     * {@link org.apache.jackrabbit.oak.jcr.query.ResultSizeTest} with the only differences being the registration of
     * a {@link org.apache.jackrabbit.oak.query.SessionQuerySettingsProvider} service in the whiteboard, and the
     * runtime reconfiguration of the service before each login, in place of the calls to
     * {@code System.setProperty("oak.fastQuerySize", true/false)}.
     */
    public class OakResultSizeStub extends LuceneOakRepositoryStub {
        public OakResultSizeStub(Properties settings) throws RepositoryException {
            super(settings);
        }

        @Override
        protected void preCreateRepository(Jcr jcr, Whiteboard whiteboard) {
            // register the SessionQuerySettingsProvider service before creation of the repository.
            whiteboard.register(SessionQuerySettingsProvider.class,
                    settingsProviderService,
                    Collections.emptyMap());
            super.preCreateRepository(jcr, whiteboard);
        }
    }

    @SessionQuerySettingsProviderService.Configuration(directCountsPrincipals = {"admin"})
    static class AdminAllowed {
        /* class which hosts a config annotation for reflection */
    }

    @SessionQuerySettingsProviderService.Configuration
    static class NoneAllowed {
        /* class which hosts a config annotation for reflection */
    }

    private SessionQuerySettingsProviderService settingsProviderService =
            new SessionQuerySettingsProviderService();
    private RepositoryStub stub;
    private volatile Repository repository;
    private volatile Session adminSession;

    /**
     * Reconfigures the singleton {@link org.apache.jackrabbit.oak.query.SessionQuerySettingsProviderService} instance between queries.
     *
     * @param config a config annotation
     * @throws Exception for reflection errors
     */
    private void reconfigure(SessionQuerySettingsProviderService.Configuration config) throws Exception {
        Method method = SessionQuerySettingsProviderService.class.getDeclaredMethod("configure",
                SessionQuerySettingsProviderService.Configuration.class);
        method.setAccessible(true);
        method.invoke(settingsProviderService, config);
    }

    protected Session getAdminSession() throws Exception {
        if (adminSession == null) {
            adminSession = createAdminSession();
            AccessControlUtils.addAccessControlEntry(adminSession, "/", EveryonePrincipal.getInstance(),
                    new String[]{Privilege.JCR_READ}, true);
            adminSession.save();
        }
        return adminSession;
    }

    private void createData() throws Exception {
        Session session = getAdminSession();
        Node testRootNode = session.getRootNode().addNode("testroot", "nt:unstructured");
        for (int i = 0; i < 200; i++) {
            Node n = testRootNode.addNode("node" + i);
            n.setProperty("text", "Hello World");
        }
        session.save();
    }

    @Test
    public void testResultSize() throws Exception {
        doTestResultSize(false);
    }

    private void doTestResultSize(boolean aggregateAtQueryTime) throws Exception {
        createData();
        int expectedForUnion = 400;
        int expectedForTwoConditions = aggregateAtQueryTime ? 400 : 200;
        doTestResultSize(false, expectedForTwoConditions);
        doTestResultSize(true, expectedForUnion);
    }

    /**
     * Compare with {@code org.apache.jackrabbit.oak.jcr.query.ResultSizeTest#doTestResultSize(boolean, int)}.
     */
    private void doTestResultSize(boolean union, int expected) throws Exception {
        Session session = null;
        QueryManager qm;
        Query q;
        long result;
        NodeIterator it;
        StringBuilder buff;

        String xpath;
        if (union) {
            xpath = "/jcr:root//*[jcr:contains(@text, 'Hello') or jcr:contains(@text, 'World')]";
        } else {
            xpath = "/jcr:root//*[jcr:contains(@text, 'Hello World')]";
        }

        final CompletableFuture<String> fastSizeResult = new CompletableFuture<>();

        reconfigure(AdminAllowed.class.getAnnotation(SessionQuerySettingsProviderService.Configuration.class));
        try {
            session = this.createAdminSession();
            assertEquals("nt:unstructured",
                    session.getNode("/testroot").getPrimaryNodeType().getName());
            qm = session.getWorkspace().getQueryManager();
            q = qm.createQuery(xpath, "xpath");
            it = q.execute().getNodes();
            result = it.getSize();
            assertTrue("size: " + result + " expected around " + expected,
                    result > expected - 50 &&
                            result < expected + 50);
            buff = new StringBuilder();
            while (it.hasNext()) {
                Node n = it.nextNode();
                buff.append(n.getPath()).append('\n');
            }
            fastSizeResult.complete(buff.toString());
            q = qm.createQuery(xpath, "xpath");
            q.setLimit(90);
            it = q.execute().getNodes();
            assertEquals(90, it.getSize());
        } finally {
            if (session != null) {
                session.logout();
            }
        }

        reconfigure(NoneAllowed.class.getAnnotation(SessionQuerySettingsProviderService.Configuration.class));
        try {
            session = this.createAdminSession();
            qm = session.getWorkspace().getQueryManager();
            q = qm.createQuery(xpath, "xpath");
            it = q.execute().getNodes();
            result = it.getSize();
            assertEquals(-1, result);
            buff = new StringBuilder();
            while (it.hasNext()) {
                Node n = it.nextNode();
                buff.append(n.getPath()).append('\n');
            }
            String regularResult = buff.toString();
            assertEquals(regularResult, fastSizeResult.get());
        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }

    // ----< repository initialization methods >--------

    @Before
    public void setUp() throws Exception {
        getAdminSession();
    }

    @After
    public void logout() {
        if (adminSession != null) {
            adminSession.logout();
            adminSession = null;
        }
        // release repository field
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        repository = null;
        stub = null;
    }

    protected RepositoryStub getStub() throws Exception {
        if (stub == null) {
            Properties properties = new Properties();
            try (InputStream is = RepositoryStub.class.getClassLoader().getResourceAsStream(RepositoryStub.STUB_IMPL_PROPS)) {
                if (is != null) {
                    properties.load(is);
                }
            }
            stub = new OakResultSizeStub(properties);
        }
        return stub;
    }

    protected Repository getRepository() throws Exception {
        if (repository == null) {
            repository = getStub().getRepository();
        }
        return repository;
    }

    protected Session createAdminSession() throws Exception {
        return getRepository().login(getAdminCredentials());
    }

    protected Credentials getAdminCredentials() {
        return stub.getSuperuserCredentials();
    }

}
