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
package org.apache.jackrabbit.oak.upgrade;

import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IncludeExcludeSidegradeTest {

    public static final Credentials CREDENTIALS = new SimpleCredentials("admin", "admin".toCharArray());

    private NodeStore sourceNodeStore;

    private NodeStore targetNodeStore;

    private RepositoryImpl targetRepository;

    private Session targetSession;

    @Before
    public void prepareNodeStores() throws RepositoryException {
        sourceNodeStore = new MemoryNodeStore();
        withSession(sourceNodeStore, s -> {
            createCommonContent(s);
            createSourceContent(s);
        });

        targetNodeStore = new MemoryNodeStore();
        withSession(targetNodeStore, s -> {
            createCommonContent(s);
        });

        performSidegrade();

        targetRepository = getRepository(targetNodeStore);
        targetSession = targetRepository.login(CREDENTIALS);
    }

    public void cleanup() {
        targetRepository.shutdown();
    }

    @Test
    public void shouldHaveIncludedPaths() throws RepositoryException {
        assertExists(
                "/content/foo/en",
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014"
        );
    }

    @Test
    public void shouldLackPathsThatWereNotIncluded() throws RepositoryException {
        assertMissing(
                "/content/foo/de",
                "/content/foo/fr",
                "/content/foo/it"
        );
    }

    @Test
    public void shouldLackExcludedPaths() throws RepositoryException {
        assertMissing(
                "/content/assets/foo/2013",
                "/content/assets/foo/2012",
                "/content/assets/foo/2011",
                "/content/assets/foo/2010"
        );
    }

    @Test
    public void testPermissions() throws RepositoryException {
        Session aliceSession = targetRepository.login(new SimpleCredentials("alice", "bar".toCharArray()));
        Session bobSession = targetRepository.login(new SimpleCredentials("bob", "bar".toCharArray()));

        assertExists(aliceSession,
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014"
        );

        assertMissing(aliceSession,
                "/content/foo/en"
        );

        assertExists(bobSession,
                "/content/foo/en"
        );

        assertMissing(bobSession,
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014"
        );
    }

    private void createCommonContent(JackrabbitSession session) throws RepositoryException {
        UserManager um = session.getUserManager();
        um.createUser("alice", "bar");
        um.createUser("bob", "bar");
        session.save();
    }

    private void createSourceContent(JackrabbitSession session) throws RepositoryException {
        for (String p : Arrays.asList(
                "/content/foo/de",
                "/content/foo/en",
                "/content/foo/fr",
                "/content/foo/it",
                "/content/assets/foo",
                "/content/assets/foo/2015",
                "/content/assets/foo/2015/02",
                "/content/assets/foo/2015/01",
                "/content/assets/foo/2014",
                "/content/assets/foo/2013",
                "/content/assets/foo/2012",
                "/content/assets/foo/2011",
                "/content/assets/foo/2010/12")) {
            JcrUtils.getOrCreateByPath(p, JcrConstants.NT_FOLDER, JcrConstants.NT_FOLDER, session, false);
        }

        AccessControlUtils.denyAllToEveryone(session, "/content/foo/en");
        AccessControlUtils.allow(session.getNode("/content/foo/en"), "bob", "jcr:read");

        AccessControlUtils.denyAllToEveryone(session,"/content/assets/foo");
        AccessControlUtils.allow(session.getNode("/content/assets/foo"), "alice", "jcr:read");
    }

    private void performSidegrade() throws RepositoryException {
        RepositorySidegrade sidegrade = new RepositorySidegrade(sourceNodeStore, targetNodeStore);
        sidegrade.setIncludes(
                "/content/foo/en",
                "/content/assets/foo",
                "/content/other"
        );
        sidegrade.setExcludes(
                "/content/assets/foo/2013",
                "/content/assets/foo/2012",
                "/content/assets/foo/2011",
                "/content/assets/foo/2010"
        );
        sidegrade.copy();
    }

    private static RepositoryImpl getRepository(NodeStore nodeStore) {
        return (RepositoryImpl) new Jcr(new Oak(nodeStore).with(SecurityProviderBuilder.newBuilder().build())).createRepository();
    }

    private static void withSession(NodeStore nodeStore, SessionConsumer sessionConsumer) throws RepositoryException {
        RepositoryImpl repository = getRepository(nodeStore);
        Session session = repository.login(CREDENTIALS);
        try {
            sessionConsumer.accept((JackrabbitSession) session);
            session.save();
        } finally {
            session.logout();
            repository.shutdown();
        }
    }

    private void assertExists(String... paths) throws RepositoryException {
        assertExists(targetSession, paths);

    }

    private void assertExists(Session session, String... paths) throws RepositoryException {
        for (String path : paths) {
            assertTrue("node " + path + " should exist", session.nodeExists(path));
        }
    }

    private void assertMissing(String... paths) throws RepositoryException {
        assertMissing(targetSession, paths);
    }

    private void assertMissing(Session session, String... paths) throws RepositoryException {
        for (String path : paths) {
            assertFalse("node " + path + " should not exist", session.nodeExists(path));
        }
    }

    private interface SessionConsumer {
        void accept(JackrabbitSession session) throws RepositoryException;
    }

}
