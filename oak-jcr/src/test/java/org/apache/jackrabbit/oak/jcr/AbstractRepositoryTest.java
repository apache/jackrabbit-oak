/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.jcr.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Abstract base class for repository tests providing methods for accessing
 * the repository, a session and nodes and properties from that session.
 *
 * Users of this class must call clear to close the session associated with
 * this instance and clean up the repository when done.
 */
@RunWith(Parallelized.class)
@Ignore("This abstract base class does not have any tests")
public abstract class AbstractRepositoryTest {

    protected NodeStoreFixture fixture;
    private NodeStore nodeStore;
    private Repository repository;
    private Session adminSession;
    protected int observationQueueLength = Jcr.DEFAULT_OBSERVATION_QUEUE_LENGTH;

    /**
     * The system property "ns-fixtures" can be used to provide a
     * whitespace-separated list of fixtures names for which the
     * tests should be run (the default is to use all fixtures).
     */
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    public AbstractRepositoryTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        if (FIXTURES.contains(Fixture.DOCUMENT_MK)) {
            result.add(new Object[] { NodeStoreFixture.DOCUMENT_MK });
        }
        if (FIXTURES.contains(Fixture.DOCUMENT_NS)) {
            result.add(new Object[] { NodeStoreFixture.DOCUMENT_NS });
        }
        if (FIXTURES.contains(Fixture.SEGMENT_MK)) {
            result.add(new Object[] { NodeStoreFixture.SEGMENT_MK });
        }
        if (FIXTURES.contains(Fixture.DOCUMENT_JDBC)) {
            result.add(new Object[] { NodeStoreFixture.DOCUMENT_JDBC });
        }
        return result;
    }

    @After
    public void logout() {
        // release session field
        if (adminSession != null) {
            adminSession.logout();
            adminSession = null;
        }
        // release repository field
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        repository = null;
        if (nodeStore != null) {
            fixture.dispose(nodeStore);
        }
    }

    protected Repository getRepository() throws RepositoryException {
        if (repository == null) {
            nodeStore = createNodeStore(fixture);
            repository  = new Jcr(nodeStore)
                    .withObservationQueueLength(observationQueueLength)
                    .createRepository();
        }
        return repository;
    }

    protected NodeStore createNodeStore(NodeStoreFixture fixture) throws RepositoryException {
        return fixture.createNodeStore();
    }

    protected Session getAdminSession() throws RepositoryException {
        if (adminSession == null) {
            adminSession = createAdminSession();
        }
        return adminSession;
    }

    protected Session createAnonymousSession() throws RepositoryException {
        Session admin = getAdminSession();
        AccessControlUtils.addAccessControlEntry(admin, "/", EveryonePrincipal.getInstance(), new String[] {Privilege.JCR_READ}, true);
        admin.save();
        return getRepository().login(new GuestCredentials());
    }

    protected Session createAdminSession() throws RepositoryException {
        return getRepository().login(getAdminCredentials());
    }

    protected SimpleCredentials getAdminCredentials() {
        return new SimpleCredentials("admin", "admin".toCharArray());
    }

    public static <R extends Repository> R dispose(R repository) {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        return null;
    }

}
