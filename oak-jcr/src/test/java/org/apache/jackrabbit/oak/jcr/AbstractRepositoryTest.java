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

import java.util.Collection;
import java.util.Set;

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
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

    protected final NodeStoreFixture fixture;

    private volatile NodeStore nodeStore;
    private volatile Repository repository;
    private volatile Session adminSession;

    /**
     * The system property "nsfixtures" can be used to provide a
     * whitespace-separated list of fixtures names for which the
     * tests should be run (the default is to use all fixtures).
     */
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    protected AbstractRepositoryTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return NodeStoreFixtures.asJunitParameters(FIXTURES);
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
            repository = createRepository(nodeStore);
        }
        return repository;
    }

    protected NodeStore createNodeStore(NodeStoreFixture fixture) throws RepositoryException {
        return fixture.createNodeStore();
    }

    protected Repository createRepository(NodeStore nodeStore) {
        return initJcr(new Jcr(nodeStore)).createRepository();
    }

    protected Jcr initJcr(Jcr jcr) {
        QueryEngineSettings qs = new QueryEngineSettings();
        qs.setFullTextComparisonWithoutIndex(true);
        jcr.with(BundlingConfigInitializer.INSTANCE);
        return jcr.withAsyncIndexing().with(qs);
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
        return new SimpleCredentials(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID.toCharArray());
    }

    public static <R extends Repository> R dispose(R repository) {
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        return null;
    }

}
