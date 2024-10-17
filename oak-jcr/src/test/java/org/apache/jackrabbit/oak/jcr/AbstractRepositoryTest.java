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

import static org.junit.Assume.assumeTrue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.jackrabbit.oak.jcr.util.ComponentHolder;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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
    private final boolean reuseNodeStore;

    private volatile NodeStore nodeStore;
    private volatile Repository repository;
    private volatile Session adminSession;
    private volatile Session anonymousSession;

    /**
     * The system property "nsfixtures" can be used to provide a
     * whitespace-separated list of fixtures names for which the
     * tests should be run (the default is to use all fixtures).
     */
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    /**
     * If reuseNodeStore is on, this caches the NodeStore returned by each fixture
     * and they'll be disposed only in @AfterClass instead of @After.
     */
    private static final Map<NodeStoreFixture, NodeStore> NODE_STORES = new ConcurrentHashMap<>();

    /**
     * Default fixtures based on a list in the system property `nsfixtures`.
     * To change the fixtures in a subclass, provide a static method with same @Parameterized
     * annotation but different name that returns a different fixture list.
     */
    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return NodeStoreFixtures.asJunitParameters(FIXTURES);
    }

    /**
     * Constructor enabling that a new NodeStore will be created from
     * the NodeStoreFixture for each test (method).
     *
     * @param fixture the fixture, which gets passed in as junit parameter
     */
    protected AbstractRepositoryTest(NodeStoreFixture fixture) {
        this(fixture, false);
    }

    /**
     * Constructor giving a choice on NodeStore reuse across tests.
     *
     * @param fixture the fixture, which gets passed in as junit parameter
     * @param reuseNodeStore if true, the NodeStore will be reused for all test methods of the
     *                       given test class. if false, a new NodeStore will be created from
     *                       the NodeStoreFixture for each test (method).
     */
    protected AbstractRepositoryTest(NodeStoreFixture fixture, boolean reuseNodeStore) {
        this.fixture = fixture;
        this.reuseNodeStore = reuseNodeStore;
    }

    @Before
    public void ignoreIfFixtureUnavailable() {
        assumeTrue("Skipping unavailable fixture: " + fixture.toString(), fixture.isAvailable());
    }

    @After
    public void logout() {
        // release session fields
        if (adminSession != null) {
            adminSession.logout();
            adminSession = null;
        }
        if (anonymousSession != null) {
            anonymousSession.logout();
            anonymousSession = null;
        }
        // release repository field
        if (repository instanceof JackrabbitRepository) {
            ((JackrabbitRepository) repository).shutdown();
        }
        repository = null;
        if (nodeStore != null && !reuseNodeStore) {
            fixture.dispose(nodeStore);
        }
    }

    @AfterClass
    public static void disposeNodeStores() {
        // only dispose after the entire class
        for (Map.Entry<NodeStoreFixture, NodeStore> e : NODE_STORES.entrySet()) {
            e.getKey().dispose(e.getValue());
        }
        NODE_STORES.clear();
    }

    protected Repository getRepository() throws RepositoryException {
        if (repository == null) {

            if (reuseNodeStore) {
                // see if there is an existing NodeStore from a previous test
                nodeStore = NODE_STORES.get(fixture);
                if (nodeStore == null) {
                    nodeStore = createNodeStore(fixture);
                }
                NODE_STORES.put(fixture, nodeStore);
            } else {
                // always create a new NodeStore (default behavior)
                nodeStore = createNodeStore(fixture);
            }

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

    protected NodeStore getNodeStore() {
        return nodeStore;
    }

    protected <T> T getNodeStoreComponent(Class<T> type) {
        return getNodeStoreComponent(type.getName());
    }

    protected <T> T getNodeStoreComponent(String name) {
        if (fixture instanceof ComponentHolder) {
            return ((ComponentHolder) fixture).get(nodeStore, name);
        }
        return null;
    }

    protected Session getAnonymousSession() throws RepositoryException {
        if (anonymousSession == null) {
            anonymousSession = createAnonymousSession();
        }
        return anonymousSession;
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
