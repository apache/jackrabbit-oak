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

import java.util.Arrays;
import java.util.Collection;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
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

    public AbstractRepositoryTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Object[][] fixtures = new Object[][] {
                {NodeStoreFixture.MK_IMPL},
                {NodeStoreFixture.MONGO_MK},
                {NodeStoreFixture.MONGO_NS},
                {NodeStoreFixture.SEGMENT_MK},
                {NodeStoreFixture.MONGO_JDBC},
        };
        return Arrays.asList(fixtures);
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

    protected Repository getRepository() {
        if (repository == null) {
            nodeStore = fixture.createNodeStore();
            repository  = new Jcr(nodeStore).createRepository();
        }
        return repository;
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

}
