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

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

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
@RunWith(Parameterized.class)
@Ignore("This abstract base class does not have any tests")
public abstract class AbstractRepositoryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Object[][] fixtures = new Object[][] {
                {NodeStoreFixture.MK_IMPL},
                {NodeStoreFixture.MONGO_MK},
                {NodeStoreFixture.SEGMENT_MK},
        };
        return Arrays.asList(fixtures);
    }

    protected NodeStoreFixture fixture;
    private NodeStore nodeStore = null;
    private Repository repository = null;
    private Session adminSession = null;

    public AbstractRepositoryTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    @After
    public void logout() throws RepositoryException {
        // release session field
        if (adminSession != null) {
            adminSession.logout();
            adminSession = null;
        }
        // release repository field
        repository = null;
        if (nodeStore != null) {
            fixture.dispose(nodeStore);
        }
    }

    protected Repository getRepository() throws RepositoryException {
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
        // FIXME: provider proper permission setup for the anonymous session (e.g. full read access)
        // return getRepository().login(new GuestCredentials());
        return createAdminSession();
    }

    protected Session createAdminSession() throws RepositoryException {
        return getRepository().login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

}
