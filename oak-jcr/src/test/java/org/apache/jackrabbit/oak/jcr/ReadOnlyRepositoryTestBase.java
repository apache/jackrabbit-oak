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

import java.util.Map;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Maps;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.AfterClass;

/**
 * A base class for tests that never read from the repository and therefore
 * share a node store that is initialized just once per test class.
 */
public abstract class ReadOnlyRepositoryTestBase extends AbstractRepositoryTest {

    /** Cache of initialized node stores per fixture when test is read-only */
    private static final Map<NodeStoreFixture, NodeStore> STORES = Maps.newConcurrentMap();

    private Repository repository;

    public ReadOnlyRepositoryTestBase(NodeStoreFixture fixture) {
        super(fixture);
    }

    @AfterClass
    public static void disposeStores() {
        for (Map.Entry<NodeStoreFixture, NodeStore> e : STORES.entrySet()) {
            e.getKey().dispose(e.getValue());
        }
        STORES.clear();
    }

    @Override
    protected Repository getRepository() throws RepositoryException {
        if (repository == null) {
            repository = createRepositoryFromCachedStore(fixture);
            Session s = repository.login(getAdminCredentials());
            try {
                initializeRepository(s);
            } finally {
                s.logout();
            }
        }
        return repository;
    }

    protected void initializeRepository(Session session) {
        // default does nothing
    }

    private Repository createRepositoryFromCachedStore(NodeStoreFixture fixture)
            throws RepositoryException {
        NodeStore ns = null;
        for (Map.Entry<NodeStoreFixture, NodeStore> e : STORES.entrySet()) {
            if (e.getKey().getClass().equals(fixture.getClass())) {
                ns = e.getValue();
            }
        }
        if (ns == null) {
            ns = createNodeStore(fixture);
            STORES.put(fixture, ns);
        }
        return createRepository(ns);
    }
}
