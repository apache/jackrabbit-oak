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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A base class for two node cluster tests with a virtual clock.
 */
@Ignore("This base test does not have tests")
@RunWith(Parameterized.class)
public class AbstractTwoNodeTest {

    protected final DocumentStoreFixture fixture;

    protected DocumentStore store1;
    protected DocumentStore store2;
    protected DocumentNodeStore ds1;
    protected DocumentNodeStore ds2;
    protected int c1Id;
    protected int c2Id;
    protected Clock clock;

    public AbstractTwoNodeTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    /**
     * Can be overwritten by tests to customize / wrap the store.
     *
     * @param store the store to customize.
     * @return the customized store.
     */
    protected DocumentStore customize(DocumentStore store) {
        return store;
    }

    //----------------------------------------< Set Up >

    @Parameterized.Parameters(name = "{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = Lists.newArrayList();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture("RDB-H2(file)", "jdbc:h2:file:./target/ds-test", "sa", "");
        if (rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }
        return fixtures;
    }

    @Before
    public void setUp() throws InterruptedException {
        MongoUtils.dropCollections(MongoUtils.DB);

        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());

        ClusterNodeInfo.setClock(clock);
        Revision.setClock(clock);
        store1 = fixture.createDocumentStore(1);
        if (!fixture.hasSinglePersistence()) {
            store2 = store1;
        } else {
            store2 = fixture.createDocumentStore(2);
        }

        ds1 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .clock(clock)
                .setDocumentStore(wrap(customize(store1)))
                .setLeaseCheck(false)
                .setClusterId(1)
                .getNodeStore();
        c1Id = ds1.getClusterId();

        ds2 = new DocumentMK.Builder()
                .setAsyncDelay(0)
                .clock(clock)
                .setDocumentStore(wrap(customize(store2)))
                .setLeaseCheck(false)
                .setClusterId(2)
                .getNodeStore();
        c2Id = ds2.getClusterId();
    }

    @After
    public void tearDown() throws Exception {
        ds1.dispose();
        ds2.dispose();
        store1.dispose();
        store2.dispose();
        fixture.dispose();
        ClusterNodeInfo.resetClockToDefault();
        Revision.resetClockToDefault();
    }

    private static DocumentStore wrap(DocumentStore ds) {
        return new DocumentStoreTestWrapper(ds);
    }

    private static class DocumentStoreTestWrapper extends DocumentStoreWrapper {

        DocumentStoreTestWrapper(DocumentStore store) {
            super(store);
        }

        @Override
        public void dispose() {
            // ignore
        }
    }

}
