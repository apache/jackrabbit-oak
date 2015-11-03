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
package org.apache.jackrabbit.oak;

import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.DOCUMENT_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.MEMORY_NS;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_MK;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class OakBaseTest {

    /**
     * The system property "nsfixtures" can be used to provide a
     * whitespace-separated list of fixtures names for which the
     * tests should be run (the default is to use all fixtures).
     */
    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        Collection<Object[]> result = new ArrayList<Object[]>();
        if (FIXTURES.contains(DOCUMENT_NS)) {
            result.add(new Object[] { NodeStoreFixture.MONGO_NS });
        }
        if (FIXTURES.contains(SEGMENT_MK)) {
            result.add(new Object[] { NodeStoreFixture.SEGMENT_MK });
        }
        if (FIXTURES.contains(MEMORY_NS)) {
            result.add(new Object[] { NodeStoreFixture.MEMORY_NS });
        }
        return result;
    }


    protected final NodeStoreFixture fixture;
    protected final NodeStore store;

    @After
    public void teardown() {
        fixture.dispose(store);
    }

    protected OakBaseTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
        this.store = fixture.createNodeStore();
    }

    protected ContentRepository createContentRepository() {
        return new Oak(store).with(new OpenSecurityProvider()).createContentRepository();
    }

    protected ContentSession createContentSession() {
        return new Oak(store).with(new OpenSecurityProvider()).createContentSession();
    }
}