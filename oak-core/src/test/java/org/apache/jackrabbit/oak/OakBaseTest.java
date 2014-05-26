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

import java.util.Arrays;
import java.util.Collection;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class OakBaseTest {

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        Object[][] fixtures = new Object[][] {
                {NodeStoreFixture.MONGO_MK},
                {NodeStoreFixture.MONGO_NS},
                {NodeStoreFixture.SEGMENT_MK},
        };
        return Arrays.asList(fixtures);
    }

    protected NodeStoreFixture fixture;
    protected NodeStore store;

    @Before
    public void setup() {
        store = fixture.createNodeStore();
    }

    @After
    public void teardown() {
        fixture.dispose(store);
    }

    protected OakBaseTest(NodeStoreFixture fixture) {
        this.fixture = fixture;
    }

    protected ContentRepository createContentRepository() {
        return new Oak(store).with(new OpenSecurityProvider()).createContentRepository();
    }

    protected ContentSession createContentSession() {
        return new Oak(store).with(new OpenSecurityProvider()).createContentSession();
    }
}