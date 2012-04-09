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
package org.apache.jackrabbit.mk.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ServiceLoader;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

/**
 * Abstract base class for {@link MicroKernel} integration tests.
 */
public abstract class AbstractMicroKernelIT {

    /**
     * Finds and returns all {@link MicroKernelFixture} services available
     * in the current classpath.
     *
     * @return available {@link MicroKernelFixture} services
     */
    @Parameters
    public static Collection<MicroKernelFixture> loadFixtures() {
        Collection<MicroKernelFixture> fixtures =
                new ArrayList<MicroKernelFixture>();

        Class<MicroKernelFixture> iface = MicroKernelFixture.class;
        ServiceLoader<MicroKernelFixture> loader =
                ServiceLoader.load(iface, iface.getClassLoader());
        for (MicroKernelFixture fixture : loader) {
            fixtures.add(fixture);
        }

        return fixtures;
    }

    /**
     * The {@link MicroKernelFixture} service used by this test case instance.
     */
    protected final MicroKernelFixture fixture;

    /**
     * The {@link MicroKernel} cluster node instances used by this test case.
     */
    protected final MicroKernel[] mks;

    /**
     * The {@link MicroKernel} instance used by this test case.
     * In a clustered setup this is the first node of the cluster.
     */
    protected MicroKernel mk;

    /**
     * Creates a {@link MicroKernel} test case for a cluster of the given
     * size created using the given {@link MicroKernelFixture} service.
     *
     * @param fixture {@link MicroKernelFixture} service
     * @param nodeCount number of nodes that the test cluster should contain
     */
    protected AbstractMicroKernelIT(MicroKernelFixture fixture, int nodeCount) {
        assert nodeCount > 0;
        this.fixture = fixture;
        this.mks = new MicroKernel[nodeCount];
    }

    /**
     * Prepares the test case by initializing the {@link #mks} and
     * {@link #mk} variables with a new {@link MicroKernel} cluster
     * from the {@link MicroKernelFixture} service associated with
     * this test case.
     */
    @Before
    public void setUp() {
        fixture.setUpCluster(mks);
        mk = mks[0];
    }

    /**
     * Releases the {@link MicroKernel} cluster used by this test case.
     */
    @After
    public void tearDown() {
        fixture.tearDownCluster(mks);
    }

}
