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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public static Collection<Object[]> loadFixtures() {
        Collection<Object[]> fixtures = new ArrayList<Object[]>();

        Class<MicroKernelFixture> iface = MicroKernelFixture.class;
        ServiceLoader<MicroKernelFixture> loader =
                ServiceLoader.load(iface, iface.getClassLoader());
        for (MicroKernelFixture fixture : loader) {
            fixtures.add(new Object[] { fixture });
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
     * A JSON parser instance that can be used for parsing JSON-format data;
     * {@code JSONParser} instances are not <i>not</i> thread-safe.
     * @see #getJSONParser()
     */
    private JSONParser parser;

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
        addInitialTestContent();
    }

    /**
     * Adds initial content used by the test case. This method is
     * called by the {@link #setUp()} method after the {@link MicroKernel}
     * cluster has been set up and before the actual test is run.
     * The default implementation does nothing, but subclasses can
     * override this method to perform extra initialization.
     */
    protected void addInitialTestContent() {
    }

    /**
     * Releases the {@link MicroKernel} cluster used by this test case.
     */
    @After
    public void tearDown() {
        fixture.tearDownCluster(mks);
    }

    /**
     * Returns a {@code JSONParser} instance for parsing JSON format data.
     * This method returns a cached instance.
     * <p/>
     * {@code JSONParser} instances are <i>not</i> thread-safe. Multi-threaded
     * unit tests should therefore override this method and return a fresh
     * instance on every invocation.
     *
     * @return a {@code JSONParser} instance
     */
    protected synchronized JSONParser getJSONParser() {
        if (parser == null) {
            parser = new JSONParser();
        }
        return parser;
    }

    //--------------------------------< utility methods for parsing json data >
    /**
     * Parses the provided string into a {@code JSONObject}.
     *
     * @param json string to be parsed
     * @return a {@code JSONObject}
     * @throws {@code AssertionError} if the string cannot be parsed into a {@code JSONObject}
     */
    protected JSONObject parseJSONObject(String json) throws AssertionError {
        JSONParser parser = getJSONParser();
        try {
            Object obj = parser.parse(json);
            assertTrue(obj instanceof JSONObject);
            return (JSONObject) obj;
        } catch (Exception e) {
            throw new AssertionError("not a valid JSON object: " + e.getMessage());
        }
    }

    /**
     * Parses the provided string into a {@code JSONObject}.
     *
     * @param json string to be parsed
     * @return a {@code JSONArray}
     * @throws {@code AssertionError} if the string cannot be parsed into a {@code JSONArray}
     */
    protected JSONArray parseJSONArray(String json) throws AssertionError {
        JSONParser parser = getJSONParser();
        try {
            Object obj = parser.parse(json);
            assertTrue(obj instanceof JSONArray);
            return (JSONArray) obj;
        } catch (Exception e) {
            throw new AssertionError("not a valid JSON array: " + e.getMessage());
        }
    }
}
