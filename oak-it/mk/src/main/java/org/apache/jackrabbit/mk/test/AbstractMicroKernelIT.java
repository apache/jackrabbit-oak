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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        checkArgument(nodeCount > 0);
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

        // Clear fields to avoid consuming memory after the test has run.
        // It looks like JUnit keeps references to all test instances until
        // the entire test suite has been run.
        Arrays.fill(mks, null);
        mk = null;
        parser = null;
    }

    //--------------------------------< utility methods for parsing json data >
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
     * Parses the provided string into a {@code JSONArray}.
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

    protected Set<String> getNodeNames(JSONObject obj) {
        Set<String> names = new HashSet<String>();
        Set<Map.Entry> entries = obj.entrySet();
        for (Map.Entry entry : entries) {
            if (entry.getValue() instanceof JSONObject) {
                names.add((String) entry.getKey());
            }
        }
        return names;
    }

    protected Set<String> getPropertyNames(JSONObject obj) {
        Set<String> names = new HashSet<String>();
        Set<Map.Entry> entries = obj.entrySet();
        for (Map.Entry entry : entries) {
            if (! (entry.getValue() instanceof JSONObject)) {
                names.add((String) entry.getKey());
            }
        }
        return names;
    }

    protected void assertPropertyExists(JSONObject obj, String relPath)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);
    }

    protected void assertPropertyNotExists(JSONObject obj, String relPath)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNull(val);
    }

    protected void assertPropertyExists(JSONObject obj, String relPath, Class type)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertTrue(type.isInstance(val));
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Double expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Long expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Boolean expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, String expected)
            throws AssertionError {
        Object val = resolveValue(obj, relPath);
        assertNotNull("not found: " + relPath, val);

        assertEquals(expected, val);
    }

    protected void assertPropertyValue(JSONObject obj, String relPath, Object[] expected)
            throws AssertionError {
        JSONArray array = resolveArrayValue(obj, relPath);
        assertNotNull("not found: " + relPath, array);

        assertEquals(expected.length, array.size());

        // JSON numeric types: Double, Long
        // convert types as necessary for comparison using equals method
        for (int i = 0; i < array.size(); i++) {
            Object o1 = expected[i];
            Object o2 = array.get(i);
            if (o1 instanceof Number && o2 instanceof Number) {
                if (o1 instanceof Integer) {
                    o1 = new Long((Integer) o1);
                } else if (o1 instanceof Short) {
                    o1 = new Long((Short) o1);
                } else if (o1 instanceof Float) {
                    o1 = new Double((Float) o1);
                }
            }
            assertEquals(o1, o2);
        }
    }

    protected JSONObject resolveObjectValue(JSONObject obj, String relPath) {
        Object val = resolveValue(obj, relPath);
        if (val instanceof JSONObject) {
            return (JSONObject) val;
        }
        throw new AssertionError("failed to resolve JSONObject value at " + relPath + ": " + val);
    }

    protected JSONObject getObjectArrayEntry(JSONArray array, int pos) {
        assertTrue(pos >= 0 && pos < array.size());
        Object entry = array.get(pos);
        if (entry instanceof JSONObject) {
            return (JSONObject) entry;
        }
        throw new AssertionError("failed to resolve JSONObject array entry at pos " + pos + ": " + entry);
    }

    protected JSONArray resolveArrayValue(JSONObject obj, String relPath) {
        Object val = resolveValue(obj, relPath);
        if (val instanceof JSONArray) {
            return (JSONArray) val;
        }
        throw new AssertionError("failed to resolve JSONArray value at " + relPath + ": " + val);
    }

    protected Object resolveValue(JSONObject obj, String relPath) {
        String names[] = relPath.split("/");
        Object val = obj;
        for (String name : names) {
            if (! (val instanceof JSONObject)) {
                throw new AssertionError("not found: " + relPath);
            }
            val = ((JSONObject) val).get(name);
        }
        return val;
    }
}
