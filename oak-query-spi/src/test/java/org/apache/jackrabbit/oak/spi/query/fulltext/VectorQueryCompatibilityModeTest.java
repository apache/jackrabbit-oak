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
package org.apache.jackrabbit.oak.spi.query.fulltext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VectorQueryCompatibilityModeTest {

    @Before
    public void setUp() {
        // Set up any necessary system properties or configurations
        System.setProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY, "true");
    }

    @After
    public void tearDown() {
        // Clean up any system properties set during the tests
        System.clearProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY);
        System.clearProperty(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX_KEY);
    }

    @Test
    public void testQueryWithEmptyConfigExperimentalInferenceCompatible() {
        // Input string: "??query text"
        String inputString = VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text";
        VectorQuery query = new VectorQuery(inputString);

        assertEquals("{}", query.getQueryInferenceConfig());
        assertEquals(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text", query.getQueryText());
    }

    @Test
    public void testPrefixOnlyQueryExperimentalInferenceCompatible() {
        // Input string: "?query text"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text");
        assertEquals("{}", query.getQueryInferenceConfig());
        // With the implementation fix, the prefix should now be correctly stripped
        assertEquals("query text", query.getQueryText());
    }

    // We don't need to explicitly enable experimental compatibility mode in each test anymore
    // as it's already set in setUp()
}