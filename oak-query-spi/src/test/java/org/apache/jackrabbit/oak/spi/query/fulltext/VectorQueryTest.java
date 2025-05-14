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

public class VectorQueryTest {

    @Before
    public void setUp() {
        // Ensure compatibility mode is disabled for these tests
        System.setProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY, "false");
    }

    @After
    public void tearDown() {
        // Clean up all system properties set during the tests
        System.clearProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY);
    }

    @Test
    public void testBasicQuery() {
        // Input string: "simple query"
        VectorQuery query = new VectorQuery("simple query");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("simple query", query.getQueryText());
    }

    @Test
    public void testQueryWithInferenceConfig() {
        // Input string: "?{"model":"gpt-4"}?search for oak trees"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{\"model\":\"gpt-4\"}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "search for oak trees");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals("search for oak trees", query.getQueryText());
    }

    @Test
    public void testQueryWithComplexInferenceConfig() {
        // Input string: "?{"model":"gpt-4","temperature":0.7,"options":{"filter":true}}?oak trees"
        VectorQuery query = new VectorQuery(
            VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{\"model\":\"gpt-4\",\"temperature\":0.7,\"options\":{\"filter\":true}}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "oak trees");
        assertEquals("{\"model\":\"gpt-4\",\"temperature\":0.7,\"options\":{\"filter\":true}}",
            query.getQueryInferenceConfig());
        assertEquals("oak trees", query.getQueryText());
    }

    @Test
    public void testQueryWithQuestionMarksInText() {
        // Input string: "?{"model":"gpt-4"}?what are oak trees?"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{\"model\":\"gpt-4\"}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "what are oak trees?");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals("what are oak trees?", query.getQueryText());
    }

    @Test
    public void testQueryWithoutInferencePrefix() {
        // Input string: "{"model":"gpt-4"}?query"
        VectorQuery query = new VectorQuery("{\"model\":\"gpt-4\"}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("{\"model\":\"gpt-4\"}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query", query.getQueryText());
    }

    @Test
    public void testQueryWithInvalidJson() {
        // Input string: "?{invalid json}?query"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{invalid json}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query");
        assertEquals("{}", query.getQueryInferenceConfig());
        assertEquals("{invalid json}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query", query.getQueryText());
    }

    @Test
    public void testQueryWithWhitespace() {
        String whiteSpaces = "    ";
        // Input string: "   ?{"model":"gpt-4"}?   search query   "
        VectorQuery query = new VectorQuery("   " + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{\"model\":\"gpt-4\"}" + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + whiteSpaces + "search query   ");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals(whiteSpaces + "search query", query.getQueryText());
    }

    @Test
    public void testEmptyQuery() {
        // Input string: ""
        VectorQuery query = new VectorQuery("");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("", query.getQueryText());
    }

    @Test
    public void testNoJsonEndDelimiterQuery() {
        // Input string: "?{"model":"gpt-4"query text"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "{\"model\":\"gpt-4\"query text");
        assertEquals("{}", query.getQueryInferenceConfig());
        // With the implementation fix, the prefix should now be correctly stripped
        assertEquals("{\"model\":\"gpt-4\"query text", query.getQueryText());
    }

    @Test
    public void testQueryWithEmptyConfigExperimentalInferenceNonCompatible() {
        // Input string: "??query text"
        String inputString = VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text";
        VectorQuery query = new VectorQuery(inputString);

        assertEquals("", query.getQueryInferenceConfig());
        assertEquals(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text", query.getQueryText());
    }

    @Test
    public void testPrefixOnlyQueryExperimentalInferenceNonCompatible() {
        // Input string: "?query text"
        VectorQuery query = new VectorQuery(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text");
        assertEquals("", query.getQueryInferenceConfig());
        // When compatibility mode is disabled, the prefix should remain part of the query text
        assertEquals(VectorQuery.INFERENCE_QUERY_CONFIG_PREFIX + "query text", query.getQueryText());
    }

}