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

import org.junit.Test;

import static org.junit.Assert.*;

public class VectorQueryTest {

    @Test
    public void testBasicQuery() {
        VectorQuery query = new VectorQuery("simple query");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("simple query", query.getQueryText());
    }

    @Test
    public void testQueryWithInferenceConfig() {
        VectorQuery query = new VectorQuery("?{\"model\":\"gpt-4\"}?search for oak trees");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals("search for oak trees", query.getQueryText());
    }

    @Test
    public void testQueryWithComplexInferenceConfig() {
        VectorQuery query = new VectorQuery(
            "?{\"model\":\"gpt-4\",\"temperature\":0.7,\"options\":{\"filter\":true}}?oak trees");
        assertEquals("{\"model\":\"gpt-4\",\"temperature\":0.7,\"options\":{\"filter\":true}}", 
            query.getQueryInferenceConfig());
        assertEquals("oak trees", query.getQueryText());
    }

    @Test
    public void testQueryWithQuestionMarksInText() {
        VectorQuery query = new VectorQuery("?{\"model\":\"gpt-4\"}?what are oak trees?");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals("what are oak trees?", query.getQueryText());
    }

    @Test
    public void testQueryWithoutInferencePrefix() {
        VectorQuery query = new VectorQuery("{\"model\":\"gpt-4\"}?query");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("{\"model\":\"gpt-4\"}?query", query.getQueryText());
    }

    @Test
    public void testQueryWithInvalidJson() {
        VectorQuery query = new VectorQuery("?{invalid json}?query");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("{invalid json}?query", query.getQueryText());
    }

    @Test
    public void testQueryWithEmptyConfig() {
        VectorQuery query = new VectorQuery("??query text");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("??query text", query.getQueryText());
    }

    @Test
    public void testQueryWithWhitespace() {
        VectorQuery query = new VectorQuery("   ?{\"model\":\"gpt-4\"}?   search query   ");
        assertEquals("{\"model\":\"gpt-4\"}", query.getQueryInferenceConfig());
        assertEquals("search query", query.getQueryText());
    }

    @Test
    public void testEmptyQuery() {
        VectorQuery query = new VectorQuery("");
        assertEquals("", query.getQueryInferenceConfig());
        assertEquals("", query.getQueryText());
    }

}