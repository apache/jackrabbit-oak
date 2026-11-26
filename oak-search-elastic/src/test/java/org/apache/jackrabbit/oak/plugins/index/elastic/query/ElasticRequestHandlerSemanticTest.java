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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
import org.apache.jackrabbit.oak.spi.query.fulltext.VectorQueryTestUtil;
import org.junit.Test;

import java.time.LocalDate;

import static org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler.isNegationOnly;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler.plainQueryText;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler.queryConfig;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticRequestHandler.withoutInferencePrefix;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElasticRequestHandlerSemanticTest {

    @Test
    public void ft_oak_12549_toggleShouldBeRemoved() {
        // Time-bombed: if this test fails, the feature toggle FT_OAK-12549 and its guard in
        // ElasticRequestHandler#fullTextQuery (visitTerm) should be cleaned up.
        assertTrue("Feature toggle " + ElasticFeatureToggles.FT_OAK_12549 + " is overdue for removal",
                LocalDate.now().isBefore(LocalDate.of(2027, 11, 20)));
    }

    @Test
    public void stripsInferencePrefix() {
        assertEquals("red fox", withoutInferencePrefix("?{}?red fox"));
        assertEquals("a b", withoutInferencePrefix("?{\"inferenceModelConfig\":\"x\"}?a b"));
        // only the first prefix is removed, as for queries using inference
        assertEquals("?{}?a b", withoutInferencePrefix("?{}??{}?a b"));
        assertEquals("?", withoutInferencePrefix("?{}??"));
    }

    @Test
    public void stripsInferencePrefixOfNegatedTerms() {
        // SQL NOT CONTAINS(*, '?{}?red fox') is passed as "-?{}?red -fox"
        assertEquals("-red -fox", withoutInferencePrefix("-?{}?red -fox"));
        assertEquals("-red OR -fox", withoutInferencePrefix("-?{}?red OR -fox"));
    }

    @Test
    public void keepsTextWithoutCompleteInferencePrefix() {
        for (String text : new String[]{
                "red fox", "-red fox", "-red -fox", "?", "? ", "-?", "-?{}?", "-?foo bar",
                // malformed: VectorQuery falls back to "{}" without consuming a prefix
                "-?{foo -bar",
                // mixed text is not what NOT CONTAINS produces, and is not recognised as prefixed today
                "-?{}?red fox",
                // spaced prefix inside NOT CONTAINS
                "-?{}? -red"}) {
            assertEquals(text, withoutInferencePrefix(text));
        }
    }

    @Test
    public void compatibilityMode() {
        String previous = VectorQueryTestUtil.setCompatibilityMode("true");
        try {
            assertEquals("foo bar", withoutInferencePrefix("?foo bar"));
            assertEquals("-?foo -bar", withoutInferencePrefix("-?foo -bar"));
            assertEquals("-red -fox", withoutInferencePrefix("-?{}?red -fox"));

            VectorQueryTestUtil.setCompatibilityMode("false");
            assertEquals("?foo bar", withoutInferencePrefix("?foo bar"));
        } finally {
            VectorQueryTestUtil.setCompatibilityMode(previous);
        }
    }

    @Test
    public void negationOnly() {
        assertTrue(isNegationOnly("-a -b"));
        assertTrue(isNegationOnly("-a OR -b"));
        assertTrue(isNegationOnly("or"));
        assertTrue(isNegationOnly("-a  -b"));
        assertFalse(isNegationOnly("-a b"));
        assertFalse(isNegationOnly("a"));
        assertFalse(isNegationOnly("-"));
        assertFalse(isNegationOnly("- a"));
        assertFalse(isNegationOnly(" "));
    }

    @Test
    public void plainQueryTextWithoutPrefix() {
        assertEquals("red fox", plainQueryText("?{}?red fox"));
        assertEquals("red fox", plainQueryText("  red fox  "));
        assertEquals("-a -b", plainQueryText("-?{}?a -b"));
        assertEquals("?", plainQueryText("?{}??"));
        assertEquals("{foo", plainQueryText("?{foo"));
        assertEquals("?", plainQueryText("?"));
        assertEquals("?", plainQueryText(" ? "));
        String previous = VectorQueryTestUtil.setCompatibilityMode("false");
        try {
            assertEquals("?machine\\ ", plainQueryText("?machine\\ "));
            VectorQueryTestUtil.setCompatibilityMode("true");
            assertEquals("foo", plainQueryText("?foo"));
        } finally {
            VectorQueryTestUtil.setCompatibilityMode(previous);
        }
    }

    @Test
    public void queryConfigForSemanticAlways() {
        assertEquals("{}", queryConfig("", "a b", false));
        assertEquals("", queryConfig("", "a b", true));
        assertEquals("", queryConfig("", "-a -b", false));
        assertEquals("", queryConfig("", "or", false));
        assertEquals("{\"inferenceModelConfig\":\"m\"}", queryConfig("{\"inferenceModelConfig\":\"m\"}", "a b", false));
        boolean previous = ElasticFeatureToggles.FT_OAK_12549_DISABLE.getAndSet(true);
        try {
            assertEquals("", queryConfig("", "a b", false));
        } finally {
            ElasticFeatureToggles.FT_OAK_12549_DISABLE.set(previous);
        }
    }
}
