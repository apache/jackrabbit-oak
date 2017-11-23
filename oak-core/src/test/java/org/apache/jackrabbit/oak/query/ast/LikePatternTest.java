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
package org.apache.jackrabbit.oak.query.ast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.jackrabbit.oak.spi.query.fulltext.LikePattern;
import org.junit.Test;

/**
 * Tests "like" pattern matching.
 */
public class LikePatternTest {

    @Test
    public void pattern() {
        pattern("%_", "X", "", null, null);
        pattern("A%", "A", "X", "A", "B");
        pattern("A%%", "A", "X", "A", "B");
        pattern("%\\_%", "A_A", "AAA", null, null);
    }

    private static void pattern(String pattern, String match, String noMatch, String lower, String upper) {
        LikePattern p = new LikePattern(pattern);
        if (match != null) {
            assertTrue(p.matches(match));
        }
        if (noMatch != null) {
            assertFalse(p.matches(noMatch));
        }
        assertEquals(lower, p.getLowerBound());
        assertEquals(upper, p.getUpperBound());
    }

}
