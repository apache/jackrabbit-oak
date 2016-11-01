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
package org.apache.jackrabbit.oak.jcr.observation;

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class OakEventFilterImplTest {

    @Test
    public void testGlobAsRegex() throws Exception {
        assertMatches("foo", "foo");
        assertMatches("**", "foo");
        assertMatches("**/bar", "foo/bar");
        assertMatches("**/bar", "bar");
        assertMatches("**/bar/**", "bar");
    }

    private void assertMatches(String regexp, String str) {
        String p = OakEventFilterImpl.globAsRegex(regexp);
        Pattern pattern = Pattern.compile(p);
        assertTrue("'"+regexp+"' is expected to match '"+str+"'", pattern.matcher(str).matches());
    }
    
}
