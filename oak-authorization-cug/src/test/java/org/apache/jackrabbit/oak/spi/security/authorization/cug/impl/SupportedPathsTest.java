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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SupportedPathsTest {

    @Test
    public void testIncludes() {
        SupportedPaths supportedPaths = new SupportedPaths(ImmutableSet.of("/content"));

        Map<String, Boolean> pathMap = new HashMap<String, Boolean>();
        pathMap.put("/content", true);
        pathMap.put("/content/a", true);
        pathMap.put("/content/a/rep:cugPolicy", true);
        pathMap.put("/content/a/b", true);
        pathMap.put("/content/a/b/c/jcr:primaryType", true);
        pathMap.put("/content/aa", true);
        pathMap.put("/content/aa/bb/cc", true);
        pathMap.put("/jcr:system", false);
        pathMap.put("/", false);
        pathMap.put("/testRoot", false);
        pathMap.put("/some/other/path", false);

        for (String path : pathMap.keySet()) {
            boolean expected = pathMap.get(path);

            assertEquals(path, expected, supportedPaths.includes(path));
            assertEquals(path, expected, supportedPaths.includes(path + '/'));
        }
    }

    @Test
    public void testMayContainCug() {
        SupportedPaths supportedPaths = new SupportedPaths(ImmutableSet.of("/content/a"));

        Map<String, Boolean> pathMap = new HashMap<String, Boolean>();
        pathMap.put("/", true);
        pathMap.put("/content", true);
        pathMap.put("/jcr:system", false);
        pathMap.put("/testRoot", false);
        pathMap.put("/some/other/path", false);
        pathMap.put("/content/a", false);
        pathMap.put("/content/a/b", false);

        for (String path : pathMap.keySet()) {
            boolean expected = pathMap.get(path);
            assertEquals(path, expected, supportedPaths.mayContainCug(path));
        }
    }


    @Test
    public void testRootPath() {
        SupportedPaths supportedPaths = new SupportedPaths(ImmutableSet.of("/"));

        List<String> paths = ImmutableList.of("/", "/content", "/jcr:system", "/testRoot", "/some/other/path", "/content/a", "/content/a/b");

        for (String path : paths) {
            assertTrue(path, supportedPaths.includes(path));
            assertTrue(path, supportedPaths.mayContainCug(path));
        }
    }

    @Test
    public void testEmpty() {
        SupportedPaths supportedPaths = new SupportedPaths(ImmutableSet.<String>of());

        List<String> paths = ImmutableList.of("/", "/content", "/jcr:system", "/testRoot", "/some/other/path", "/content/a", "/content/a/b");

        for (String path : paths) {
            assertFalse(path, supportedPaths.includes(path));
            assertFalse(path, supportedPaths.mayContainCug(path));
        }
    }
}