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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PathComparator}.
 */
public class PathComparatorTest {

    @Test
    public void sort() {
        List<String> paths = new ArrayList<String>();
        paths.add("/foo");
        paths.add("/foo/bar");
        paths.add("/bar/qux");
        paths.add("/");
        paths.add("/bar");

        Collections.sort(paths, PathComparator.INSTANCE);

        List<String> expected = Lists.newArrayList(
                "/bar/qux", "/foo/bar", "/bar", "/foo", "/");

        assertEquals(expected, paths);
    }
}
