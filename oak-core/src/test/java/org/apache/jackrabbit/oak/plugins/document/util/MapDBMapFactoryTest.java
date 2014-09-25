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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.PathComparator;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.junit.Assert.assertEquals;

/**
 * <code>MapDBMapFactoryTest</code>...
 */
public class MapDBMapFactoryTest {

    @Test
    public void comparator() {
        MapFactory factory = new MapDBMapFactory();

        Revision r = new Revision(1, 0, 1);
        Map<String, Revision> map = factory.create(PathComparator.INSTANCE);

        map.put("/", r);
        map.put("/foo", r);
        map.put("/foo/bar", r);
        map.put("/foo/baz", r);
        map.put("/foo/bar/qux", r);
        map.put("/bar/baz", r);
        map.put("/qux", r);

        List<String> expected = Lists.newArrayList(
                "/foo/bar/qux",
                "/bar/baz",
                "/foo/bar",
                "/foo/baz",
                "/foo",
                "/qux",
                "/");
        List<String> actual = Lists.newArrayList(map.keySet());

        assertEquals(expected, actual);

        factory.dispose();
    }
}
