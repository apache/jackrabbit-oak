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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

public class PathElementComparatorTest {

    @Test
    public void sortPathsParentChild() {
        List<String> sorted = TestUtils.sortPaths(asList("/a", "/a b", "/a/bw"));
        assertEquals(asList("/a", "/a/bw", "/a b"), sorted);
    }

    @Test
    public void sort2() {
        assertSorted(asList("/a", "/a/b", "/a/b/c", "/d", "/e/f", "/g"));
        assertSorted(asList("/", "/a", "/a/b", "/a/b/c", "/d", "/e/f", "/g"));
        assertSorted(asList("/", "/a", "/a/b", "/a/b/b", "/a/b/c", "/d", "/e/f", "/g"));
        assertSorted(asList("/", "/a", "/a/b", "/a/b/bc", "/a/b/c", "/d", "/e/f", "/g"));

        //Duplicates
        assertSorted(asList("/", "/a", "/a", "/a/b", "/a/b/c", "/d", "/e/f", "/g"));
    }

    @Test
    public void preferredElements() {
        PathElementComparator c = new PathElementComparator(singleton("jcr:content"));
        assertEquals(asList("/a", "/a/jcr:content", "/a/b"), TestUtils.sortPaths(asList("/a/jcr:content", "/a/b", "/a"), c));

        assertSorted(asList("/a", "/a/jcr:content", "/a/b"),c);
        assertSorted(asList("/a", "/a/jcr:content", "/a/b", "/a/b/c", "/d", "/e/f", "/g"), c);
    }

    private void assertSorted(List<String> sorted) {
        assertSorted(sorted, new PathElementComparator());
    }

    private void assertSorted(List<String> sorted, Comparator<Iterable<String>> comparator) {
        List<String> copy = new ArrayList<>(sorted);
        Collections.shuffle(copy);
        List<String> sortedNew = TestUtils.sortPaths(copy, comparator);
        assertEquals(sorted, sortedNew);
    }

}