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

package org.apache.jackrabbit.oak.commons.sort;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringSortTest {
    private Comparator<String> comparator = new PathComparator();
    private StringSort collector;

    @Test
    public void inMemory() throws Exception{
        List<String> paths = createTestPaths(5, false);
        collector = new StringSort(paths.size() + 1,comparator);
        addPathsToCollector(paths);

        assertConstraints(paths);
        assertFalse(collector.usingFile());
        collector.close();
    }

    @Test
    public void overflowToDisk() throws Exception{
        //Create ~50k paths
        List<String> paths = createTestPaths(10, true);
        collector = new StringSort(1000, comparator);
        addPathsToCollector(paths);

        assertTrue(collector.usingFile());
        assertConstraints(paths);

        collector.close();
    }

    @Test
    public void sortWithEntriesHavingLineBreaks() throws Exception{
        List<String> paths = Lists.newArrayList("/a", "/a/b\nc", "/a/b\rd", "/a/b\r\ne", "/a/c");

        collector = new StringSort(0, comparator);
        addPathsToCollector(paths);

        assertTrue(collector.usingFile());
        assertConstraints(paths);

        collector.close();
    }

    /**
     * Test for the case where sorting order should not be affected by escaping
     *
     * "aa", "aa\n1", "aa\r2", "aa\\" -> "aa", "aa\n1", "aa\r2", "aa\\"
     * "aa", "aa\\n1", "aa\\r2", "aa\\\\" -> "aa", "aa\\", "aa\n1", "aa\r2",
     *
     * In above case the sorting order for escaped string is different. So
     * it needs to be ensured that sorting order remain un affected by escaping
     * @throws Exception
     */
    @Test
    public void sortWithEntriesHavingLineBreaks2() throws Exception{
        List<String> paths = Lists.newArrayList("/a", "/a/a\nc", "/a/a\rd", "/a/a\r\ne", "/a/a\\");

        collector = new StringSort(0, comparator);
        addPathsToCollector(paths);

        assertTrue(collector.usingFile());
        assertConstraints(paths);

        collector.close();
    }

    private void assertConstraints(List<String> paths) throws IOException {
        assertEquals(paths.size(), collector.getSize());

        Collections.sort(paths, comparator);
        collector.sort();

        List<String> sortedPaths = ImmutableList.copyOf(collector.getIds());
        assertEquals(paths.size(), sortedPaths.size());
        assertEquals(paths, sortedPaths);
    }

    private void addPathsToCollector(Iterable<String> paths) throws IOException {
        for (String path : paths){
            collector.add(path);
        }
    }

    private static List<String> createTestPaths(int depth, boolean permutation){
        List<String> rootPaths = Arrays.asList("a", "b", "c", "d", "e", "f", "g");
        List<String> paths = new ArrayList<String>();


        if (permutation){
            List<String> newRoots = new ArrayList<String>();
            for (List<String> permuts : Collections2.orderedPermutations(rootPaths)){
                newRoots.add(Joiner.on("").join(permuts));
            }
            rootPaths = newRoots;
        }

        for (String root : rootPaths){
            List<String> pathElements = new ArrayList<String>();
            pathElements.add(root);
            paths.add(createId(pathElements));
            for (int i = 0; i < depth; i++){
                pathElements.add(root + i);
                paths.add(createId(pathElements));
            }
        }

        Set<String> idSet = new HashSet<String>(paths);
        assertEquals(paths.size(), idSet.size());

        Collections.shuffle(paths);
        return paths;
    }

    private static String createId(Iterable<String> pathElements){
        return "/" + Joiner.on('/').join(pathElements);
    }

    private static  class PathComparator implements Comparator<String>, Serializable {
        @Override
        public int compare(String o1, String o2) {
            int d1 = pathDepth(o1);
            int d2 = pathDepth(o2);
            if (d1 != d2) {
                return Integer.signum(d2 - d1);
            }
            return o1.compareTo(o2);
        }

        private static int pathDepth(String path) {
            if (path.equals("/")) {
                return 0;
            }
            int depth = 0;
            for (int i = 0; i < path.length(); i++) {
                if (path.charAt(i) == '/') {
                    depth++;
                }
            }
            return depth;
        }
    }
}
