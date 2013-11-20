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
package org.apache.jackrabbit.oak.spi.commit;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * MoveTrackerTest... TODO
 */
public class MoveTrackerTest {

    @Test
    public void testIsEmpty() {
        assertTrue(new MoveTracker().isEmpty());
    }

    @Test
    public void testNotEmpty() {
        for (Tree.Status st : Tree.Status.values()) {
            MoveTracker mt = new MoveTracker();
            mt.addMove("/a/b/c", "/d/e/f", st);
            assertFalse(mt.isEmpty());
        }
    }

    @Test
    public void testSingleMove() {
        Map<String, String> moves = ImmutableMap.of(
                "/a/b/c", "/d/e/f",
                "/aa/bb/cc", "/aa/f/cc"
        );

        for (String src : moves.keySet()) {
            MoveTracker mt = new MoveTracker();
            String dest = moves.get(src);
            mt.addMove(src, dest);

            assertEquals(src, mt.getOriginalSourcePath(dest));
            assertEquals(dest, mt.getDestPath(src));
        }
    }

    @Test
    public void testIndependantMoves() {
        Map<String, String> m = ImmutableMap.of(
                "/a/b/c", "/d/e/f",
                "/aa/bb/cc", "/aa/f/cc"
        );

        MoveTracker mt = new MoveTracker();
        for (String src : m.keySet()) {
            String dest = m.get(src);
            mt.addMove(src, dest);
        }

        for (String src : m.keySet()) {
            String dest = m.get(src);
            assertEquals(src, mt.getOriginalSourcePath(dest));
            assertEquals(dest, mt.getDestPath(src));
        }
    }

    /**
     * Existing tree structure:
     *
     * - /a/b/c/d/e
     * - /a/f
     *
     * Moves:
     * - /a/b/c    -> /a/f/c
     * - /a/f/c/d  -> /a/b/d
     * - /a/b/d/e  -> /a/f/c/e
     * - /a/f/c    -> /a/b/c
     *
     * Expected mapping original-source vs destination:
     * - "/a/b/c"     -> "/a/f/c"
     * - "/a/b/c/d"   -> "/a/b/d"
     * - "/a/b/c/d/e" -> "/a/f/c/e"
     */
    @Test
    public void testMultiMove() {
        MoveTest test = new MoveTest()
                .addMove("/a/b/c", "/a/f/c", "/a/b/c")
                .addMove("/a/f/c/d", "/a/b/d", "/a/b/c/d")
                .addMove("/a/b/d/e", "/a/f/c/e", "/a/b/c/d/e");
        test.assertResult();
    }

    /**
     * Existing tree structure:
     *
     * - /a/b/c/d/e
     * - /a/f
     *
     * Moves:
     * - /a/b/c    -> /a/f/c
     * - /a/f/c/d  -> /a/b/d
     * - /a/b/d/e  -> /a/f/c/e
     * - /a/f/c/e  -> /a/f/e
     *
     * Expected mapping original-source vs destination:
     * - "/a/b/c"     -> "/a/f/c"
     * - "/a/b/c/d"   -> "/a/b/d"
     * - "/a/b/c/d/e" -> "/a/f/e"
     */
    @Test
    public void testMultiMove2() {
        MoveTest test = new MoveTest()
                .addMove("/a/b/c", "/a/f/c", "/a/b/c")
                .addMove("/a/f/c/d", "/a/b/d", "/a/b/c/d")
                .addMove("/a/b/d/e", "/a/f/c/e", "/a/b/c/d/e")
                .addMove("/a/f/c/e", "/a/f/e", "/a/b/c/d/e");
        test.assertResult();
    }

    @Test
    public void testMultiMove3() {
        MoveTest test = new MoveTest()
                .addMove("/a/b/c", "/a/f/c", "/a/b/c")
                .addMove("/a/f/c/d", "/a/b/d", "/a/b/c/d")
                .addMove("/a/b/d/e", "/a/f/c/e", "/a/b/c/d/e")
                // move 'e' and rename it to 'c' -> replacing '/a/b/c'
                .addMove("/a/f/c/e", "/a/b/c", "/a/b/c/d/e");
        test.assertResult();

        // move the 'd' node to the renamed 'e' node at '/a/b/c'.
        test.addMove("/a/b/d", "/a/b/c/d", "/a/b/c/d");
        test.assertResult();
    }

    @Test
    public void testMultiMove4() {
        MoveTest test = new MoveTest()
                .addMove("/a/b/c", "/a/f/c", "/a/b/c")
                .addMove("/a/f/c/d", "/a/b/d", "/a/b/c/d")
                .addMove("/a/b/d/e", "/a/f/c/e", "/a/b/c/d/e")
                // move 'c' to '/a/b/d/' and rename it to 'e'
                .addMove("/a/f/c", "/a/b/d/e", "/a/b/c");
        test.assertResult();
    }

    @Test
    public void testMultiMoveIncludingNewNodes() {
        MoveTest test = new MoveTest()
                .addMove("/a/b/c", "/a/f/c", "/a/b/c")
                .addMove("/a/f/c/d", "/a/b/d", "/a/b/c/d")
                .addMove("/a/b/d/e", "/a/f/c/e", "/a/b/c/d/e")
                // assume /a/b/c and /a/b/c/d have been recreated again (NEW nodes)
                // move 'e' to the new structure -> 'original' setup path wise
                .addMove("/a/b/d/e", "/a/b/c/d/e", "/a/b/c/d/e");
        test.assertResult();
    }


    private static final class MoveTest {

        private final Map<String,String> src2dest = new LinkedHashMap<String, String>();
        private final Map<String,String> dest2orig = new LinkedHashMap<String, String>();
        private final Map<String,String> orig2dest = new LinkedHashMap<String, String>();

        MoveTracker mt = new MoveTracker();

        MoveTest addMove(String src, String dst, String originalSrc) {
            src2dest.put(src, dst);
            dest2orig.put(dst, originalSrc);
            orig2dest.put(originalSrc, dst);

            mt.addMove(src, dst);
            return this;
        }

        MoveTest addMove(String src, String dst, String originalSrc, Tree.Status status) {
            src2dest.put(src, dst);
            dest2orig.put(dst, originalSrc);
            orig2dest.put(originalSrc, dst);

            mt.addMove(src, dst, status);
            return this;
        }

        void assertResult() {
            // map destination -> original path
            for (String dest : src2dest.values()) {
                String expectedOrgSource = dest2orig.get(dest);
                assertEquals(expectedOrgSource, mt.getOriginalSourcePath(dest));
            }

            // map original path -> destination
            for (String original : orig2dest.keySet()) {
                assertEquals(orig2dest.get(original), mt.getDestPath(original));
            }
        }
    }
}