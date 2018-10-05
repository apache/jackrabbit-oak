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
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MoveTrackerTest {

    @Test
    public void testIsEmpty() {
        assertTrue(new MoveTracker().isEmpty());
    }

    /**
     * 1.   >/a:/b >/c:/d     =  >/c:/d >/a:b
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 222
     */
    @Test
    public void test1() {
        MoveTracker mt = new MoveTracker();
        mt.addMove("/a", "/b");
        mt.addMove("/c", "/d");

        assertEquals("/a", mt.getSourcePath("/b"));
        assertEquals("/c", mt.getSourcePath("/d"));
        assertEquals("/b", mt.getDestPath("/a"));
        assertEquals("/d", mt.getDestPath("/c"));
    }

    /**
     * 4.   >/a/b:/c >/a:/d   =  >/a:/d >/d/b:/c
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 225
     */
    @Test
    public void test4() {
        MoveTracker mt1 = new MoveTracker();
        mt1.addMove("/a/b", "/c");
        mt1.addMove("/a", "/d");

        assertEquals("/a/b", mt1.getSourcePath("/c"));
        assertEquals("/a", mt1.getSourcePath("/d"));
        assertEquals("/c", mt1.getDestPath("/a/b"));
        assertEquals("/d", mt1.getDestPath("/a"));
    }

    /**
     * 4.   >/a/b:/c >/a:/c/d    does not commute  (q < s)
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 226
     */
    @Test
    public void test4a() {
        MoveTracker mt2 = new MoveTracker();
        mt2.addMove("/a/b", "/c");
        mt2.addMove("/a", "/c/d");

        assertEquals("/a/b", mt2.getSourcePath("/c"));
        assertEquals("/a", mt2.getSourcePath("/c/d"));
        assertEquals("/c", mt2.getDestPath("/a/b"));
        assertEquals("/c/d", mt2.getDestPath("/a"));
    }

    /**
     * 7.   >/a:/b >/c:/a        does not commute
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 231
     */
    @Test
    public void test7() {
        MoveTracker mt3 = new MoveTracker();
        mt3.addMove("/a", "/b");
        mt3.addMove("/c", "/a");

        assertEquals("/a", mt3.getSourcePath("/b"));
        assertEquals("/c", mt3.getSourcePath("/a"));
        assertEquals("/b", mt3.getDestPath("/a"));
        assertEquals("/a", mt3.getDestPath("/c"));
    }

    /**
     * 10.  >/a:/b >/b/c:/d   =  >/a/c:/d >/a:/b
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 234
     */
    @Test
    public void test10() {
        MoveTracker mt = new MoveTracker();
        mt.addMove("/a", "/b");
        mt.addMove("/b/c", "/d");

        assertEquals("/a", mt.getSourcePath("/b"));
        assertEquals("/a/c", mt.getSourcePath("/d"));
        assertEquals("/b", mt.getDestPath("/a"));
        assertEquals("/d", mt.getDestPath("/a/c"));
    }

    /**
     * 11.  >/a:/b >/b:/c     =  >/a:/c
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 236
     */
    @Test
    public void test11() {
        MoveTracker mt = new MoveTracker();
        mt.addMove("/a", "/b");
        mt.addMove("/b", "/c");

        assertEquals("/a", mt.getSourcePath("/c"));
        assertEquals("/c", mt.getDestPath("/a"));
    }

    /**
     * 12.  >/a:/b/c >/b:/d   =  >/b:/d >/a:/d/c
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 237
     */
    @Test
    @Ignore("Known Limitation of OAK-710")
    public void test12() {
        MoveTracker mt4 = new MoveTracker();
        mt4.addMove("/a", "/b/c");
        mt4.addMove("/b", "/d");

        assertEquals("/a", mt4.getSourcePath("/d/c"));
        assertEquals("/b", mt4.getSourcePath("/d"));
        assertEquals("/d/c", mt4.getDestPath("/a"));
        assertEquals("/d", mt4.getDestPath("/b"));
    }

    /**
     * 14.  >/a:/b >/c:/b/d   =  >/c:/a/d >/a:/b
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 240
     */
    @Test
    public void test14() {
        MoveTracker mt5 = new MoveTracker();
        mt5.addMove("/a", "/b");
        mt5.addMove("/c", "/b/d");

        assertEquals("/a", mt5.getSourcePath("/b"));
        assertEquals("/c", mt5.getSourcePath("/b/d"));
        assertEquals("/b", mt5.getDestPath("/a"));
        assertEquals("/b/d", mt5.getDestPath("/c"));
    }

    /**
     * 14.  >/a/b:/b >/a:/b/d    does not commute  (p > r)
     * See http://svn.apache.org/viewvc/jackrabbit/sandbox/jackrabbit-microkernel/src/main/java/org/apache/jackrabbit/state/ChangeLog.java?view=markup
     * Line 241
     */
    @Test
    public void test14a() {
        MoveTracker mt6 = new MoveTracker();
        mt6.addMove("/a/b", "/b");
        mt6.addMove("/a", "/b/d");

        assertEquals("/a/b", mt6.getSourcePath("/b"));
        assertEquals("/a", mt6.getSourcePath("/b/d"));
        assertEquals("/b", mt6.getDestPath("/a/b"));
        assertEquals("/b/d", mt6.getDestPath("/a"));
    }

    @Test
    public void testNotEmpty() {
        MoveTracker mt = new MoveTracker();
        mt.addMove("/a/b/c", "/d/e/f");
        assertFalse(mt.isEmpty());
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

            assertEquals(src, mt.getSourcePath(dest));
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
            assertEquals(src, mt.getSourcePath(dest));
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

        void assertResult() {
            // map destination -> original path
            for (String dest : src2dest.values()) {
                String expectedOrgSource = dest2orig.get(dest);
                assertEquals(expectedOrgSource, mt.getSourcePath(dest));
            }

            // map original path -> destination
            for (String original : orig2dest.keySet()) {
                assertEquals(orig2dest.get(original), mt.getDestPath(original));
            }
        }
    }
}