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
package org.apache.jackrabbit.oak.commons;

import java.util.Set;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Test the PathUtils class.
 */
public class PathUtilsTest extends TestCase {
    static boolean assertsEnabled;

    static {
        assert assertsEnabled = true;
    }

    @Test
    public void test() {

        try {
            PathUtils.getParentPath("invalid/path/");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }

        try {
            PathUtils.getName("invalid/path/");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }

        test("parent", "child");
        test("x", "y");
    }

    @Test
    public void testGetDepth() {
        assertEquals(0, PathUtils.getDepth("/"));
        assertEquals(0, PathUtils.getDepth(""));
        assertEquals(1, PathUtils.getDepth("/a"));
        assertEquals(1, PathUtils.getDepth("a"));
        assertEquals(2, PathUtils.getDepth("/a/b"));
        assertEquals(2, PathUtils.getDepth("a/b"));
    }

    @Test
    public void testConcatRelativePaths() {
        assertNull(PathUtils.concatRelativePaths("", "", ""));
        assertNull(PathUtils.concatRelativePaths());

        assertEquals("a/b/c", PathUtils.concatRelativePaths("a", "b", "c"));
        assertEquals("a/b/c", PathUtils.concatRelativePaths("a", "b/c"));
        assertEquals("a/b/c", PathUtils.concatRelativePaths("a/b/c", ""));
        assertEquals("a/b/c", PathUtils.concatRelativePaths("a/b", "c"));
        assertEquals("a/b/c", PathUtils.concatRelativePaths("/", "a", "", "b/c/"));
    }


    private static int getElementCount(String path) {
        int count = 0;
        for (String p : PathUtils.elements(path)) {
            assertFalse(PathUtils.isAbsolute(p));
            count++;
        }
        return count;
    }

    private static String getElement(String path, int index) {
        int count = 0;
        for (String p : PathUtils.elements(path)) {
            if (index == count++) {
                return p;
            }
        }
        fail();
        return "";
    }

    private static void test(String parent, String child) {

        // split
        assertEquals(0, getElementCount(""));
        assertEquals(0, getElementCount("/"));
        assertEquals(1, getElementCount(parent));
        assertEquals(2, getElementCount(parent + "/" + child));
        assertEquals(1, getElementCount("/" + parent));
        assertEquals(2, getElementCount("/" + parent + "/" + child));
        assertEquals(3, getElementCount("/" + parent + "/" + child + "/" + child));
        assertEquals(parent, getElement(parent, 0));
        assertEquals(parent, getElement(parent + "/" + child, 0));
        assertEquals(child, getElement(parent + "/" + child, 1));
        assertEquals(child, getElement(parent + "/" + child + "/" + child + "1", 1));
        assertEquals(child + "1", getElement(parent + "/" + child + "/" + child + "1", 2));

        // concat
        assertEquals(parent + "/" + child, PathUtils.concat(parent, child));
        try {
            assertEquals(parent + "/" + child, PathUtils.concat(parent + "/", "/" + child));
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (IllegalArgumentException e) {
            if (assertsEnabled) {
                throw e;
            }
        } catch (AssertionError e) {
            if (!assertsEnabled) {
                throw e;
            }
        }
        try {
            assertEquals(parent + "/" + child, PathUtils.concat(parent, "/" + child));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(parent + "/" + child + "/" + child, PathUtils.concat(parent, child, child));
        assertEquals(parent + "/" + child, PathUtils.concat(parent, "", child));
        assertEquals(parent + "/" + child, PathUtils.concat(parent, child, ""));
        assertEquals(child + "/" + child, PathUtils.concat("", child, child));
        assertEquals(child, PathUtils.concat("", child, ""));
        assertEquals(child, PathUtils.concat("", "", child));
        assertEquals(child, PathUtils.concat("", child));

        assertEquals("/" + child, PathUtils.concat("", "/" + child));
        assertEquals(parent + "/" + child, PathUtils.concat(parent, child));
        try {
            PathUtils.concat("/" + parent, "/" + child);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat("", "//");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.concat("/", "/");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (IllegalArgumentException e) {
            // expected
        }

        // denotesRoot
        assertTrue(PathUtils.denotesRoot("/"));
        assertTrue(PathUtils.denotesRoot(PathUtils.ROOT_PATH));
        assertFalse(PathUtils.denotesRoot("/" + parent));

        // getName
        assertEquals("", PathUtils.getName("/"));
        assertEquals(PathUtils.ROOT_NAME, PathUtils.getName(PathUtils.ROOT_PATH));
        assertEquals(parent, PathUtils.getName("/" + parent));
        assertEquals(child, PathUtils.getName("/" + parent + "/" + child));

        // getParentPath
        assertEquals("/", PathUtils.getParentPath("/"));
        assertEquals("/", PathUtils.getParentPath("/" + parent));
        assertEquals("/" + parent, PathUtils.getParentPath("/" + parent + "/" + child));

        // getAncestorPath
        assertEquals("/", PathUtils.getAncestorPath("/", 1));
        assertEquals("/", PathUtils.getAncestorPath("/" + parent, 1));
        assertEquals("/" + parent, PathUtils.getAncestorPath("/" + parent + "/" + child, 1));
        assertEquals("/" + parent + "/" + child, PathUtils.getAncestorPath("/" + parent + "/" + child, 0));
        assertEquals("/", PathUtils.getAncestorPath("/" + parent + "/" + child, 2));

        assertEquals(PathUtils.getParentPath("/foo"), PathUtils.getAncestorPath("/foo", 1));
        assertEquals(PathUtils.getParentPath("/foo/bar"), PathUtils.getAncestorPath("/foo/bar", 1));
        assertEquals(PathUtils.getParentPath("foo/bar"), PathUtils.getAncestorPath("foo/bar", 1));
        assertEquals(PathUtils.getParentPath("foo"), PathUtils.getAncestorPath("foo", 1));

        // isAbsolute
        assertEquals(true, PathUtils.isAbsolute("/"));
        assertEquals(false, PathUtils.isAbsolute(parent));
        assertEquals(true, PathUtils.isAbsolute("/" + parent));
        assertEquals(false, PathUtils.isAbsolute(child));
        assertEquals(true, PathUtils.isAbsolute("/" + parent + "/" + child));
        assertEquals(false, PathUtils.isAbsolute(parent + "/" + child));

        // isAncestor
        assertFalse(PathUtils.isAncestor("/", "/"));
        assertFalse(PathUtils.isAncestor("/" + parent, "/" + parent));
        assertFalse(PathUtils.isAncestor(parent, parent));
        assertTrue(PathUtils.isAncestor("/", "/" + parent));
        assertTrue(PathUtils.isAncestor(parent, parent + "/" + child));
        assertFalse(PathUtils.isAncestor("/", parent + "/" + child));
        assertTrue(PathUtils.isAncestor("/" + parent, "/" + parent + "/" + child));
        assertFalse(PathUtils.isAncestor(parent, child));
        assertFalse(PathUtils.isAncestor("/" + parent, "/" + parent + "123"));
        assertFalse(PathUtils.isAncestor("/" + parent, "/" + parent + "123/foo"));

        // relativize
        assertEquals("", PathUtils.relativize("/", "/"));
        assertEquals("", PathUtils.relativize("/" + parent, "/" + parent));
        assertEquals(child, PathUtils.relativize("/" + parent, "/" + parent + "/" + child));
        assertEquals(parent + "/" + child, PathUtils.relativize("/", "/" + parent + "/" + child));
        try {
            PathUtils.relativize("x/y", "y/x");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    public void testMore() {
        String[] paths = {
                "",
                "/",
                "foo",
                "/foo",
                "foo/bar",
                "/foo/bar",
                "foo/bar/baz",
                "/foo/bar/baz",
                "x",
                "/x",
                "x/y",
                "/x/y",
                "x/y/z",
                "/x/y/z",
        };

        for (String path : paths) {
            String parent = PathUtils.getParentPath(path);
            String name = PathUtils.getName(path);
            String concat = PathUtils.concat(parent, name);

            assertEquals("original: " + path + " parent: " + parent +
                    " name: " + name + " concat: " + concat,
                    path, concat);
        }
    }

    public void testNextSlash() {
        String path = "/test/a";
        int n = PathUtils.getNextSlash(path, 0);
        assertEquals(0, n);
        n = PathUtils.getNextSlash(path, n + 1);
        assertEquals(5, n);
        n = PathUtils.getNextSlash(path, n + 1);
        assertEquals(-1, n);
    }

    public void testValidate() {
        for (String invalid : new String[]{
                "//",
                "//test",
                "/test/",
                "test/",
                "/test//",
                "/test//test",
                "//x",
                "/x/",
                "x/",
                "/x//",
                "/x//x",
        }) {
            try {
                PathUtils.validate(invalid);
                fail(invalid);
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
        for (String valid : new String[]{
                "",
                "/",
                "test",
                "test/test",
                "/test",
                "/test/test",
                "x",
                "x/x",
                "/x",
                "/x/x",
        }) {
            PathUtils.validate(valid);
        }
        try {
            PathUtils.concat("", "/test", "");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

    }

    public void testValidateEverything() {
        String invalid = "/test/test//test/test";
        try {
            PathUtils.denotesRoot(invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.concat(invalid, "x");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.concat("/x", invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (IllegalArgumentException e) {
            if (assertsEnabled) {
                throw e;
            }
        } catch (AssertionError e) {
            if (!assertsEnabled) {
                throw e;
            }
        }
        try {
            PathUtils.concat("/x", "y", invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (IllegalArgumentException e) {
            if (assertsEnabled) {
                throw e;
            }
        } catch (AssertionError e) {
            if (!assertsEnabled) {
                throw e;
            }
        }
        try {
            PathUtils.concat(invalid, "y", "z");
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.getDepth(invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.getName(invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.getNextSlash(invalid, 0);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.getParentPath(invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.isAbsolute(invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.relativize(invalid, invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
        try {
            PathUtils.relativize("/test", invalid);
            if (assertsEnabled) {
                fail();
            }
        } catch (AssertionFailedError e) {
            throw e;
        } catch (AssertionError e) {
            // expected
        }
    }

    public void testPathElements() {
        String[] invalidPaths = new String[]{"//", "/a/", "a/", "/a//", "a//b"};
        for (String path: invalidPaths) {
            try {
                PathUtils.elements(path);
                fail();
            } catch (AssertionError e) {
                // expected
            }
        }
    }

    public void testElements() {
        String path = "a/b/c";
        String[] elementsArray = path.split("/");
        Iterable<String> elementsIterable = PathUtils.elements(path);
        int k = 0;
        for (String name : elementsIterable) {
            Assert.assertEquals(elementsArray[k++], name);
        }
        assertEquals(3, k);

        k = 0;
        for (String name : elementsIterable) {
            Assert.assertEquals(elementsArray[k++], name);
        }
        assertEquals(3, k);
    }

    public void testOptimizeForIncludes() throws Exception{
        Set<String> includes = newHashSet("/a", "/a/b");
        Set<String> excludes = newHashSet("/a/b");
        PathUtils.unifyInExcludes(includes, excludes);
        assertEquals("Excludes supercedes include", newHashSet("/a"), includes);
        assertEquals(newHashSet("/a/b"), excludes);

        includes = newHashSet("/a", "/a/b/c");
        excludes = newHashSet("/a/b");
        PathUtils.unifyInExcludes(includes, excludes);
        assertEquals("Excludes supercedes include", newHashSet("/a"), includes);
        assertEquals(newHashSet("/a/b"), excludes);

        includes = newHashSet("/a", "/a/b/c");
        excludes = newHashSet();
        PathUtils.unifyInExcludes(includes, excludes);
        assertEquals(newHashSet("/a"), includes);
    }

    public void testOptimizeForExcludes() throws Exception{
        Set<String> includes = newHashSet("/a", "/b");
        Set<String> excludes = newHashSet("/c");
        PathUtils.unifyInExcludes(includes, excludes);
        assertEquals(newHashSet("/a", "/b"), includes);
        assertEquals(newHashSet(), excludes);
    }
}
