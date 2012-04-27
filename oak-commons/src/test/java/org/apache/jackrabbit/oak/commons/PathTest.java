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

import junit.framework.TestCase;

import java.util.Iterator;

public class PathTest extends TestCase {

    public void test() {

        try {
            PathUtils.getParentPath("invalid/path/");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            PathUtils.getName("invalid/path/");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        test("parent", "child");
        test("x", "y");
    }

    private void test(String parent, String child) {

        // split
        assertEquals(0, PathUtils.split("").length);
        assertEquals(0, PathUtils.split("/").length);
        assertEquals(1, PathUtils.split(parent).length);
        assertEquals(2, PathUtils.split(parent + "/" + child).length);
        assertEquals(1, PathUtils.split("/" + parent).length);
        assertEquals(2, PathUtils.split("/" + parent + "/" + child).length);
        assertEquals(3, PathUtils.split("/" + parent + "/" + child + "/" + child).length);
        assertEquals(parent, PathUtils.split(parent)[0]);
        assertEquals(parent, PathUtils.split(parent + "/" + child)[0]);
        assertEquals(child, PathUtils.split(parent + "/" + child)[1]);
        assertEquals(child, PathUtils.split(parent + "/" + child + "/" + child + "1")[1]);
        assertEquals(child + "1", PathUtils.split(parent + "/" + child + "/" + child + "1")[2]);

        // concat
        assertEquals(parent + "/" + child, PathUtils.concat(parent, child));
        try {
            assertEquals(parent + "/" + child, PathUtils.concat(parent + "/", "/" + child));
            fail();
        } catch (IllegalArgumentException e) {
            // expected
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
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat("/", "/");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        // denotesRoot
        assertTrue(PathUtils.denotesRoot("/"));
        assertFalse(PathUtils.denotesRoot("/" + parent));

        // getDepth
        assertEquals(0, PathUtils.getDepth("/"));
        assertEquals(1, PathUtils.getDepth("/" + parent));
        assertEquals(2, PathUtils.getDepth("/" + parent + "/" + child));
        assertEquals(1, PathUtils.getDepth(parent));
        assertEquals(2, PathUtils.getDepth(parent + "/" + child));

        // getName
        assertEquals("", PathUtils.getName("/"));
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
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat(invalid, "x");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat("/x", invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat("/x", "y", invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.concat(invalid, "y", "z");
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.getDepth(invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.getName(invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.getNextSlash(invalid, 0);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.getParentPath(invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.isAbsolute(invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.relativize(invalid, invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.relativize("/test", invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            PathUtils.split(invalid);
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testPathElements() {
        String[] paths = new String[]{"", "/", "/a", "a", "/abc/def/ghj", "abc/def/ghj"};
        for (String path : paths) {
            String[] elements = PathUtils.split(path);
            Iterator<String> it = PathUtils.elements(path).iterator();
            for (String element : elements) {
                assertTrue(it.hasNext());
                assertEquals(element, it.next());
            }
            assertFalse(it.hasNext());
        }

        String[] invalidPaths = new String[]{"//", "/a/", "a/", "/a//", "a//b"};
        for (String path: invalidPaths) {
            try {
                PathUtils.elements(path);
                fail();
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

}
