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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Path.ROOT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PathTest {

    private final Path root = ROOT;
    private final Path foo = new Path(root, "foo");
    private final Path fooBar = new Path(foo, "bar");
    private final Path fooBarQuux = new Path(fooBar, "quux");
    private final Path relFoo = new Path("foo");
    private final Path relFooBar = new Path(relFoo, "bar");
    private final Path relFooBarQuux = new Path(relFooBar, "quux");

    @Test
    public void equals() {
        assertEquals(ROOT, Path.fromString("/"));
        assertEquals(foo, Path.fromString("/foo"));
        assertEquals(fooBar, Path.fromString("/foo/bar"));
        assertEquals(relFoo, Path.fromString("foo"));
        assertEquals(relFooBar, Path.fromString("foo/bar"));
        assertNotEquals(fooBar, Path.fromString("foo/bar"));
        assertNotEquals(relFooBar, Path.fromString("/foo/bar"));
    }

    @Test
    public void pathToString() {
        assertEquals("/", root.toString());
        assertEquals("/foo", foo.toString());
        assertEquals("/foo/bar", fooBar.toString());
        assertEquals("/foo/bar/quux", fooBarQuux.toString());
        assertEquals("foo", relFoo.toString());
        assertEquals("foo/bar", relFooBar.toString());
        assertEquals("foo/bar/quux", relFooBarQuux.toString());
    }

    @Test
    public void toStringBuilder() {
        StringBuilder sb = new StringBuilder();
        root.toStringBuilder(sb);
        assertEquals(root.toString(), sb.toString());
        sb.setLength(0);
        foo.toStringBuilder(sb);
        assertEquals(foo.toString(), sb.toString());
        sb.setLength(0);
        fooBar.toStringBuilder(sb);
        assertEquals(fooBar.toString(), sb.toString());
        sb.setLength(0);
        fooBarQuux.toStringBuilder(sb);
        assertEquals(fooBarQuux.toString(), sb.toString());
        sb.setLength(0);
        relFoo.toStringBuilder(sb);
        assertEquals(relFoo.toString(), sb.toString());
        sb.setLength(0);
        relFooBar.toStringBuilder(sb);
        assertEquals(relFooBar.toString(), sb.toString());
        sb.setLength(0);
        relFooBarQuux.toStringBuilder(sb);
        assertEquals(relFooBarQuux.toString(), sb.toString());
    }

    @Test
    public void fromString() {
        assertEquals(root, Path.fromString(root.toString()));
        assertEquals(foo, Path.fromString(foo.toString()));
        assertEquals(fooBar, Path.fromString(fooBar.toString()));
        assertEquals(fooBarQuux, Path.fromString(fooBarQuux.toString()));
        assertEquals(relFoo, Path.fromString(relFoo.toString()));
        assertEquals(relFooBar, Path.fromString(relFooBar.toString()));
        assertEquals(relFooBarQuux, Path.fromString(relFooBarQuux.toString()));
    }

    @Test
    public void length() {
        assertEquals(root.toString().length(), root.length());
        assertEquals(foo.toString().length(), foo.length());
        assertEquals(fooBar.toString().length(), fooBar.length());
        assertEquals(fooBarQuux.toString().length(), fooBarQuux.length());
        assertEquals(relFoo.toString().length(), relFoo.length());
        assertEquals(relFooBar.toString().length(), relFooBar.length());
        assertEquals(relFooBarQuux.toString().length(), relFooBarQuux.length());
    }

    @Test
    public void isRoot() {
        assertTrue(root.isRoot());
        assertFalse(foo.isRoot());
        assertFalse(fooBar.isRoot());
        assertFalse(fooBarQuux.isRoot());
        assertFalse(relFoo.isRoot());
        assertFalse(relFooBar.isRoot());
        assertFalse(relFooBarQuux.isRoot());
    }

    @Test
    public void getParent() {
        assertNull(root.getParent());
        assertEquals(foo.getParent(), root);
        assertEquals(fooBar.getParent(), foo);
        assertEquals(fooBarQuux.getParent(), fooBar);
        assertNull(relFoo.getParent());
        assertEquals(relFooBar.getParent(), relFoo);
        assertEquals(relFooBarQuux.getParent(), relFooBar);
    }

    @Test
    public void getDepth() {
        assertEquals(PathUtils.getDepth(root.toString()), root.getDepth());
        assertEquals(PathUtils.getDepth(foo.toString()), foo.getDepth());
        assertEquals(PathUtils.getDepth(fooBar.toString()), fooBar.getDepth());
        assertEquals(PathUtils.getDepth(fooBarQuux.toString()), fooBarQuux.getDepth());
        assertEquals(PathUtils.getDepth(relFoo.toString()), relFoo.getDepth());
        assertEquals(PathUtils.getDepth(relFooBar.toString()), relFooBar.getDepth());
        assertEquals(PathUtils.getDepth(relFooBarQuux.toString()), relFooBarQuux.getDepth());
    }

    @Test
    public void getAncestor() {
        assertEquals(root, root.getAncestor(-1));
        assertEquals(root, root.getAncestor(0));
        assertEquals(root, root.getAncestor(1));
        assertEquals(foo, foo.getAncestor(0));
        assertEquals(root, foo.getAncestor(1));
        assertEquals(root, foo.getAncestor(2));
        assertEquals(fooBar, fooBar.getAncestor(0));
        assertEquals(foo, fooBar.getAncestor(1));
        assertEquals(root, fooBar.getAncestor(2));
        assertEquals(root, fooBar.getAncestor(3));
        assertEquals(fooBar, fooBarQuux.getAncestor(1));

        assertEquals(relFoo, relFoo.getAncestor(-1));
        assertEquals(relFoo, relFoo.getAncestor(0));
        assertEquals(relFoo, relFoo.getAncestor(1));
        assertEquals(relFooBar, relFooBar.getAncestor(0));
        assertEquals(relFoo, relFooBar.getAncestor(1));
        assertEquals(relFoo, relFooBar.getAncestor(2));
        assertEquals(relFoo, relFooBar.getAncestor(3));
        assertEquals(relFooBar, relFooBarQuux.getAncestor(1));
    }

    @Test
    public void getName() {
        assertEquals("", root.getName());
        assertEquals("foo", foo.getName());
        assertEquals("bar", fooBar.getName());
        assertEquals("quux", fooBarQuux.getName());
        assertEquals("foo", relFoo.getName());
        assertEquals("bar", relFooBar.getName());
        assertEquals("quux", relFooBarQuux.getName());
    }

    @Test
    public void elements() {
        assertThat(root.elements(), emptyIterable());
        assertThat(foo.elements(), contains("foo"));
        assertThat(fooBar.elements(), contains("foo", "bar"));
        assertThat(fooBarQuux.elements(), contains("foo", "bar", "quux"));
        assertThat(relFoo.elements(), contains("foo"));
        assertThat(relFooBar.elements(), contains("foo", "bar"));
        assertThat(relFooBarQuux.elements(), contains("foo", "bar", "quux"));
    }

    @Test
    public void isAncestorOf() {
        assertTrue(root.isAncestorOf(foo));
        assertTrue(root.isAncestorOf(fooBar));
        assertTrue(foo.isAncestorOf(fooBar));
        assertTrue(fooBar.isAncestorOf(fooBarQuux));
        assertFalse(root.isAncestorOf(root));
        assertFalse(foo.isAncestorOf(root));
        assertFalse(foo.isAncestorOf(foo));
        assertFalse(fooBar.isAncestorOf(fooBar));
        assertFalse(fooBar.isAncestorOf(foo));
        assertFalse(fooBar.isAncestorOf(root));

        assertFalse(root.isAncestorOf(relFoo));
        assertFalse(root.isAncestorOf(relFooBar));
        assertFalse(relFoo.isAncestorOf(relFoo));
        assertFalse(relFooBar.isAncestorOf(relFoo));
        assertFalse(relFooBar.isAncestorOf(relFooBar));
        assertFalse(relFooBarQuux.isAncestorOf(relFooBar));
        assertFalse(relFooBarQuux.isAncestorOf(relFooBarQuux));
        assertTrue(relFoo.isAncestorOf(relFooBar));
        assertTrue(relFooBar.isAncestorOf(relFooBarQuux));

        assertFalse(foo.isAncestorOf(relFooBar));
        assertFalse(foo.isAncestorOf(relFooBarQuux));
        assertFalse(relFoo.isAncestorOf(fooBar));
        assertFalse(relFoo.isAncestorOf(fooBarQuux));
    }

    @Test
    public void isAbsolute() {
        assertTrue(ROOT.isAbsolute());
        assertTrue(foo.isAbsolute());
        assertTrue(fooBar.isAbsolute());
        assertTrue(fooBarQuux.isAbsolute());
        assertFalse(relFoo.isAbsolute());
        assertFalse(relFooBar.isAbsolute());
        assertFalse(relFooBarQuux.isAbsolute());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyName() {
        new Path(ROOT, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromStringWithEmptyString() {
        Path.fromString("");
    }

    @Test
    public void compareTo() {
        Path baz = Path.fromString("/baz");
        Path fooBaz = Path.fromString("/foo/baz");
        Path relFooBaz = Path.fromString("foo/baz");
        List<Path> paths = new ArrayList<>();
        paths.add(root);
        paths.add(baz);
        paths.add(foo);
        paths.add(fooBar);
        paths.add(fooBaz);
        paths.add(fooBarQuux);
        paths.add(relFoo);
        paths.add(relFooBar);
        paths.add(relFooBaz);
        for (int i = 0; i < 20; i++) {
            Collections.shuffle(paths);
            Collections.sort(paths);
            assertThat(paths, contains(root, baz, foo, fooBar, fooBarQuux, fooBaz, relFoo, relFooBar, relFooBaz));
        }
    }
}
