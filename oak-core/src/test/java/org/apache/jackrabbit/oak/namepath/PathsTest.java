/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.jackrabbit.oak.namepath;

import org.apache.jackrabbit.oak.util.Function1;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PathsTest {
    private static final Function1<String, String> RESOLVER = new Function1<String, String>() {
        private final Map<String, String> map = new HashMap<String, String>();
        {
            map.put("a", "x");
            map.put("b", "y");
            map.put("c", "z");
        }

        @Override
        public String apply(String argument) {
            return map.get(argument);
        }
    };

    @Test
    public void getPrefix() {
        assertEquals(null, Paths.getPrefixFromElement("foo"));
        assertEquals("foo", Paths.getPrefixFromElement("foo:bar"));
    }
    
    @Test
    public void getName() {
        assertEquals("foo", Paths.getNameFromElement("foo"));
        assertEquals("bar", Paths.getNameFromElement("foo:bar"));
    }

    @Test
    public void isValidElement() {
        assertTrue(Paths.isValidElement("foo"));
        assertTrue(Paths.isValidElement("foo:bar"));

        assertFalse(Paths.isValidElement(""));
        assertFalse(Paths.isValidElement(":"));
        assertFalse(Paths.isValidElement("foo:"));
        assertFalse(Paths.isValidElement("fo/o:"));
        assertFalse(Paths.isValidElement(":bar"));
    }

    @Test
    public void isValidAbsolutePath() {
        assertTrue(Paths.isValidPath("/"));
        assertTrue(Paths.isValidPath("/a/b/c"));
        assertTrue(Paths.isValidPath("/p:a/q:b/r:c"));

        assertFalse(Paths.isValidPath(""));
        assertFalse(Paths.isValidPath("/a/b/c/"));
        assertFalse(Paths.isValidPath("/p:a/:b/r:c"));
        assertFalse(Paths.isValidPath("/p:a/q:/r:c"));
        assertFalse(Paths.isValidPath("/p:a/:/r:c"));
        assertFalse(Paths.isValidPath("/p:a//r:c"));
        assertFalse(Paths.isValidPath("//"));
    }
    
    @Test
    public void isValidRelativePath() {
        assertTrue(Paths.isValidPath("a/b/c"));
        assertTrue(Paths.isValidPath("p:a/q:b/r:c"));

        assertFalse(Paths.isValidPath("a/b/c/"));
        assertFalse(Paths.isValidPath("p:a/:b/r:c"));
        assertFalse(Paths.isValidPath("p:a/q:/r:c"));
        assertFalse(Paths.isValidPath("p:a/:/r:c"));
        assertFalse(Paths.isValidPath("p:a//r:c"));
    }

    @Test
    public void resolveElement() {
        assertEquals("foo", Paths.resolveElement("foo", RESOLVER));
        assertEquals("x:foo", Paths.resolveElement("a:foo", RESOLVER));

        try {
            Paths.resolveElement("q:foo", RESOLVER);
            fail();
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void resolveAbsolutePath() {
        assertEquals("/foo", Paths.resolvePath("/foo", RESOLVER));
        assertEquals("/foo/bar", Paths.resolvePath("/foo/bar", RESOLVER));
        assertEquals("/x:foo", Paths.resolvePath("/a:foo", RESOLVER));
        assertEquals("/x:foo/y:bar", Paths.resolvePath("/a:foo/b:bar", RESOLVER));

        try {
            Paths.resolvePath("/a:foo/q:bar", RESOLVER);
            fail();
        }
        catch (IllegalArgumentException expected) {
        }

    }

    @Test
    public void resolveRelativePath() {
        assertEquals("foo", Paths.resolvePath("foo", RESOLVER));
        assertEquals("foo/bar", Paths.resolvePath("foo/bar", RESOLVER));
        assertEquals("x:foo", Paths.resolvePath("a:foo", RESOLVER));
        assertEquals("x:foo/y:bar", Paths.resolvePath("a:foo/b:bar", RESOLVER));

        try {
            Paths.resolvePath("a:foo/q:bar", RESOLVER);
            fail();
        }
        catch (IllegalArgumentException expected) {
        }

    }

}
