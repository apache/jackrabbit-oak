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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class NamePathMapperImplTest {

    private static final Map<String, String> GLOBAL = ImmutableMap.of(
            "oak-jcr", "http://www.jcp.org/jcr/1.0",
            "oak-nt", "http://www.jcp.org/jcr/nt/1.0",
            "oak-mix", "http://www.jcp.org/jcr/mix/1.0",
            "oak-foo", "http://www.example.com/foo",
            "oak-quu", "http://www.example.com/quu");

    private static final Map<String, String> LOCAL = ImmutableMap.of(
            "jcr-jcr", "http://www.jcp.org/jcr/1.0",
            "jcr-nt", "http://www.jcp.org/jcr/nt/1.0",
            "jcr-mix", "http://www.jcp.org/jcr/mix/1.0",
            "foo", "http://www.example.com/foo",
            "quu", "http://www.example.com/quu");

    private NameMapper mapper = new LocalNameMapper(LOCAL) {
        @Override
        protected Map<String, String> getNamespaceMap() {
            return GLOBAL;
        }
    };

    private NamePathMapper npMapper = new NamePathMapperImpl(mapper);

    @Test
    public void testInvalidIdentifierPath() {
        String uuid = IdentifierManager.generateUUID();
        List<String> invalid = new ArrayList<String>();
        invalid.add('[' + uuid + "]abc");
        invalid.add('[' + uuid + "]/a/b/c");

        for (String jcrPath : invalid) {
            assertNull(npMapper.getOakPath(jcrPath));
        }
    }

    @Test
    public void testEmptyName() {
        assertEquals("", npMapper.getJcrName(""));
        assertEquals("", npMapper.getOakName(""));
    }

    @Test
    public void testTrailingSlash() {
        assertEquals("/oak-foo:bar/oak-quu:qux",npMapper.getOakPath("/foo:bar/quu:qux/"));
        assertEquals("/a/b/c",npMapper.getOakPath("/a/b/c/"));
    }

    @Test
    public void testJcrToOak() {
        assertEquals("/", npMapper.getOakPath("/"));
        assertEquals("foo", npMapper.getOakPath("{}foo"));
        assertEquals("/oak-foo:bar", npMapper.getOakPath("/foo:bar"));
        assertEquals("/oak-foo:bar/oak-quu:qux", npMapper.getOakPath("/foo:bar/quu:qux"));
        assertEquals("oak-foo:bar", npMapper.getOakPath("foo:bar"));
        assertEquals("oak-nt:unstructured", npMapper.getOakPath("{http://www.jcp.org/jcr/nt/1.0}unstructured"));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("foobar", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/.."));
        assertEquals("", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/../.."));
        assertEquals("..", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/../../.."));
        assertEquals("../..", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/../../../.."));
        assertEquals("oak-jcr:content", npMapper.getOakPath("foobar/../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("../oak-jcr:content", npMapper.getOakPath("foobar/../../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("..", npMapper.getOakPath(".."));
        assertEquals("", npMapper.getOakPath("."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPath("foobar/{http://www.jcp.org/jcr/1.0}content/./."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPath("foobar/./{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("oak-jcr:content", npMapper.getOakPath("foobar/./../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("/a/b/c", npMapper.getOakPath("/a/b[1]/c[01]"));
    }

    @Test
    public void testJcrToOakKeepIndex() {
        assertEquals("/", npMapper.getOakPathKeepIndex("/"));
        assertEquals("foo", npMapper.getOakPathKeepIndex("{}foo"));
        assertEquals("/oak-foo:bar", npMapper.getOakPathKeepIndex("/foo:bar"));
        assertEquals("/oak-foo:bar/oak-quu:qux", npMapper.getOakPathKeepIndex("/foo:bar/quu:qux"));
        assertEquals("oak-foo:bar", npMapper.getOakPathKeepIndex("foo:bar"));
        assertEquals("oak-nt:unstructured", npMapper.getOakPathKeepIndex("{http://www.jcp.org/jcr/nt/1.0}unstructured"));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("foobar", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/.."));
        assertEquals("", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/../.."));
        assertEquals("..", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/../../.."));
        assertEquals("../..", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/../../../.."));
        assertEquals("oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("../oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/../../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("..", npMapper.getOakPathKeepIndex(".."));
        assertEquals("", npMapper.getOakPathKeepIndex("."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/{http://www.jcp.org/jcr/1.0}content/./."));
        assertEquals("foobar/oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/./{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("oak-jcr:content", npMapper.getOakPathKeepIndex("foobar/./../{http://www.jcp.org/jcr/1.0}content"));
        assertEquals("/a/b[1]/c[1]", npMapper.getOakPathKeepIndex("/a/b[1]/c[01]"));
    }

    @Test
    public void testJcrToOakKeepIndexNoRemap() {
        NameMapper mapper = new GlobalNameMapper() {
            @Override
            protected Map<String, String> getNamespaceMap() {
                return GLOBAL;
            }
        };
        NamePathMapper npMapper = new NamePathMapperImpl(mapper);

        checkIdentical(npMapper, "/");
        checkIdentical(npMapper, "/foo:bar");
        checkIdentical(npMapper, "/foo:bar/quu:qux");
        checkIdentical(npMapper, "foo:bar");
    }

    @Test
    public void testOakToJcr() {
        assertEquals("/foo:bar", npMapper.getJcrPath("/oak-foo:bar"));
        assertEquals("/foo:bar/quu:qux", npMapper.getJcrPath("/oak-foo:bar/oak-quu:qux"));
        assertEquals("foo:bar", npMapper.getJcrPath("oak-foo:bar"));
        assertEquals(".", npMapper.getJcrPath(""));

        try {
            npMapper.getJcrPath("{http://www.jcp.org/jcr/nt/1.0}unstructured");
            fail("expanded name should not be accepted");
        } catch (IllegalArgumentException expected) {
        }

        try {
            npMapper.getJcrPath("foobar/{http://www.jcp.org/jcr/1.0}content");
            fail("expanded name should not be accepted");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testInvalidJcrPaths() {
        assertNull(npMapper.getOakPath("//"));
        assertNull(npMapper.getOakPath("/foo//"));
        assertNull(npMapper.getOakPath("/..//"));
        assertNull(npMapper.getOakPath("/.."));
        assertNull(npMapper.getOakPath("/foo/../.."));
    }

    @Test
    public void testInvalidOakPaths() {
        getJcrPath("//");
        getJcrPath("/foo//");
        getJcrPath("/..//");
        getJcrPath("/..");
        getJcrPath("/foo/../..");
    }

    private void getJcrPath(String path) {
        try {
            npMapper.getJcrPath(path);
            fail("Expected IllegalArgumentException");
        }
        catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testValidateInvalidPaths() {
        assertFalse(JcrPathParser.validate("//"));
        assertFalse(JcrPathParser.validate("/foo//"));
        assertFalse(JcrPathParser.validate("/..//"));
        assertFalse(JcrPathParser.validate("/.."));
        assertFalse(JcrPathParser.validate("/foo/../.."));
    }

    private void checkIdentical(NamePathMapper npMapper, String jcrPath) {
        String oakPath = npMapper.getOakPathKeepIndex(jcrPath);
        checkIdentical(jcrPath, oakPath);
    }

    private static void checkIdentical(String expected, String actual) {
        assertEquals(expected, actual);
        if (expected != actual) {
            fail("Expected the strings to be the same");
        }
    }

}
