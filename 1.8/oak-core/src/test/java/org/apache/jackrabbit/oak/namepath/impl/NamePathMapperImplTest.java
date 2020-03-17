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
package org.apache.jackrabbit.oak.namepath.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.JcrPathParser;
import org.apache.jackrabbit.oak.commons.UUIDUtils;
import org.junit.Test;

public class NamePathMapperImplTest {

    private static final Map<String, String> GLOBAL = ImmutableMap.of(
            "oak-jcr", "http://www.jcp.org/jcr/1.0",
            "oak-nt", "http://www.jcp.org/jcr/nt/1.0",
            "oak-foo", "http://www.example.com/foo",
            "oak-quu", "http://www.example.com/quu",
            "oak",     "http://jackrabbit.apache.org/oak/ns/1.0");

    private static final Map<String, String> LOCAL = ImmutableMap.of(
            "jcr-jcr", "http://www.jcp.org/jcr/1.0",
            "jcr-nt", "http://www.jcp.org/jcr/nt/1.0",
            "foo", "http://www.example.com/foo",
            "quu", "http://www.example.com/quu");

    private final NameMapper mapper = new LocalNameMapper(GLOBAL, LOCAL);

    private NamePathMapper npMapper = new NamePathMapperImpl(mapper);

    @Test
    public void testInvalidIdentifierPath() {
        String uuid = UUIDUtils.generateUUID();
        List<String> invalid = new ArrayList<String>();
        invalid.add('[' + uuid + "]abc");
        invalid.add('[' + uuid + "]/a/b/c");

        for (String jcrPath : invalid) {
            assertNull(npMapper.getOakPath(jcrPath));
        }
    }

    @Test
    public void testEmptyName() throws RepositoryException {
        assertEquals("", npMapper.getJcrName(""));
        assertEquals("", npMapper.getOakNameOrNull(""));
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
        assertEquals("/a/b[2]/c[3]", npMapper.getOakPath("/a[1]/b[2]/c[03]"));
        assertEquals("/a/b", npMapper.getOakPath("/a/b/a/.."));
    }

    @Test
    public void testJcrToOakKeepIndexNoRemap() {
        NameMapper mapper = new GlobalNameMapper(GLOBAL);
        NamePathMapper npMapper = new NamePathMapperImpl(mapper);

        assertEquals("/", npMapper.getOakPath("/"));
        assertEquals("/foo:bar", npMapper.getOakPath("/foo:bar"));
        assertEquals("/foo:bar/quu:qux", npMapper.getOakPath("/foo:bar/quu:qux"));
        assertEquals("foo:bar", npMapper.getOakPath("foo:bar"));
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
        String[] paths = {
                "//",
                "/foo//",
                "/..//",
                "/..",
                "/foo/../..",
                "foo::bar",
                "foo:bar:baz",
                "foo:bar]baz",
                "foo:bar[baz",
                "foo:bar|baz",
                "foo:bar*baz"
        };

        NamePathMapper[] mappers = {
                npMapper,
                new NamePathMapperImpl(new LocalNameMapper(
                        GLOBAL, Collections.<String, String>emptyMap()))
        };

        for (NamePathMapper mapper : mappers) {
            for (String path : paths) {
                assertNull(path, mapper.getOakPath(path));
            }
        }
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

    @Test
    public void testBracketsInNodeName() throws Exception {
        String[] childNames = { "{A}", "B}", "{C", "(D)", "E)", "(F", };

        for (String name : childNames) {
            assertEquals(name, npMapper.getOakName(name));
        }
    }

    @Test
    public void testBracketsInPaths() throws Exception {
        String[] paths = {
                "/parent/childB1",
                "/parent/}childB2",
                "/parent/{childB3}",
                "/parent/sub/childB4",
                "/parent/sub/}childB5",
                "/parent/sub/{childB6}",
        };

        for (String path : paths) {
            assertEquals(path, npMapper.getOakPath(path));
        }
    }
    
    @Test
    public void testIllegalBracketsInPaths() throws Exception {
        String[] paths = {
                "/parent/sub/{childB7", 
                "/parent/sub/{childB7",
                "/parent/{", 
                "/parent/{childA1", 
                "/parent/{{childA2"        };

        for (String path : paths) {
            assertNull(npMapper.getOakPath(path));
        }
    }    

    @Test
    public void testWhitespace() {
        String[] paths = new String[] {
                " leading", "trailing\n", " ", "\t",
                "oak: leading", "oak:trailing\n", "oak: ", "oak:\t" };
        NamePathMapper noLocal = new NamePathMapperImpl(new LocalNameMapper(
                GLOBAL, Collections.<String, String>emptyMap()));
        for (String path : paths) {
            assertEquals("without local mappings", path, noLocal.getOakPath(path));
            assertEquals("with local mappings", path, npMapper.getOakPath(path));
        }
    }

}
