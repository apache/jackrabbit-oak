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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class NamePathMapperImplTest {
    private TestNameMapper mapper = new TestNameMapper(true);
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
    public void testNullName() {
        assertNull(npMapper.getJcrName(null));
        assertNull(npMapper.getOakName(null));
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
        TestNameMapper mapper = new TestNameMapper(false); // a mapper with no prefix remappings present
        NamePathMapper npMapper = new NamePathMapperImpl(mapper);

        checkIdentical(npMapper, "/");
        checkIdentical(npMapper, "/foo:bar");
        checkIdentical(npMapper, "/foo:bar/quu:qux");
        checkIdentical(npMapper, "foo:bar");
    }

    @Test
    public void testOakToJcr() {
        assertEquals("/jcr-foo:bar", npMapper.getJcrPath("/foo:bar"));
        assertEquals("/jcr-foo:bar/jcr-quu:qux", npMapper.getJcrPath("/foo:bar/quu:qux"));
        assertEquals("jcr-foo:bar", npMapper.getJcrPath("foo:bar"));
        assertEquals(".", npMapper.getJcrPath(""));

        try {
            npMapper.getJcrPath("{http://www.jcp.org/jcr/nt/1.0}unstructured");
            fail("expanded name should not be accepted");
        } catch (IllegalStateException expected) {
        }

        try {
            npMapper.getJcrPath("foobar/{http://www.jcp.org/jcr/1.0}content");
            fail("expanded name should not be accepted");
        } catch (IllegalStateException expected) {
        }
    }

    private void checkEquals(NamePathMapper npMapper, String jcrPath) {
        String oakPath = npMapper.getOakPathKeepIndex(jcrPath);
        assertEquals(jcrPath, oakPath);
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
    
    private class TestNameMapper extends AbstractNameMapper {

        private boolean withRemappings;
        private Map<String, String> uri2oakprefix = new HashMap<String, String>();

        public TestNameMapper(boolean withRemappings) {
            this.withRemappings = withRemappings;

            uri2oakprefix.put("", "");
            uri2oakprefix.put("http://www.jcp.org/jcr/1.0", "jcr");
            uri2oakprefix.put("http://www.jcp.org/jcr/nt/1.0", "nt");
            uri2oakprefix.put("http://www.jcp.org/jcr/mix/1.0", "mix");
            uri2oakprefix.put("http://www.w3.org/XML/1998/namespace", "xml");
        }

        @Override
        protected String getJcrPrefix(String oakPrefix) {
            if (oakPrefix.isEmpty() || !withRemappings) {
                return oakPrefix;
            } else {
                return "jcr-" + oakPrefix;
            }
        }

        @Override
        protected String getOakPrefix(String jcrPrefix) {
            if (jcrPrefix.isEmpty() || !withRemappings) {
                return jcrPrefix;
            } else {
                return "oak-" + jcrPrefix;
            }
        }

        @Override
        protected String getOakPrefixFromURI(String uri) {
            return (withRemappings ? "oak-" : "") + uri2oakprefix.get(uri);
        }

        @Override
        public boolean hasSessionLocalMappings() {
            return withRemappings;
        }

    }
}
