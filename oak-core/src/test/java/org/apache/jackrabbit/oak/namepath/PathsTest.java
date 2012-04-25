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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class PathsTest {

    private TestNameMapper mapper = new TestNameMapper();
    
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
    public void testJcrToOak() {
        assertEquals("/oak-foo:bar", Paths.toOakPath("/foo:bar", mapper));
        assertEquals("/oak-foo:bar/oak-quu:qux", Paths.toOakPath("/foo:bar/quu:qux", mapper));
        assertEquals("oak-foo:bar", Paths.toOakPath("foo:bar", mapper));
        assertEquals("oak-nt:unstructured", Paths.toOakPath("{http://www.jcp.org/jcr/nt/1.0}unstructured", mapper));
        assertEquals("foobar/oak-jcr:content", Paths.toOakPath("foobar/{http://www.jcp.org/jcr/1.0}content", mapper));
    }
    
    private class TestNameMapper extends AbstractNameMapper {

        private Map<String, String> uri2oakprefix = new HashMap<String, String>();
        
        public TestNameMapper() {
            uri2oakprefix.put("", "");
            uri2oakprefix.put("http://www.jcp.org/jcr/1.0", "jcr");
            uri2oakprefix.put("http://www.jcp.org/jcr/nt/1.0", "nt");
            uri2oakprefix.put("http://www.jcp.org/jcr/mix/1.0", "mix");
            uri2oakprefix.put("http://www.w3.org/XML/1998/namespace", "xml");
        }
        
        @Override
        protected String getJcrPrefix(String oakPrefix) {
            if (oakPrefix.length() == 0) {
                return oakPrefix;
            } else {
                return "jcr-" + oakPrefix;
            }
        }

        @Override
        protected String getOakPrefix(String jcrPrefix) {
            if (jcrPrefix.length() == 0) {
                return jcrPrefix;
            } else {
                return "oak-" + jcrPrefix;
            }
        }

        @Override
        protected String getOakPrefixFromURI(String uri) {
            return "oak-" + uri2oakprefix.get(uri);
        }
        
    }
}