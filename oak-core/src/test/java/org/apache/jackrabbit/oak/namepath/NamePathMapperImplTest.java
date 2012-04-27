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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NamePathMapperImplTest {

    private TestNameMapper mapper = new TestNameMapper();
    private NamePathMapper npMapper = new NamePathMapperImpl(mapper);

    @Test
    public void testValidIdentifierPath() {
        String idPath = '[' + UUID.randomUUID().toString()+ ']';
        npMapper.toOakPath(idPath);
    }

    @Test
    public void testInvalidIdentifierPath() {
        List<String> invalid = new ArrayList();
        invalid.add('[' + UUID.randomUUID().toString()+ "]abc");
        invalid.add('[' + UUID.randomUUID().toString()+ "]/a/b/c");

        for (String jcrPath : invalid) {
            try {
                npMapper.toOakPath(jcrPath);
                fail("Invalid identifier path");
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testJcrToOak() {
        assertEquals("/", npMapper.toOakPath("/"));
        assertEquals("foo", npMapper.toOakPath("{}foo"));
        assertEquals("/oak-foo:bar", npMapper.toOakPath("/foo:bar"));
        assertEquals("/oak-foo:bar/oak-quu:qux",
                npMapper.toOakPath("/foo:bar/quu:qux"));
        assertEquals("oak-foo:bar", npMapper.toOakPath("foo:bar"));
        assertEquals("oak-nt:unstructured", npMapper.toOakPath(
                "{http://www.jcp.org/jcr/nt/1.0}unstructured"));
        assertEquals("foobar/oak-jcr:content", npMapper.toOakPath(
                "foobar/{http://www.jcp.org/jcr/1.0}content"));
    }

    @Test
    public void testOakToJcr() {
        assertEquals("/jcr-foo:bar", npMapper.toJcrPath("/foo:bar"));
        assertEquals("/jcr-foo:bar/jcr-quu:qux",
                npMapper.toJcrPath("/foo:bar/quu:qux"));
        assertEquals("jcr-foo:bar", npMapper.toJcrPath("foo:bar"));

        try {
            npMapper.toJcrPath("{http://www.jcp.org/jcr/nt/1.0}unstructured");
            fail("expanded name should not be accepted");
        } catch (IllegalStateException expected) {
        }

        try {
            npMapper.toJcrPath("foobar/{http://www.jcp.org/jcr/1.0}content");
            fail("expanded name should not be accepted");
        } catch (IllegalStateException expected) {
        }
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