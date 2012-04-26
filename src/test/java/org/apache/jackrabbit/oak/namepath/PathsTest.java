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

public class PathsTest {

    private TestNameMapper mapper = new TestNameMapper();

    @Test
    public void testValidIdentifierPath() {
        String idPath = '[' + UUID.randomUUID().toString()+ ']';
        Paths.toOakPath(idPath, mapper);
    }

    @Test
    public void testInvalidIdentifierPath() {
        List<String> invalid = new ArrayList();
        invalid.add('[' + UUID.randomUUID().toString()+ "]abc");
        invalid.add('[' + UUID.randomUUID().toString()+ "]/a/b/c");

        for (String jcrPath : invalid) {
            try {
                Paths.toOakPath(jcrPath, mapper);
                fail("Invalid identifier path");
            } catch (Exception e) {
                // success
            }
        }
    }

    @Test
    public void testJcrToOak() {
        assertEquals("/", Paths.toOakPath("/", mapper));
        assertEquals("foo", Paths.toOakPath("{}foo", mapper));
        assertEquals("/oak-foo:bar", Paths.toOakPath("/foo:bar", mapper));
        assertEquals("/oak-foo:bar/oak-quu:qux",
                Paths.toOakPath("/foo:bar/quu:qux", mapper));
        assertEquals("oak-foo:bar", Paths.toOakPath("foo:bar", mapper));
        assertEquals("oak-nt:unstructured", Paths.toOakPath(
                "{http://www.jcp.org/jcr/nt/1.0}unstructured", mapper));
        assertEquals("foobar/oak-jcr:content", Paths.toOakPath(
                "foobar/{http://www.jcp.org/jcr/1.0}content", mapper));
    }

    @Test
    public void testOakToJcr() {
        assertEquals("/jcr-foo:bar", Paths.toJcrPath("/foo:bar", mapper));
        assertEquals("/jcr-foo:bar/jcr-quu:qux",
                Paths.toJcrPath("/foo:bar/quu:qux", mapper));
        assertEquals("jcr-foo:bar", Paths.toJcrPath("foo:bar", mapper));

        try {
            Paths.toJcrPath("{http://www.jcp.org/jcr/nt/1.0}unstructured", mapper);
            fail("expanded name should not be accepted");
        } catch (IllegalStateException expected) {
        }

        try {
            Paths.toJcrPath("foobar/{http://www.jcp.org/jcr/1.0}content", mapper);
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