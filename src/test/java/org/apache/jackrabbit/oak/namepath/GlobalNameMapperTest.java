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

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GlobalNameMapperTest {

    private static final Map<String, String> NAMESPACES = ImmutableMap.of(
            "jcr", "http://www.jcp.org/jcr/1.0",
            "nt", "http://www.jcp.org/jcr/nt/1.0",
            "mix", "http://www.jcp.org/jcr/mix/1.0",
            "foo", "http://www.example.com/foo",
            "quu", "http://www.example.com/quu");

    private NameMapper mapper = new GlobalNameMapper() {
        @Override
        protected Map<String, String> getNamespaceMap() {
            return NAMESPACES;
        }
    };

    @Test
    public void testEmptyName() {
        assertEquals("", mapper.getJcrName(""));
        assertEquals("", mapper.getOakName(""));
    }


    @Test
    public void testSimpleNames() {
        assertEquals("foo", mapper.getOakName("foo"));
        assertEquals("foo", mapper.getJcrName("foo"));
        assertEquals("foo ", mapper.getOakName("foo "));
        assertEquals("foo ", mapper.getJcrName("foo "));
        assertEquals(" foo ", mapper.getOakName(" foo "));
        assertEquals(" foo ", mapper.getJcrName(" foo "));
        assertEquals("foo.bar", mapper.getOakName("foo.bar"));
        assertEquals("foo.bar", mapper.getJcrName("foo.bar"));
        assertEquals(".", mapper.getOakName("."));
        assertEquals(".", mapper.getJcrName("."));
        assertEquals("..", mapper.getOakName(".."));
        assertEquals("..", mapper.getJcrName(".."));
        assertEquals("/", mapper.getOakName("/"));
        assertEquals("/", mapper.getJcrName("/"));
        assertEquals(" ", mapper.getOakName(" "));
        assertEquals(" ", mapper.getJcrName(" "));
    }

    @Test
    public void testExpandedNames() {
        assertEquals("foo", mapper.getOakName("{}foo"));
        assertEquals("{foo", mapper.getOakName("{foo"));
        assertEquals("{foo}", mapper.getOakName("{foo}"));
        assertEquals("{0} foo", mapper.getOakName("{0} foo")); // OAK-509
        assertEquals("{", mapper.getOakName("{"));
        assertEquals("nt:base", mapper.getOakName("{http://www.jcp.org/jcr/nt/1.0}base"));
        assertEquals("foo:bar", mapper.getOakName("{http://www.example.com/foo}bar"));
        assertEquals("quu:bar", mapper.getOakName("{http://www.example.com/quu}bar"));
        assertNull(mapper.getOakName("{http://www.example.com/bar}bar"));
    }

    @Test
    public void testPrefixedNames() {
        assertEquals("nt:base", mapper.getOakName("nt:base"));
        assertEquals("nt:base", mapper.getJcrName("nt:base"));
        assertEquals("foo: bar", mapper.getOakName("foo: bar"));
        assertEquals("foo: bar", mapper.getJcrName("foo: bar"));
        assertEquals("quu:bar ", mapper.getOakName("quu:bar "));
        assertEquals("quu:bar ", mapper.getJcrName("quu:bar "));

        // unknown prefixes are only captured by the NameValidator
        assertEquals("unknown:bar", mapper.getOakName("unknown:bar"));
        assertEquals("unknown:bar", mapper.getJcrName("unknown:bar"));
    }

}
