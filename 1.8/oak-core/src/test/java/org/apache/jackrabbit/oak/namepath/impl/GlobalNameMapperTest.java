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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.namepath.NameMapper;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GlobalNameMapperTest {

    private static final Map<String, String> NAMESPACES = ImmutableMap.of(
            "jcr", "http://www.jcp.org/jcr/1.0",
            "nt", "http://www.jcp.org/jcr/nt/1.0",
            "mix", "http://www.jcp.org/jcr/mix/1.0",
            "foo", "http://www.example.com/foo",
            "quu", "http://www.example.com/quu");

    private NameMapper mapper = new GlobalNameMapper(NAMESPACES);

    @Test
    public void testEmptyName() throws RepositoryException {
        assertEquals("", mapper.getJcrName(""));
        assertEquals("", mapper.getOakNameOrNull(""));
        assertEquals("", mapper.getOakName(""));
    }


    @Test
    public void testSimpleNames() throws RepositoryException {
        List<String> simpleNames = new ArrayList<String>();
        simpleNames.add("foo");
        simpleNames.add(" foo ");
        simpleNames.add("foo.bar");
        simpleNames.add(".");
        simpleNames.add("..");
        simpleNames.add("/");
        simpleNames.add(" ");

        for (String name : simpleNames) {
            assertEquals(name, mapper.getOakNameOrNull(name));
            assertEquals(name, mapper.getOakName(name));
            assertEquals(name, mapper.getJcrName(name));
        }
    }

    @Test
    public void testExpandedNames() throws RepositoryException {
        Map<String, String> jcrToOak = new HashMap<String, String>();
        jcrToOak.put("{}foo", "foo");
        jcrToOak.put("{foo", "{foo");
        jcrToOak.put("{foo}", "{foo}");
        jcrToOak.put("{0} foo", "{0} foo"); // OAK-509
        jcrToOak.put("{", "{");
        jcrToOak.put("{http://www.jcp.org/jcr/nt/1.0}base", "nt:base");
        jcrToOak.put("{http://www.example.com/foo}bar", "foo:bar");
        jcrToOak.put("{http://www.example.com/quu}bar", "quu:bar");

        for (String jcrName : jcrToOak.keySet()) {
            assertEquals(jcrToOak.get(jcrName), mapper.getOakNameOrNull(jcrName));
            assertEquals(jcrToOak.get(jcrName), mapper.getOakName(jcrName));
        }

        assertNull(mapper.getOakNameOrNull("{http://www.example.com/bar}bar"));
        try {
            mapper.getOakName("{http://www.example.com/bar}bar");
            fail("RepositoryException expected");
        } catch (RepositoryException e) {
            // successs
        }
    }

    @Test
    public void testPrefixedNames() throws RepositoryException {
        List<String> prefixed = new ArrayList<String>();
        prefixed.add("nt:base");
        prefixed.add("foo: bar");
        prefixed.add("quu:bar ");
        // unknown prefixes are only captured by the NameValidator
        prefixed.add("unknown:bar");

        for (String name : prefixed) {
            assertEquals(name, mapper.getOakNameOrNull(name));
            assertEquals(name, mapper.getOakName(name));
            assertEquals(name, mapper.getJcrName(name));
        }
    }
}
