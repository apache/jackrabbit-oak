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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LdapIdentityPropertiesTest {

    private LdapIdentityProperties properties = new LdapIdentityProperties();

    @Before
    public void before() {
        properties.put("a", "value");
        properties.put("A", "value2");
    }

    @Test
    public void testConstructorWithInitialSize() {
        LdapIdentityProperties props = new LdapIdentityProperties(5);
        assertTrue(props.isEmpty());
    }

    @Test
    public void testConstructorWithInitialSizeLoadFactor() {
        LdapIdentityProperties props = new LdapIdentityProperties(5, Long.MAX_VALUE);
        assertTrue(props.isEmpty());
    }

    @Test
    public void testConstructorWithMap() {
        Map m = Maps.newLinkedHashMap();
        m.put("b", "v");
        m.put("B", "v2");
        LdapIdentityProperties props = new LdapIdentityProperties(m);
        assertEquals(2, props.size());
        assertEquals(props.get("b"), props.get("B"));
    }

    @Test
    public void testContainsKey() {
        assertTrue(properties.containsKey("a"));
        assertTrue(properties.containsKey("A"));
        assertFalse(properties.containsKey("c"));
    }

    @Test
    public void testContainsKeyNull() {
        assertFalse(properties.containsKey(null));
    }

    @Test
    public void testPut() {
        assertEquals(2, properties.size());
        assertTrue(properties.containsKey("A"));
        assertTrue(properties.containsKey("a"));
    }

    @Test
    public void testPutNull() {
        assertNull(properties.put(null, "value"));
    }

    @Test
    public void testPutAll() {
        Map m = Maps.newLinkedHashMap();
        m.put("b", "v");
        m.put("B", "v2");
        properties.putAll(m);
        assertEquals(4, properties.size());
        assertEquals(properties.get("b"), properties.get("B"));
    }

    @Test
    public void testGet() {
        assertEquals(properties.get("a"), properties.get("A"));
    }

    @Test
    public void testGetNull() {
        assertNull(properties.get(null));
    }

    @Test
    public void testRemove() {
        assertEquals("value2", properties.remove("A"));
        assertEquals(1, properties.size());
        assertEquals("value", properties.remove("a"));
        assertTrue(properties.isEmpty());
    }

    @Test
    public void testClear() {
        properties.clear();
        assertTrue(properties.isEmpty());
        assertFalse(properties.containsKey("a"));
        assertFalse(properties.containsKey("A"));
    }

    @Test
    public void testNonStringKey() {
        properties.put("2", "value");
        assertTrue(properties.containsKey(2));
        assertFalse(properties.containsKey(3));
    }
}