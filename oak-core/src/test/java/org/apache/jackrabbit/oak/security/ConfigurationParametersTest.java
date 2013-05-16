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
package org.apache.jackrabbit.oak.security;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;

public class ConfigurationParametersTest {

    @Before
    public void setup() {}

    @After
    public void tearDown() {}

    @Test
    public void testContains() {
        ConfigurationParameters params = ConfigurationParameters.EMPTY;
        assertFalse(params.contains("some"));
        assertFalse(params.contains(""));

        Map<String, String> map = new HashMap<String, String>();
        map.put("key1", "v");
        map.put("key2", "v");
        params = new ConfigurationParameters(map);
        assertTrue(params.contains("key1"));
        assertTrue(params.contains("key2"));
        assertFalse(params.contains("another"));
        assertFalse(params.contains(""));
    }

    @Test
    public void testGetConfigValue() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("o1", "v");
        ConfigurationParameters options = new ConfigurationParameters(map);

        assertEquals("v", options.getConfigValue("o1", "v2"));
        assertEquals("v2", options.getConfigValue("missing", "v2"));
    }

    @Test
    public void testGetNullableConfigValue() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("o1", "v");
        ConfigurationParameters options = new ConfigurationParameters(map);

        assertEquals("v", options.getNullableConfigValue("o1", null));
        assertEquals("v", options.getNullableConfigValue("o1", "v2"));
        assertEquals("v2", options.getNullableConfigValue("missing", "v2"));
        assertEquals(null, options.getNullableConfigValue("missing", null));

    }

    @Test
    public void testDefaultValue() {
        TestObject obj = new TestObject("t");
        Integer int1000 = new Integer(1000);

        ConfigurationParameters options = new ConfigurationParameters();

        assertEquals(obj, options.getConfigValue("missing", obj));
        assertEquals(int1000, options.getConfigValue("missing", int1000));

        assertNull(options.getNullableConfigValue("missing", null));
        assertEquals(obj, options.getNullableConfigValue("missing", obj));
        assertEquals(int1000, options.getNullableConfigValue("missing", int1000));
    }

    @Test
    public void testArrayDefaultValue() {
        TestObject[] testArray = new TestObject[] {new TestObject("t")};

        TestObject[] result = ConfigurationParameters.EMPTY.getConfigValue("test", new TestObject[0]);
        assertNotNull(result);
        assertEquals(0, result.length);
        assertArrayEquals(testArray, ConfigurationParameters.EMPTY.getConfigValue("test", testArray));

        ConfigurationParameters options = new ConfigurationParameters(Collections.singletonMap("test", testArray));
        assertArrayEquals(testArray, options.getConfigValue("test", new TestObject[] {new TestObject("s")}));
    }

    @Test
    public void testArrayDefaultValue2() {
        TestObject[] testArray = new TestObject[] {new TestObject("t")};

        TestObject[] result = ConfigurationParameters.EMPTY.getNullableConfigValue("test", new TestObject[0]);
        assertNotNull(result);
        assertEquals(0, result.length);
        assertArrayEquals(testArray, ConfigurationParameters.EMPTY.getNullableConfigValue("test", testArray));

        ConfigurationParameters options = new ConfigurationParameters(Collections.singletonMap("test", testArray));
        assertArrayEquals(testArray, (TestObject[]) options.getNullableConfigValue("test", null));
        assertArrayEquals(testArray, options.getNullableConfigValue("test", new TestObject[] {new TestObject("s")}));
    }

    @Test
    public void testConversion() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = new Integer(1000);

        Map<String,Object> m = new HashMap<String, Object>();
        m.put("TEST", testObject);
        m.put("String", "1000");
        m.put("Int2", new Integer(1000));
        m.put("Int3", 1000);
        ConfigurationParameters options = new ConfigurationParameters(m);

        assertEquals(testObject, options.getConfigValue("TEST", testObject));
        assertEquals("t", options.getConfigValue("TEST", "defaultString"));

        assertEquals(int1000, options.getConfigValue("String", new Integer(10)));
        assertEquals(new Long(1000), options.getConfigValue("String", new Long(10)));
        assertEquals("1000", options.getConfigValue("String", "10"));

        assertEquals(int1000, options.getConfigValue("Int2", new Integer(10)));
        assertEquals("1000", options.getConfigValue("Int2", "1000"));

        assertEquals(int1000, options.getConfigValue("Int3", new Integer(10)));
        assertEquals("1000", options.getConfigValue("Int3", "1000"));
    }

    @Test
    public void testConversion2() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = new Integer(1000);

        Map<String,Object> m = new HashMap<String, Object>();
        m.put("TEST", testObject);
        m.put("String", "1000");
        m.put("Int2", new Integer(1000));
        m.put("Int3", 1000);
        ConfigurationParameters options = new ConfigurationParameters(m);

        assertNotNull(options.getNullableConfigValue("TEST", null));
        assertEquals(testObject, options.getNullableConfigValue("TEST", null));

        assertEquals(testObject, options.getNullableConfigValue("TEST", testObject));
        assertEquals("t", options.getNullableConfigValue("TEST", "defaultString"));

        assertEquals("1000", options.getNullableConfigValue("String", null));
        assertEquals(int1000, options.getNullableConfigValue("String", new Integer(10)));
        assertEquals(new Long(1000), options.getNullableConfigValue("String", new Long(10)));
        assertEquals("1000", options.getNullableConfigValue("String", "10"));

        assertEquals(int1000, options.getNullableConfigValue("Int2", null));
        assertEquals(int1000, options.getNullableConfigValue("Int2", new Integer(10)));
        assertEquals("1000", options.getNullableConfigValue("Int2", "1000"));

        assertEquals(1000, options.getNullableConfigValue("Int3", null));
        assertEquals(int1000, options.getNullableConfigValue("Int3", new Integer(10)));
        assertEquals("1000", options.getNullableConfigValue("Int3", "1000"));
    }

    @Test
    public void testNullValue() {
        ConfigurationParameters options = new ConfigurationParameters(Collections.singletonMap("test", null));

        assertNull(options.getConfigValue("test", null));
        assertEquals("value", options.getConfigValue("test", "value"));
        TestObject to = new TestObject("t");
        assertEquals(to, options.getConfigValue("test", to));
        assertFalse(options.getConfigValue("test", false));
    }

    @Test
    public void testNullValue2() {
        ConfigurationParameters options = new ConfigurationParameters(Collections.singletonMap("test", null));

        assertNull(options.getNullableConfigValue("test", null));
        assertNull(options.getNullableConfigValue("test", "value"));
        assertNull(options.getNullableConfigValue("test", "value"));
        assertNull(options.getNullableConfigValue("test", new TestObject("t")));
        assertNull(options.getNullableConfigValue("test", false));
    }

    private class TestObject {

        private final String name;

        private TestObject(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }

        public int hashCode() {
            return name.hashCode();
        }

        public boolean equals(Object object) {
            if (object == this) {
                return true;
            }
            if (object instanceof TestObject) {
                return name.equals(((TestObject) object).name);
            }
            return false;
        }
    }
}