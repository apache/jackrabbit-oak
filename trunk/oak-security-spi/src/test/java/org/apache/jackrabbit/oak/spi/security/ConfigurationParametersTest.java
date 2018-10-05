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
package org.apache.jackrabbit.oak.spi.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConfigurationParametersTest {

    @Before
    public void setup() {}

    @After
    public void tearDown() {}

    @Test
    public void testCreation() {
        ConfigurationParameters params = ConfigurationParameters.of(
                ConfigurationParameters.EMPTY,
                null,
                ConfigurationParameters.of("a", "a"));
        assertFalse(params.isEmpty());
        assertEquals(1, params.size());
        assertTrue(params.contains("a"));
    }

    @Test
    public void testCreationFromNull() {
        ConfigurationParameters cp = null;
        ConfigurationParameters params = ConfigurationParameters.of(new ConfigurationParameters[] {cp});
        assertSame(ConfigurationParameters.EMPTY, params);
    }

    @Test
    public void testCreationFromMap() {
        Map<String, String> m = ImmutableMap.of("a", "b");
        ConfigurationParameters cp = ConfigurationParameters.of(m);
        assertEquals(m.size(), cp.size());

        // verify shortcut if the passed map is already an instanceof ConfigurationParameters
        ConfigurationParameters cp2 = ConfigurationParameters.of(cp);
        assertSame(cp, cp2);
    }

    @Test
    public void testCreationFromEmptyProperties() {
        ConfigurationParameters cp = ConfigurationParameters.of(new Properties());
        assertSame(ConfigurationParameters.EMPTY, cp);
    }

    @Test
    public void testCreationFromProperties() {
        Properties properties = new Properties();
        properties.put("a", "b");

        ConfigurationParameters cp = ConfigurationParameters.of(properties);
        assertEquals(ImmutableSet.copyOf(properties.keySet()), ImmutableSet.copyOf(cp.keySet()));
        assertEquals(ImmutableSet.copyOf(properties.values()), ImmutableSet.copyOf(cp.values()));

    }

    @Test
    public void testCreationFromEmptyDictionary() {
        Dictionary dict = new Properties();
        ConfigurationParameters cp = ConfigurationParameters.of(dict);
        assertSame(ConfigurationParameters.EMPTY, cp);
    }

    @Test
    public void testCreationFromDictionary() {
        Dictionary dict = new Properties();
        dict.put("a", "b");

        ConfigurationParameters cp = ConfigurationParameters.of(dict);
        assertEquals(ImmutableSet.copyOf(Iterators.forEnumeration(dict.keys())), ImmutableSet.copyOf(cp.keySet()));
        assertEquals(ImmutableSet.copyOf(Iterators.forEnumeration(dict.elements())), ImmutableSet.copyOf(cp.values()));

    }

    @Test
    public void testContains() {
        ConfigurationParameters params = ConfigurationParameters.EMPTY;
        assertFalse(params.contains("some"));
        assertFalse(params.contains(""));

        Map<String, String> map = new HashMap<String, String>();
        map.put("key1", "v");
        map.put("key2", "v");
        params = ConfigurationParameters.of(map);
        assertTrue(params.contains("key1"));
        assertTrue(params.contains("key2"));
        assertFalse(params.contains("another"));
        assertFalse(params.contains(""));
    }

    @Test
    public void testGetConfigValue() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("o1", "v");
        ConfigurationParameters options = ConfigurationParameters.of(map);

        assertEquals("v", options.getConfigValue("o1", "v2"));
        assertEquals("v2", options.getConfigValue("missing", "v2"));
    }

    @Test
    public void testGetNullableConfigValue() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("o1", "v");
        ConfigurationParameters options = ConfigurationParameters.of(map);

        assertEquals("v", options.getConfigValue("o1", null, null));
        assertEquals("v", options.getConfigValue("o1", null, String.class));

        assertEquals("v", options.getConfigValue("o1", "v2", null));
        assertEquals("v", options.getConfigValue("o1", "v2", String.class));

        assertEquals("v2", options.getConfigValue("missing", "v2", null));
        assertEquals("v2", options.getConfigValue("missing", "v2", String.class));

        assertNull(options.getConfigValue("missing", null, null));
        assertNull(options.getConfigValue("missing", null, TestObject.class));

    }

    @Test
    public void testDefaultValue() {
        TestObject obj = new TestObject("t");
        Integer int1000 = 1000;

        ConfigurationParameters options = ConfigurationParameters.EMPTY;

        assertEquals(obj, options.getConfigValue("missing", obj));
        assertEquals(int1000, options.getConfigValue("missing", int1000));

        assertNull(options.getConfigValue("missing", null, null));
        assertNull(options.getConfigValue("missing", null, String.class));
        assertEquals(obj, options.getConfigValue("missing", obj, null));
        assertEquals(obj, options.getConfigValue("missing", obj, TestObject.class));
        assertEquals(int1000, options.getConfigValue("missing", int1000, Integer.class));
    }

    @Test
    public void testArrayDefaultValue() {
        TestObject[] testArray = new TestObject[] {new TestObject("t")};

        TestObject[] result = ConfigurationParameters.EMPTY.getConfigValue("test", new TestObject[0]);
        assertNotNull(result);
        assertEquals(0, result.length);
        assertArrayEquals(testArray, ConfigurationParameters.EMPTY.getConfigValue("test", testArray));

        ConfigurationParameters options = ConfigurationParameters.of("test", testArray);
        assertArrayEquals(testArray, options.getConfigValue("test", new TestObject[] {new TestObject("s")}));
    }

    @Test
    public void testArrayDefaultValue2() {
        TestObject[] testArray = new TestObject[] {new TestObject("t")};

        TestObject[] result = ConfigurationParameters.EMPTY.getConfigValue("test", new TestObject[0], null);
        assertNotNull(result);
        assertEquals(0, result.length);
        assertArrayEquals(testArray, ConfigurationParameters.EMPTY.getConfigValue("test", testArray, null));
        assertArrayEquals(testArray, ConfigurationParameters.EMPTY.getConfigValue("test", testArray, TestObject[].class));

        ConfigurationParameters options = ConfigurationParameters.of("test", testArray);
        assertArrayEquals(testArray, (TestObject[]) options.getConfigValue("test", null, null));
        assertArrayEquals(testArray, options.getConfigValue("test", null, TestObject[].class));
        assertArrayEquals(testArray, options.getConfigValue("test", new TestObject[]{new TestObject("s")}, null));
        assertArrayEquals(testArray, options.getConfigValue("test", new TestObject[]{new TestObject("s")}, TestObject[].class));
    }

    @Test
    public void testCollectionAsArray() throws Exception{
        String[] testArray = {"t"};
        ConfigurationParameters options = ConfigurationParameters.of("test", Arrays.asList(testArray));
        assertArrayEquals(testArray, options.getConfigValue("test", null, String[].class));
    }

    @Test
    public void testConversion() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = 1000;

        Map<String,Object> m = new HashMap<String, Object>();
        m.put("TEST", testObject);
        m.put("String", "1000");
        m.put("Int2", 1000);
        m.put("Int3", 1000);
        m.put("time0", "1s");
        m.put("time1", 1000);
        ConfigurationParameters options = ConfigurationParameters.of(m);

        assertEquals(testObject, options.getConfigValue("TEST", testObject));
        assertEquals("t", options.getConfigValue("TEST", "defaultString"));

        assertTrue(1000 == options.getConfigValue("String", 10, int.class));
        assertTrue(1000 == options.getConfigValue("String", 10));
        assertEquals(int1000, options.getConfigValue("String", 10));
        assertEquals(new Long(1000), options.getConfigValue("String", 10l));
        assertEquals("1000", options.getConfigValue("String", "10"));

        assertEquals(int1000, options.getConfigValue("Int2", 10));
        assertEquals("1000", options.getConfigValue("Int2", "1000"));

        assertEquals(int1000, options.getConfigValue("Int3", 10));
        assertEquals("1000", options.getConfigValue("Int3", "1000"));

        assertEquals(ConfigurationParameters.Milliseconds.of(1000), options.getConfigValue("time0", ConfigurationParameters.Milliseconds.NULL));
        assertEquals(ConfigurationParameters.Milliseconds.of(1000), options.getConfigValue("time1", ConfigurationParameters.Milliseconds.NULL));
    }

    @Test
    public void testConversion2() {
        TestObject testObject = new TestObject("t");
        Integer int1000 = 1000;

        Map<String,Object> m = new HashMap<String, Object>();
        m.put("TEST", testObject);
        m.put("String", "1000");
        m.put("Int2", 1000);
        m.put("Int3", 1000);
        ConfigurationParameters options = ConfigurationParameters.of(m);

        assertNotNull(options.getConfigValue("TEST", null, null));
        assertNotNull(options.getConfigValue("TEST", null, TestObject.class));

        assertEquals(testObject, options.getConfigValue("TEST", null, null));
        assertEquals(testObject, options.getConfigValue("TEST", null, TestObject.class));

        assertEquals(testObject, options.getConfigValue("TEST", testObject, null));
        assertEquals(testObject, options.getConfigValue("TEST", testObject, TestObject.class));

        assertEquals("t", options.getConfigValue("TEST", "defaultString", null));
        assertEquals("t", options.getConfigValue("TEST", "defaultString", String.class));

        assertEquals("1000", options.getConfigValue("String", null, null));
        assertEquals("1000", options.getConfigValue("String", null, String.class));

        assertEquals(int1000, options.getConfigValue("String", 10, null));
        assertEquals(int1000, options.getConfigValue("String", 10, Integer.class));

        assertEquals(new Long(1000), options.getConfigValue("String", 10l, null));
        assertEquals(new Long(1000), options.getConfigValue("String", 10l, Long.class));

        assertEquals("1000", options.getConfigValue("String", "10", null));
        assertEquals("1000", options.getConfigValue("String", "10", String.class));

        assertEquals(int1000, options.getConfigValue("Int2", null, null));
        assertEquals(int1000, options.getConfigValue("Int2", 10, null));
        assertEquals("1000", options.getConfigValue("Int2", "1000", null));

        assertEquals(int1000, options.getConfigValue("Int3", null, null));
        assertEquals(int1000, options.getConfigValue("Int3", 10, null));
        assertEquals("1000", options.getConfigValue("Int3", "1000", null));
    }

    @Test
    public void testImpossibleConversion() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("string", "v");
        map.put("obj", new TestObject("test"));
        map.put("int", 10);
        ConfigurationParameters options = ConfigurationParameters.of(map);

        Map<String, Class> impossible = new HashMap<String, Class>();
        impossible.put("string", TestObject.class);
        impossible.put("string", Integer.class);
        impossible.put("string", Calendar.class);
        impossible.put("obj", Integer.class);
        impossible.put("int", TestObject.class);
        impossible.put("int", Calendar.class);

        for (String key : impossible.keySet()) {
            try {
                options.getConfigValue(key, null, impossible.get(key));
                fail("Impossible conversion for " + key + " to " + impossible.get(key));
            } catch (IllegalArgumentException e) {
                // success
            }
        }
    }

    @Test
    public void testConversionToSet() {
        String[] stringArray = new String[] {"a", "b"};
        Set<String> stringSet = ImmutableSet.copyOf(stringArray);

        TestObject[] testObjectArray = new TestObject[] {new TestObject("a"), new TestObject("b")};
        Set<TestObject> testObjectSet = ImmutableSet.copyOf(testObjectArray);

        // map of config value (key) and expected result set.
        Map<Object, Set> configValues = new HashMap<Object, Set>();
        configValues.put("a", ImmutableSet.of("a"));
        configValues.put(stringArray, stringSet);
        configValues.put(stringSet, stringSet);
        configValues.put(testObjectArray, testObjectSet);
        configValues.put(testObjectSet, testObjectSet);
        configValues.put(new String[0], Collections.<String>emptySet());
        configValues.put(new HashSet(), Collections.emptySet());
        configValues.put(ImmutableSet.of(), Collections.emptySet());
        configValues.put(new ArrayList(), Collections.emptySet());
        configValues.put(ConfigurationParameters.EMPTY, Collections.<String>emptySet());

        Set<String> defaultStrings = ImmutableSet.of("abc", "def", "ghi");
        Set<TestObject> defaultObjects = ImmutableSet.of(new TestObject("abc"), new TestObject("def"));

        for (Object value : configValues.keySet()) {
            ConfigurationParameters config;
            if (value instanceof ConfigurationParameters) {
                config = ConfigurationParameters.of((ConfigurationParameters) value);
            } else {
                config = ConfigurationParameters.of("key", value);
            }

            Set expected = configValues.get(value);
            assertEquals(expected, config.getConfigValue("key", Collections.emptySet()));
            assertEquals(expected, config.getConfigValue("key", Collections.<String>emptySet()));
            assertEquals(expected, config.getConfigValue("key", ImmutableSet.of()));

            assertEquals(expected, config.getConfigValue("key", Collections.emptySet(), Set.class));
            assertEquals(expected, config.getConfigValue("key", Collections.<String>emptySet(), Set.class));
            assertEquals(expected, config.getConfigValue("key", ImmutableSet.of(), Set.class));

            // test with default values
            if (!config.containsKey("key")) {
                assertEquals(defaultStrings, config.getConfigValue("key", defaultStrings, Set.class));
                assertEquals(defaultObjects, config.getConfigValue("key", defaultObjects, Set.class));
                assertEquals(null, config.getConfigValue("key", null, Set.class));
                assertEquals(defaultStrings, config.getConfigValue("key", defaultStrings));
                assertEquals(defaultObjects, config.getConfigValue("key", defaultObjects));
            } else {
                assertEquals(expected, config.getConfigValue("key", defaultStrings, Set.class));
                assertEquals(expected, config.getConfigValue("key", defaultObjects, Set.class));
                assertEquals(expected, config.getConfigValue("key", null, Set.class));
                assertEquals(expected, config.getConfigValue("key", defaultStrings));
                assertEquals(expected, config.getConfigValue("key", defaultObjects));
            }

            // non existing kez with default values
            assertEquals(defaultStrings, config.getConfigValue("nonexisting", defaultStrings));
            assertEquals(defaultStrings, config.getConfigValue("nonexisting", defaultStrings, Set.class));
            assertEquals(defaultObjects, config.getConfigValue("nonexisting", defaultObjects));
            assertEquals(defaultObjects, config.getConfigValue("nonexisting", defaultObjects, Set.class));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConversionToSet() {
        ConfigurationParameters params = ConfigurationParameters.of("long", 23);
        params.getConfigValue("long", null, Set.class);
    }

    @Test
    public void testInvalidConversionToMilliseconds() {
        ConfigurationParameters params = ConfigurationParameters.of("str", "abc");
        assertNull(params.getConfigValue("str", null, ConfigurationParameters.Milliseconds.class));
    }

    @Test
    public void testConversionToLong() {
        ConfigurationParameters params = ConfigurationParameters.of("l1", 1, "l2", Long.valueOf(2));
        assertEquals(1, params.getConfigValue("l1", null, Long.class).longValue());
        assertEquals(1, params.getConfigValue("l1", null, long.class).longValue());
        assertEquals(2, params.getConfigValue("l2", null, Long.class).longValue());
        assertEquals(2, params.getConfigValue("l2", null, long.class).longValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConversionToLong() {
        ConfigurationParameters params = ConfigurationParameters.of("str", "abc");
        params.getConfigValue("str", null, Long.class);
    }

    @Test
    public void testConversionToFloat() {
        ConfigurationParameters params = ConfigurationParameters.of("f1", 1.1, "f2", new Float(2.2));
        assertEquals(1.1, params.getConfigValue("f1", null, Float.class).floatValue(), 0.01);
        assertEquals(1.1, params.getConfigValue("f1", null, float.class).floatValue(), 0.01);
        assertEquals(2.2, params.getConfigValue("f2", null, Float.class).floatValue(), 0.01);
        assertEquals(2.2, params.getConfigValue("f2", null, float.class).floatValue(), 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConversionToFloat() {
        ConfigurationParameters params = ConfigurationParameters.of("str", "abc");
        params.getConfigValue("str", null, Float.class);
    }

    @Test
    public void testConversionToDouble() {
        ConfigurationParameters params = ConfigurationParameters.of("d1", 1.1, "d2", new Double(2.2));
        assertEquals(1.1, params.getConfigValue("d1", null, Double.class).doubleValue(), 0.01);
        assertEquals(1.1, params.getConfigValue("d1", null, double.class).doubleValue(), 0.01);
        assertEquals(2.2, params.getConfigValue("d2", null, Double.class).doubleValue(), 0.01);
        assertEquals(2.2, params.getConfigValue("d2", null, double.class).doubleValue(), 0.01);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidConversionToDouble() {
        ConfigurationParameters params = ConfigurationParameters.of("str", "abc");
        params.getConfigValue("str", null, Double.class);
    }

    @Test
    public void testConversionToBoolean() {
        ConfigurationParameters params = ConfigurationParameters.of("b1", true, "b2", Boolean.FALSE);
        assertTrue(params.getConfigValue("b1", null, Boolean.class));
        assertTrue(params.getConfigValue("b1", null, boolean.class));

        assertFalse(params.getConfigValue("b2", null, Boolean.class));
        assertFalse(params.getConfigValue("b2", null, boolean.class));
    }

    @Test
    public void testInvalidConversionToBoolean() {
        ConfigurationParameters params = ConfigurationParameters.of("str", "abc");
        assertFalse(params.getConfigValue("str", null, Boolean.class));
    }

    @Test
    public void testConversionToStringArray() {
        String[] stringArray = new String[] {"a", "b"};
        Set<String> stringSet = ImmutableSet.copyOf(stringArray);

        TestObject[] testObjectArray = new TestObject[] {new TestObject("a"), new TestObject("b")};
        Set<TestObject> testObjectSet = ImmutableSet.copyOf(testObjectArray);

        String[] defaultStrings = new String[]{"abc", "def", "ghi"};

        // map of config value (key) and expected result set.
        Map<Object, Object[]> configValues = new HashMap<Object, Object[]>();
        configValues.put("a", new String[] {"a"});
        configValues.put(stringArray, stringArray);
        configValues.put(stringSet, stringArray);
        configValues.put(testObjectArray, stringArray);
        configValues.put(testObjectSet, stringArray);
        configValues.put(new String[0], new String[0]);
        configValues.put(new HashSet(), new String[0]);
        configValues.put(ImmutableSet.of(), new String[0]);
        configValues.put(new ArrayList(), new String[0]);
        configValues.put(ConfigurationParameters.EMPTY, new String[0]);

        for (Object value : configValues.keySet()) {
            ConfigurationParameters config;
            if (value instanceof ConfigurationParameters) {
                config = ConfigurationParameters.of((ConfigurationParameters) value);
            } else {
                config = ConfigurationParameters.of("key", value);
            }
            Object[] expected = configValues.get(value);

            assertArrayEquals(expected, config.getConfigValue("key", new String[0]));
            assertArrayEquals(expected, config.getConfigValue("key", new String[0], String[].class));

            // test with default values
            if (!config.containsKey("key")) {
                assertArrayEquals(defaultStrings, config.getConfigValue("key", defaultStrings, String[].class));
                assertArrayEquals(null, config.getConfigValue("key", null, String[].class));
                assertArrayEquals(defaultStrings, config.getConfigValue("key", defaultStrings));
            } else {
                assertArrayEquals(expected, config.getConfigValue("key", defaultStrings, String[].class));
                assertArrayEquals(expected, config.getConfigValue("key", null, String[].class));
                assertArrayEquals(expected, config.getConfigValue("key", defaultStrings));
            }

            // non existing kez with default values
            assertArrayEquals(defaultStrings, config.getConfigValue("nonexisting", defaultStrings));
            assertArrayEquals(defaultStrings, config.getConfigValue("nonexisting", defaultStrings, String[].class));
        }
    }

    @Test
    public void testNullValue() {
        ConfigurationParameters options = ConfigurationParameters.of(Collections.singletonMap("test", null));

        assertNull(options.getConfigValue("test", null));
        assertEquals("value", options.getConfigValue("test", "value"));
        TestObject to = new TestObject("t");
        assertEquals(to, options.getConfigValue("test", to));
        assertFalse(options.getConfigValue("test", false));
    }

    @Test
    public void testNullValue2() {
        ConfigurationParameters options = ConfigurationParameters.of(Collections.singletonMap("test", null));

        assertNull(options.getConfigValue("test", null, null));
        assertNull(options.getConfigValue("test", null, TestObject.class));
        assertNull(options.getConfigValue("test", "value", null));
        assertNull(options.getConfigValue("test", "value", null));
        assertNull(options.getConfigValue("test", new TestObject("t"), null));
        assertNull(options.getConfigValue("test", false, null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPut() {
        ConfigurationParameters options = ConfigurationParameters.of();
        options.put("test", "val");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutAll() {
        ConfigurationParameters options = ConfigurationParameters.of();
        options.putAll(ImmutableMap.of("test", "val", "test2", "val2"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        ConfigurationParameters options = ConfigurationParameters.of("test", "val");
        options.remove("test");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClear() {
        ConfigurationParameters options = ConfigurationParameters.of(Collections.singletonMap("test", "val"));
        options.clear();
    }

    @Test
    public void testDurationParser() {
        assertNull(ConfigurationParameters.Milliseconds.of(""));
        assertNull(ConfigurationParameters.Milliseconds.of(null));
        assertEquals(1, ConfigurationParameters.Milliseconds.of("1").value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("1ms").value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms").value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms   ").value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms  foobar").value);
        assertEquals(1000, ConfigurationParameters.Milliseconds.of("1s").value);
        assertEquals(1500, ConfigurationParameters.Milliseconds.of("1.5s").value);
        assertEquals(1500, ConfigurationParameters.Milliseconds.of("1s 500ms").value);
        assertEquals(60 * 1000, ConfigurationParameters.Milliseconds.of("1m").value);
        assertEquals(90 * 1000, ConfigurationParameters.Milliseconds.of("1m30s").value);
        assertEquals(60 * 60 * 1000 + 90 * 1000, ConfigurationParameters.Milliseconds.of("1h1m30s").value);
        assertEquals(36 * 60 * 60 * 1000 + 60 * 60 * 1000 + 90 * 1000, ConfigurationParameters.Milliseconds.of("1.5d1h1m30s").value);
    }

    @Test
    public void testDurationParserWithDefault() {
        ConfigurationParameters.Milliseconds defValue = ConfigurationParameters.Milliseconds.FOREVER;
        assertEquals(defValue, ConfigurationParameters.Milliseconds.of("", defValue));
        assertEquals(defValue, ConfigurationParameters.Milliseconds.of(null, defValue));
        assertEquals(1, ConfigurationParameters.Milliseconds.of("1", defValue).value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("1ms", defValue).value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms", defValue).value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms   ", defValue).value);
        assertEquals(1, ConfigurationParameters.Milliseconds.of("  1ms  foobar", defValue).value);
        assertEquals(1000, ConfigurationParameters.Milliseconds.of("1s", defValue).value);
        assertEquals(1500, ConfigurationParameters.Milliseconds.of("1.5s", defValue).value);
        assertEquals(1500, ConfigurationParameters.Milliseconds.of("1s 500ms", defValue).value);
        assertEquals(60 * 1000, ConfigurationParameters.Milliseconds.of("1m", defValue).value);
        assertEquals(90 * 1000, ConfigurationParameters.Milliseconds.of("1m30s", defValue).value);
        assertEquals(60 * 60 * 1000 + 90 * 1000, ConfigurationParameters.Milliseconds.of("1h1m30s", defValue).value);
        assertEquals(36 * 60 * 60 * 1000 + 60 * 60 * 1000 + 90 * 1000, ConfigurationParameters.Milliseconds.of("1.5d1h1m30s", defValue).value);
    }

    @Test
    public void testNullMilliseconds() {
        assertSame(ConfigurationParameters.Milliseconds.NULL, ConfigurationParameters.Milliseconds.of(0));
    }

    @Test
    public void testForeverMilliseconds() {
        assertSame(ConfigurationParameters.Milliseconds.FOREVER, ConfigurationParameters.Milliseconds.of(Long.MAX_VALUE));
    }

    @Test
    public void testNeverMilliseconds() {
        assertSame(ConfigurationParameters.Milliseconds.NEVER, ConfigurationParameters.Milliseconds.of(Long.MIN_VALUE));
        assertSame(ConfigurationParameters.Milliseconds.NEVER, ConfigurationParameters.Milliseconds.of(-1));
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
            return object == this || object instanceof TestObject && name.equals(((TestObject) object).name);
        }
    }
}