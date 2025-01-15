/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons.collections;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

public class CollectionUtilsTest {

    final List<String> data = Arrays.asList("one", "two", "three", null);

    @Test
    public void toArrayDequeWithNonEmptyIterable() {
        List<String> list = Arrays.asList("one", "two", "three");
        ArrayDeque<String> result = CollectionUtils.toArrayDeque(list);

        Assert.assertNotNull(result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("one", result.peekFirst());
        Assert.assertEquals("three", result.peekLast());
    }

    @Test
    public void toArrayDequeWithEmptyIterable() {
        List<String> emptyList = Collections.emptyList();
        ArrayDeque<String> result = CollectionUtils.toArrayDeque(emptyList);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void iteratorToIIteratable() {
        Iterator<String> iterator = List.of("a", "b", "c").iterator();
        iterator.next();
        Iterable<String> iterable = CollectionUtils.toIterable(iterator);
        Iterator<String> testit = iterable.iterator();
        Assert.assertEquals("b", testit.next());
        Assert.assertEquals("c", testit.next());
        Assert.assertFalse(testit.hasNext());
        try {
            testit = iterable.iterator();
            fail("should only work once");
        } catch (IllegalStateException expected) {
            // that's what we want
        }
    }

    @Test
    public void testFromProperties() {
        final Properties properties = new Properties();
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");

        final Map<String, String> result = CollectionUtils.fromProperties(properties);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
    }

    @Test
    public void testFromPropertiesWithNonStringValues() {
        final Properties properties = new Properties();
        properties.put("key1", 1.1);
        properties.setProperty("key2", "value2");

        final Map<String, String> result = CollectionUtils.fromProperties(properties);

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("1.1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
    }

    @Test
    public void testFromPropertiesAfterModifying() {
        final Properties properties = new Properties();
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");

        final Map<String, String> result = CollectionUtils.fromProperties(properties);

        // now modify the properties and verify that it doesn't affect the already created Map
        properties.setProperty("key1", "value3");

        Assert.assertEquals(2, result.size());
        Assert.assertEquals("value1", result.get("key1"));
        Assert.assertEquals("value2", result.get("key2"));
    }

    @Test
    public void testFromPropertiesEmpty() {
        final Properties properties = new Properties();

        final Map<String, String> result = CollectionUtils.fromProperties(properties);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFromPropertiesNull() {
        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.fromProperties(null));
    }

    @Test
    public void testFilterKeys() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        final Predicate<String> predicate = key -> key.startsWith("t");

        final Map<String, Integer> result = CollectionUtils.filterKeys(map, predicate);

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("two"));
        Assert.assertTrue(result.containsKey("three"));
        Assert.assertFalse(result.containsKey("one"));
    }

    @Test
    public void testFilterKeysEmptyMap() {
        final Map<String, Integer> map = new HashMap<>();
        final Predicate<String> predicate = key -> key.startsWith("t");

        final Map<String, Integer> result = CollectionUtils.filterKeys(map, predicate);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterNullKeys() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put(null, 4);

        final Predicate<String> predicate = Objects::isNull;

        final Map<String, Integer> result = CollectionUtils.filterKeys(map, predicate);

        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testFilterNonNullKeys() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put(null, 4);

        final Predicate<String> predicate = Objects::nonNull;

        final Map<String, Integer> result = CollectionUtils.filterKeys(map, predicate);

        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testFilterKeysNullMap() {
        final Predicate<String> predicate = key -> key.startsWith("t");

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterKeys(null, predicate));
    }

    @Test
    public void testFilterKeysNullPredicate() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterKeys(map, null));
    }

    @Test
    public void testFilterValues() {
        final Map<Integer, String> map = new HashMap<>();
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        final Predicate<String> predicate = value -> value.startsWith("t");

        final Map<Integer, String> result = CollectionUtils.filterValues(map, predicate);

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey(2));
        Assert.assertTrue(result.containsKey(3));
        Assert.assertFalse(result.containsKey(1));
    }

    @Test
    public void testFilterValuesEmptyMap() {
        final Map<String, String> map = new HashMap<>();
        final Predicate<String> predicate = key -> key.startsWith("t");

        final Map<String, String> result = CollectionUtils.filterValues(map, predicate);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterNullValues() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put(null, null);

        final Predicate<Integer> predicate = Objects::isNull;

        final Map<String, Integer> result = CollectionUtils.filterValues(map, predicate);

        Assert.assertEquals(1, result.size());
    }

    @Test
    public void testFilterNonNullValues() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put(null, null);

        final Predicate<Integer> predicate = Objects::nonNull;

        final Map<String, Integer> result = CollectionUtils.filterValues(map, predicate);

        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testFilterValuesNullMap() {
        final Predicate<String> predicate = key -> key.startsWith("t");

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterKeys(null, predicate));
    }

    @Test
    public void testFilterValuesNullPredicate() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterKeys(map, null));
    }

    @Test
    public void testFilterEntries() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        final Predicate<Map.Entry<String, Integer>> predicate = entry -> entry.getValue() > 1;

        final Map<String, Integer> result = CollectionUtils.filterEntries(map, predicate);

        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("two"));
        Assert.assertTrue(result.containsKey("three"));
        Assert.assertFalse(result.containsKey("one"));
    }

    @Test
    public void testFilterEntriesEmptyMap() {
        final Map<String, Integer> map = new HashMap<>();
        final Predicate<Map.Entry<String, Integer>> predicate = entry -> entry.getValue() > 1;

        final Map<String, Integer> result = CollectionUtils.filterEntries(map, predicate);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterEntriesNoMatch() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        final Predicate<Map.Entry<String, Integer>> predicate = entry -> entry.getValue() > 3;

        final Map<String, Integer> result = CollectionUtils.filterEntries(map, predicate);

        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testFilterEntriesNullMap() {
        final Predicate<Map.Entry<String, Integer>> predicate = entry -> entry.getValue() > 1;

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterEntries(null, predicate));
    }

    @Test
    public void testFilterEntriesNullPredicate() {
        final Map<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);

        Assert.assertThrows(NullPointerException.class, () -> CollectionUtils.filterEntries(map, null));
    }

    @Test
    public void ensureCapacity() {
        int capacity = CollectionUtils.ensureCapacity(8);
        Assert.assertEquals(11, capacity);
    }

    @Test
    public void ensureCapacityWithMaxValue() {
        int capacity = CollectionUtils.ensureCapacity(1073741825);
        Assert.assertEquals(1073741824, capacity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ensureCapacityWithNegativeValue() {
        int capacity = CollectionUtils.ensureCapacity(-8);
        fail("Should throw IllegalArgumentException");
    }
}