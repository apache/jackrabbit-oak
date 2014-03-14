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

package org.apache.jackrabbit.oak.commons;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertiesUtilTest {


    @SuppressWarnings({"deprecation", "UnnecessaryBoxing"})
    @Test
    public void testToDouble() {
        // we test getProperty which calls toDouble - so we can test both
        // methods in one go
        assertEquals(2.0, PropertiesUtil.toDouble(null, 2.0), 0);
        assertEquals(1.0, PropertiesUtil.toDouble(1.0, 2.0), 0);
        assertEquals(1.0, PropertiesUtil.toDouble(new Double(1.0), 2.0), 0);
        assertEquals(5.0, PropertiesUtil.toDouble(new Long(5), 2.0), 0);
        assertEquals(2.0, PropertiesUtil.toDouble("abc", 2.0), 0);
    }

    @Test
    public void testToBoolean() {
        assertEquals(true, PropertiesUtil.toBoolean(null, true));
        assertEquals(false, PropertiesUtil.toBoolean(1.0, true));
        assertEquals(false, PropertiesUtil.toBoolean(false, true));
        assertEquals(false, PropertiesUtil.toBoolean("false", true));
        assertEquals(false, PropertiesUtil.toBoolean("abc", true));
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    public void testToInteger() {
        assertEquals(2, PropertiesUtil.toInteger(null, 2));
        assertEquals(2, PropertiesUtil.toInteger(1.0, 2));
        assertEquals(2, PropertiesUtil.toInteger(new Double(1.0), 2));
        assertEquals(5, PropertiesUtil.toInteger(new Long(5), 2));
        assertEquals(5, PropertiesUtil.toInteger(new Integer(5), 2));
        assertEquals(2, PropertiesUtil.toInteger("abc", 2));
    }

    @SuppressWarnings("UnnecessaryBoxing")
    @Test
    public void testToLong() {
        assertEquals(2, PropertiesUtil.toLong(null, 2));
        assertEquals(2, PropertiesUtil.toLong(1.0, 2));
        assertEquals(2, PropertiesUtil.toLong(new Double(1.0), 2));
        assertEquals(5, PropertiesUtil.toLong(new Long(5), 2));
        assertEquals(5, PropertiesUtil.toLong(new Integer(5), 2));
        assertEquals(2, PropertiesUtil.toLong("abc", 2));
    }

    @Test
    public void testToObject() {
        assertEquals("hallo", PropertiesUtil.toObject("hallo"));
        assertEquals("1", PropertiesUtil.toObject(new String[]{"1", "2"}));
        assertEquals(null, PropertiesUtil.toObject(null));
        assertEquals(null, PropertiesUtil.toObject(new String[]{}));
        final List<String> l = new ArrayList<String>();
        assertEquals(null, PropertiesUtil.toObject(l));
        l.add("1");
        assertEquals("1", PropertiesUtil.toObject(l));
        l.add("2");
        assertEquals("1", PropertiesUtil.toObject(l));
        final Map<String, Object> m = new HashMap<String, Object>();
        assertEquals(m, PropertiesUtil.toObject(m));
    }

    @Test
    public void testToString() {
        assertEquals("hallo", PropertiesUtil.toString("hallo", null));
        assertEquals(this.toString(), PropertiesUtil.toString(null, this.toString()));
        final Map<String, Object> m = new HashMap<String, Object>();
        m.put("1", 5);
        assertEquals(m.toString(), PropertiesUtil.toString(m, this.toString()));
    }

    @Test
    public void testToStringArray() {
        final String[] defaultValue = new String[]{"1"};
        assertArrayEquals(null, PropertiesUtil.toStringArray(5));
        assertArrayEquals(null, PropertiesUtil.toStringArray(null));
        assertArrayEquals(defaultValue, PropertiesUtil.toStringArray(5, defaultValue));
        assertArrayEquals(defaultValue, PropertiesUtil.toStringArray(null, defaultValue));
        assertArrayEquals(new String[]{"hallo"}, PropertiesUtil.toStringArray("hallo", defaultValue));
        assertArrayEquals(new String[]{"hallo"}, PropertiesUtil.toStringArray(new String[]{"hallo"}, defaultValue));
        assertArrayEquals(new String[]{"hallo", "you"}, PropertiesUtil.toStringArray(new String[]{"hallo", "you"}, defaultValue));
        assertArrayEquals(new String[]{"5", "1"}, PropertiesUtil.toStringArray(new Integer[]{5, 1}, defaultValue));
        assertArrayEquals(new String[]{"5", "1"}, PropertiesUtil.toStringArray(new Integer[]{5, null, 1}, defaultValue));
        final List<String> l = new ArrayList<String>();
        assertArrayEquals(new String[]{}, PropertiesUtil.toStringArray(l, defaultValue));
        l.add("1");
        l.add("2");
        assertArrayEquals(new String[]{"1", "2"}, PropertiesUtil.toStringArray(l, defaultValue));
        l.add(null);
        assertArrayEquals(new String[]{"1", "2"}, PropertiesUtil.toStringArray(l, defaultValue));
        final Map<String, Object> m = new HashMap<String, Object>();
        m.put("1", 5);
        assertArrayEquals(defaultValue, PropertiesUtil.toStringArray(m, defaultValue));
    }

    @Test
    public void testPopulate() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("string", "foo");
        props.put("bool", "true");
        props.put("integer", "7");
        props.put("aLong", "11");

        TestBeanA bean = new TestBeanA();
        PropertiesUtil.populate(bean, props, false);

        assertEquals("foo", bean.getString());
        assertTrue(bean.getBool());
        assertEquals(7, bean.getInteger());
        assertEquals(11, bean.getaLong());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPopulateAndValidate() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("something", "foo");

        TestBeanA bean = new TestBeanA();
        PropertiesUtil.populate(bean, props, true);

    }

    private static class TestBeanA {
        private String string;
        private Boolean bool;
        private int integer;
        private long aLong;

        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        public Boolean getBool() {
            return bool;
        }

        public void setBool(Boolean bool) {
            this.bool = bool;
        }

        public int getInteger() {
            return integer;
        }

        public void setInteger(int integer) {
            this.integer = integer;
        }

        public long getaLong() {
            return aLong;
        }

        public void setaLong(long aLong) {
            this.aLong = aLong;
        }
    }

}
