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
package org.apache.jackrabbit.oak.plugins.index.old;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests the ArrayUtils class
 */
public class ArrayUtilsTest {

    @Test
    public void insertInt() {
        int[] x = {10, 20};
        int[] y = ArrayUtils.arrayInsert(x, 1, 15);
        assertFalse(x == y);
        assertEquals(3, y.length);
        assertEquals(10, y[0]);
        assertEquals(15, y[1]);
        assertEquals(20, y[2]);
    }

    @Test
    public void insertLong() {
        long[] x = {10, 20};
        long[] y = ArrayUtils.arrayInsert(x, 1, 15);
        assertFalse(x == y);
        assertEquals(3, y.length);
        assertEquals(10, y[0]);
        assertEquals(15, y[1]);
        assertEquals(20, y[2]);
    }

    @Test
    public void insertObject() {
        Long[] x = {10L, 20L};
        Long[] y = ArrayUtils.arrayInsert(x, 1, 15L);
        assertFalse(x == y);
        assertEquals(3, y.length);
        assertEquals(Long.valueOf(10), y[0]);
        assertEquals(Long.valueOf(15), y[1]);
        assertEquals(Long.valueOf(20), y[2]);
    }

    @Test
    public void insertString() {
        String[] x = {"10", "20"};
        String[] y = ArrayUtils.arrayInsert(x, 1, "15");
        assertFalse(x == y);
        assertEquals(3, y.length);
        assertEquals("10", y[0]);
        assertEquals("15", y[1]);
        assertEquals("20", y[2]);
    }

    @Test
    public void removeInt() {
        int[] x = {10, 20};
        int[] y = ArrayUtils.arrayRemove(x, 1);
        assertFalse(x == y);
        assertEquals(1, y.length);
        assertEquals(10, y[0]);
        y = ArrayUtils.arrayRemove(y, 0);
        assertEquals(0, y.length);
    }

    @Test
    public void removeLong() {
        long[] x = {10, 20};
        long[] y = ArrayUtils.arrayRemove(x, 1);
        assertFalse(x == y);
        assertEquals(1, y.length);
        assertEquals(10, y[0]);
        y = ArrayUtils.arrayRemove(y, 0);
        assertEquals(0, y.length);
    }

    @Test
    public void removeObject() {
        Long[] x = {10L, 20L};
        Long[] y = ArrayUtils.arrayRemove(x, 1);
        assertFalse(x == y);
        assertEquals(1, y.length);
        assertEquals(Long.valueOf(10), y[0]);
        y = ArrayUtils.arrayRemove(y, 0);
        assertEquals(0, y.length);
    }

    @Test
    public void removeString() {
        String[] x = {"10", "20"};
        String[] y = ArrayUtils.arrayRemove(x, 1);
        assertFalse(x == y);
        assertEquals(1, y.length);
        assertEquals("10", y[0]);
        y = ArrayUtils.arrayRemove(y, 0);
        assertEquals(0, y.length);
    }

    @Test
    public void replaceObject() {
        Long[] x = {10L, 20L};
        Long[] y = ArrayUtils.arrayReplace(x, 1, 11L);
        assertFalse(x == y);
        assertEquals(2, y.length);
        assertEquals(Long.valueOf(10), y[0]);
        assertEquals(Long.valueOf(11), y[1]);
    }

}
