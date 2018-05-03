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
package org.apache.jackrabbit.oak.security.authorization.permission;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NumEntriesTest {

    @Test
    public void testZero() {
        assertTrue(NumEntries.ZERO.isExact);
        assertEquals(0, NumEntries.ZERO.size);
    }

    @Test
    public void testValueOfExactZero() {
        assertSame(NumEntries.ZERO, NumEntries.valueOf(0, true));
    }

    @Test
    public void testValueOfNotExactZero() {
        NumEntries ne = NumEntries.valueOf(0, false);
        assertSame(NumEntries.ZERO, ne);
    }

    @Test
    public void testValueOfExact() {
        NumEntries ne = NumEntries.valueOf(25, true);
        assertTrue(ne.isExact);
        assertEquals(25, ne.size);
    }

    @Test
    public void testValueOfNotExact() {
        NumEntries ne = NumEntries.valueOf(25, false);
        assertFalse(ne.isExact);
        assertEquals(25, ne.size);
    }

    @Test
    public void testEquals() {
        assertEquals(NumEntries.ZERO, NumEntries.ZERO);
        NumEntries ne1True = NumEntries.valueOf(1, true);
        NumEntries ne2True = NumEntries.valueOf(2, true);
        NumEntries ne1False = NumEntries.valueOf(1, false);

        assertEquals(ne1True, ne1True);
        assertNotEquals(ne1False, ne1True);
        assertNotEquals(ne2True, ne1True);

        assertFalse(ne1True.equals(null));
        assertFalse(ne1True.equals(new Object()));
    }

    @Test
    public void testHashCode() {
        assertEquals(NumEntries.ZERO.hashCode(), NumEntries.ZERO.hashCode());
        NumEntries ne1True = NumEntries.valueOf(1, true);
        NumEntries ne1False = NumEntries.valueOf(1, false);

        assertEquals(ne1True.hashCode(), ne1True.hashCode());
        assertNotEquals(ne1False.hashCode(), ne1True.hashCode());
    }
}