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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TimeIntervalTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private long start = 10;
    private long end = 20;

    private TimeInterval ti = new TimeInterval(start, end);

    @Test
    public void constructFail() {
        thrown.expect(IllegalArgumentException.class);
        new TimeInterval(20, 10);
    }

    @Test
    public void contains() {
        assertTrue(ti.contains(start));
        assertTrue(ti.contains(end));
        assertTrue(ti.contains(start + (end - start) / 2));
        assertFalse(ti.contains(start - 1));
        assertFalse(ti.contains(end + 1));
    }

    @Test
    public void notLaterThan1() {
        TimeInterval t = ti.notLaterThan(end - 1);
        assertTrue(t.contains(end - 1));
        assertFalse(t.contains(end));
        assertTrue(t.contains(start));
    }

    @Test
    public void notLaterThan2() {
        TimeInterval t = ti.notLaterThan(start - 1);
        assertTrue(t.contains(start - 1));
        assertFalse(t.contains(start - 2));
        assertFalse(t.contains(start));
    }

    @Test
    public void notLaterThan3() {
        TimeInterval t = ti.notLaterThan(end + 1);
        assertEquals(t, ti);
    }

    @Test
    public void notEarlierThan1() {
        TimeInterval t = ti.notLaterThan(start + 1);
        assertTrue(t.contains(start + 1));
        assertTrue(t.contains(start));
        assertFalse(t.contains(start + 2));
        assertFalse(t.contains(start - 1));
    }

    @Test
    public void notEarlierThan2() {
        TimeInterval t = ti.notEarlierThan(end + 1);
        assertTrue(t.contains(end + 1));
        assertFalse(t.contains(end));
        assertFalse(t.contains(end + 2));
    }

    @Test
    public void notEarlierThan3() {
        TimeInterval t = ti.notEarlierThan(start - 1);
        assertEquals(t, ti);
    }

    @Test
    public void startAndDuration() {
        TimeInterval t = ti.startAndDuration(2);
        assertEquals(t, new TimeInterval(start, start + 2));
    }

    @Test
    public void getDuration() {
        assertEquals(end - start, ti.getDurationMs());
    }

    @Test
    public void endsAfter() {
        assertTrue(ti.endsAfter(end - 1));
        assertFalse(ti.endsAfter(end));
        assertFalse(ti.endsAfter(end + 1));
    }
}
