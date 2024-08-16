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
package org.apache.jackrabbit.oak.plugins.document.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.junit.Test;

public class LogSilencerTest {

    @Test
    public void testBoundaries() throws Exception {
        try {
            new LogSilencer(-1, 0);
            fail("<= 0 as cacheSize is not allowed");
        } catch(Exception e) {
            // ok
        }
        try {
            new LogSilencer(-1, -1);
            fail("<= 0 as cacheSize is not allowed");
        } catch(Exception e) {
            // ok
        }
        LogSilencer l = new LogSilencer();
        assertFalse(l.silence(null));
        assertTrue(l.silence(null));
    }

    private void assertNotSilencedWithRandomKeys(LogSilencer l) {
        for(int i = 0; i < 1000; i++) {
            final String uuid = UUID.randomUUID().toString();
            assertFalse(l.silence(uuid));
            assertTrue(l.silence(uuid));
        }
    }

    @Test
    public void testCacheSize1() throws Exception {
        final LogSilencer l = new LogSilencer(-1, 1);
        assertNotSilencedWithRandomKeys(l);
        for(int i = 0; i < 1000; i++) {
            assertFalse(l.silence("foo" + i));
            assertTrue(l.silence("foo" + i));
            assertTrue(l.silence("foo" + i));
        }
    }

    @Test
    public void testCacheSize2() throws Exception {
        final LogSilencer l = new LogSilencer(-1, 2);
        assertNotSilencedWithRandomKeys(l);
        for(int i = 0; i < 1000; i++) {
            assertFalse(l.silence("foo" + (2 * i)));
            assertFalse(l.silence("foo" + (2 * i + 1)));
            assertTrue(l.silence("foo" + (2 * i)));
            assertTrue(l.silence("foo" + (2 * i + 1)));
            assertTrue(l.silence("foo" + (2 * i)));
            assertTrue(l.silence("foo" + (2 * i + 1)));
        }
    }

    @Test
    public void testCacheSize10() throws Exception {
        final LogSilencer l = new LogSilencer(-1, 10);
        assertNotSilencedWithRandomKeys(l);
    }

    @Test
    public void testTimeout() throws Exception {
        final LogSilencer l = new LogSilencer(100, 10);
        assertFalse(l.silence("foo"));
        assertTrue(l.silence("foo"));
        Thread.sleep(150);
        assertFalse(l.silence("foo"));
        assertTrue(l.silence("foo"));
    }
}
