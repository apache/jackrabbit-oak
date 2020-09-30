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

import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.ConfigurationParameters.Milliseconds.FOREVER;
import static org.apache.jackrabbit.oak.spi.security.ConfigurationParameters.Milliseconds.NEVER;
import static org.apache.jackrabbit.oak.spi.security.ConfigurationParameters.Milliseconds.NULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class MillisecondsTest {

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
        ConfigurationParameters.Milliseconds defValue = FOREVER;
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
        assertSame(NULL, ConfigurationParameters.Milliseconds.of(0));
    }

    @Test
    public void testForeverMilliseconds() {
        assertSame(FOREVER, ConfigurationParameters.Milliseconds.of(Long.MAX_VALUE));
    }

    @Test
    public void testNeverMilliseconds() {
        assertSame(NEVER, ConfigurationParameters.Milliseconds.of(Long.MIN_VALUE));
        assertSame(NEVER, ConfigurationParameters.Milliseconds.of(NEVER.value));
    }

    @Test
    public void testHashCode() {
        assertNotEquals(NEVER.hashCode(), FOREVER.hashCode());
        assertNotEquals(FOREVER.hashCode(), NULL.hashCode());
        assertNotEquals(ConfigurationParameters.Milliseconds.of("1ms", NULL).hashCode(), ConfigurationParameters.Milliseconds.of("1s", NULL).hashCode());
    }

    @Test
    public void testNotEquals() {
        ConfigurationParameters.Milliseconds milliseconds = ConfigurationParameters.Milliseconds.of("1ms", NULL);
        assertNotEquals(NEVER, NULL);
        assertNotEquals(NULL, milliseconds);
        assertNotEquals(milliseconds, FOREVER);
        assertNotEquals(milliseconds, ConfigurationParameters.Milliseconds.of("1h", NULL));

        assertFalse(NEVER.equals(null));
        assertFalse(NULL.equals(NULL.value));
    }

    @Test
    public void testEquals() {
        assertEquals(NEVER, ConfigurationParameters.Milliseconds.of(NEVER.value));
        assertEquals(NULL, ConfigurationParameters.Milliseconds.of(NULL.value));
        assertEquals(FOREVER, ConfigurationParameters.Milliseconds.of(FOREVER.value));

        ConfigurationParameters.Milliseconds milliseconds = ConfigurationParameters.Milliseconds.of("1ms", NULL);
        assertEquals(milliseconds, ConfigurationParameters.Milliseconds.of(milliseconds.value));
    }
}