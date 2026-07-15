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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class TimeUuidTest {

    @Test
    public void stringRepresentation() {
        TimeUuid u = new TimeUuid(112989318798340096L, -5844840652695650548L);
        assertEquals("2024-08-19T15:09:41.859Z 000 aee2f464c7503f0c", u.toHumanReadableString());

        assertEquals("01916b2fd263000aee2f464c7503f0c", u.toShortString());
        assertEquals(0x1916b2fd263L, u.getTimestampPart());
        assertEquals(0L, u.getCounterPart());
        assertEquals(0xaee2f464c7503f0cL, u.getRandomPart());
    }

    @Test
    public void convert() {
        Random r = new Random(1);
        for (int i = 0; i < 1000; i++) {
            long msb = r.nextLong();
            long lsb = r.nextLong();
            TimeUuid a = TimeUuid.newUuid(msb, lsb);
            TimeUuid b = TimeUuid.newUuid(msb, lsb);
            assertEquals(a.toString(), b.toString());
        }
    }

    @Test
    public void compare() {
        Random r = new Random(1);
        TimeUuid lastY = TimeUuid.newUuid(0, 0);
        for (int i = 0; i < 1000; i++) {
            long a = r.nextBoolean() ? r.nextInt(10) : r.nextLong();
            long b = r.nextBoolean() ? r.nextInt(10) : r.nextLong();
            TimeUuid y = TimeUuid.newUuid(a, b);
            assertEquals((int) Math.signum(y.compareTo(lastY)),
                    (int) Math.signum(y.toString().compareTo(lastY.toString())));
            if (y.compareTo(lastY) == 0) {
                assertEquals(y.hashCode(), lastY.hashCode());
                assertTrue(y.equals(lastY));
            } else {
                assertFalse(y.equals(lastY));
            }
            lastY = y;
        }
    }

    @Test
    public void versionAndVariant() {
        TimeUuid x = TimeUuid.newUuid();
        UUID y = new UUID(x.getMostSignificantBits(), x.getLeastSignificantBits());
        assertEquals(7, y.version());
        assertEquals(2, y.variant());
    }

    @Test
    public void incremental() {
        TimeUuid last = TimeUuid.newUuid();
        for (int i = 0; i < 1000; i++) {
            TimeUuid x = TimeUuid.newUuid();
            assertTrue(x.compareTo(last) >= 0);
            last = x;
        }
    }

    @Test
    public void getMillisIncreasing() {
        AtomicLong lastMillis = new AtomicLong();
        assertEquals((10 << 12) + 0, TimeUuid.getMillisAndCountIncreasing(10, lastMillis));
        assertEquals((10 << 12) + 0, lastMillis.get());
        assertEquals((10 << 12) + 1, TimeUuid.getMillisAndCountIncreasing(9, lastMillis));
        assertEquals((10 << 12) + 1, lastMillis.get());
        assertEquals((11 << 12) + 0, TimeUuid.getMillisAndCountIncreasing(11, lastMillis));
        assertEquals((11 << 12) + 0, lastMillis.get());
    }

    @Test
    public void fastGeneration() {
        int size = 1 << 14;
        ArrayList<TimeUuid> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(TimeUuid.newUuid());
        }
        for (int i = 1; i < size; i++) {
            TimeUuid a = list.get(i - 1);
            TimeUuid b = list.get(i);
            assertFalse(a.equals(b));
            assertTrue(a.compareTo(b) < 0);
        }
    }

}
