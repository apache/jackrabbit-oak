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
package org.apache.jackrabbit.oak.plugins.index.old.mk.simple;

import org.apache.jackrabbit.oak.plugins.index.old.mk.simple.AscendingClock;

import junit.framework.TestCase;

/**
 * A test for the class {@code AscendingClock}.
 */
public class AscendingClockTest extends TestCase {

    public void testMillis() throws InterruptedException {
        long start, last;
        last = start = System.currentTimeMillis() + 10000;
        AscendingClock c = new AscendingClock(start);
        for (int i = 0; i < 10000; i++) {
            long t = c.time();
            assertTrue(t > last);
            last = t;
        }
    }

    public void testNanos() throws InterruptedException {
        long start, last;
        last = start = System.currentTimeMillis() + 10000;
        AscendingClock c = new AscendingClock(start);
        assertTrue(c.nanoTime() > last * 1000000);
        for (int i = 0; i < 10000; i++) {
            long t = c.nanoTime();
            assertTrue(t > last);
            last = t;
        }
    }

}
