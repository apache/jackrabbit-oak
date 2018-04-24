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
 *
 */

package org.apache.jackrabbit.oak.segment.spi.monitor;

import static org.junit.Assert.assertEquals;

import java.io.File;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.junit.Test;

public class CompositeIOMonitorTest {
    private static final File FILE = new File("");

    @Test
    public void testComposition() {
        ImmutableList<IOMonitorAssertion> ioMonitors =
                ImmutableList.of(new IOMonitorAssertion(), new IOMonitorAssertion());
        IOMonitor ioMonitor = new CompositeIOMonitor(ioMonitors);

        ioMonitor.beforeSegmentRead(FILE, 0, 0, 0);
        ioMonitor.afterSegmentRead(FILE, 0, 0, 1, 0);
        ioMonitor.beforeSegmentWrite(FILE, 0, 0, 2);
        ioMonitor.afterSegmentWrite(FILE, 0, 0, 3, 0);

        ioMonitors.forEach(ioMonitorAssertion -> {
            ioMonitorAssertion.assertBeforeReadLength(0);
            ioMonitorAssertion.assertAfterReadLength(1);
            ioMonitorAssertion.assertBeforeWriteLength(2);
            ioMonitorAssertion.assertAfterWriteLength(3);
        });
    }

    @Test
    public void testUnregisterComposition() {
        ImmutableList<IOMonitorAssertion> ioMonitors =
                ImmutableList.of(new IOMonitorAssertion(), new IOMonitorAssertion());

        CompositeIOMonitor ioMonitor = new CompositeIOMonitor();
        ioMonitor.registerIOMonitor(ioMonitors.get(0));
        Registration registration = ioMonitor.registerIOMonitor(ioMonitors.get(1));

        ioMonitor.beforeSegmentRead(FILE, 0, 0, 0);
        ioMonitor.afterSegmentRead(FILE, 0, 0, 1, 0);
        ioMonitor.beforeSegmentWrite(FILE, 0, 0, 2);
        ioMonitor.afterSegmentWrite(FILE, 0, 0, 3, 0);

        ioMonitors.forEach(ioMonitorAssertion -> {
            ioMonitorAssertion.assertBeforeReadLength(0);
            ioMonitorAssertion.assertAfterReadLength(1);
            ioMonitorAssertion.assertBeforeWriteLength(2);
            ioMonitorAssertion.assertAfterWriteLength(3);
        });

        registration.unregister();

        ioMonitor.beforeSegmentRead(FILE, 0, 0, 4);
        ioMonitor.afterSegmentRead(FILE, 0, 0, 5, 0);
        ioMonitor.beforeSegmentWrite(FILE, 0, 0, 6);
        ioMonitor.afterSegmentWrite(FILE, 0, 0, 7, 0);

        ioMonitors.get(0).assertBeforeReadLength(4);
        ioMonitors.get(0).assertAfterReadLength(5);
        ioMonitors.get(0).assertBeforeWriteLength(6);
        ioMonitors.get(0).assertAfterWriteLength(7);
        ioMonitors.get(1).assertBeforeReadLength(0);
        ioMonitors.get(1).assertAfterReadLength(1);
        ioMonitors.get(1).assertBeforeWriteLength(2);
        ioMonitors.get(1).assertAfterWriteLength(3);
    }

    private static class IOMonitorAssertion implements IOMonitor {
        private int beforeReadLength = -1;
        private int afterReadLength = -1;
        private int beforeWriteLength = -1;
        private int afterWriteLength = -1;

        @Override
        public void beforeSegmentRead(File file, long msb, long lsb, int length) {
            beforeReadLength = length;
        }

        @Override
        public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
            afterReadLength = length;
        }

        @Override
        public void beforeSegmentWrite(File file, long msb, long lsb, int length) {
            beforeWriteLength = length;
        }

        @Override
        public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
            afterWriteLength = length;
        }

        public void assertBeforeReadLength(int length) {
            assertEquals(length, beforeReadLength);
        }

        public void assertAfterReadLength(int length) {
            assertEquals(length, afterReadLength);
        }

        public void assertBeforeWriteLength(int length) {
            assertEquals(length, beforeWriteLength);
        }

        public void assertAfterWriteLength(int length) {
            assertEquals(length, afterWriteLength);
        }
    }
}
