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
package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class GCNodeWriteMonitorTest {

    private static final Logger LOG = LoggerFactory.getLogger(GCNodeWriteMonitorTest.class);

    @Test
    public void testOnNodeLoggingWithEstimatedProgress() {
        GCMonitor gcMonitor = mock(GCMonitor.class);
        long gcProgressLog = 5;
        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(gcProgressLog, gcMonitor);

        // Previous run: 1000 bytes, 100 nodes. Current run: 2000 bytes => estimated 200 nodes
        monitor.init(1000, 100, 2000);

        monitor.onProperty();
        monitor.onProperty();
        monitor.onBinary();

        for (int i = 0; i < gcProgressLog; i++) {
            monitor.onNode();
        }

        ArgumentCaptor<Object[]> argumentsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(gcMonitor, times(1)).info(
                eq("compacted {} nodes, {} properties, {} binaries in {} at {} nodes/second "
                        + "(last {} nodes: {}, {} nodes/second). {}"),
                argumentsCaptor.capture()
        );

        Object[] loggedArgs = argumentsCaptor.getValue();
        assertEquals(9, loggedArgs.length);
        assertEquals(5L, loggedArgs[0]); // nodes
        assertEquals(2L, loggedArgs[1]); // properties
        assertEquals(1L, loggedArgs[2]); // binaries
        assertTrue(loggedArgs[3] instanceof String); // elapsed formatted string
        assertEquals(5L, loggedArgs[5]); // intervalNodes ("last 5 nodes")
        assertTrue(loggedArgs[6] instanceof String); // interval duration formatted string
        assertEquals("2% complete.", loggedArgs[8]); // percentage done (5 / 200 * 100)
    }

    @Test
    public void testFinishedLogging() {
        GCMonitor gcMonitor = mock(GCMonitor.class);
        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(10, gcMonitor);

        monitor.init(1000, 100, 2000);
        monitor.onProperty();
        monitor.onProperty();
        monitor.onBinary();

        for (int i = 0; i < 7; i++) {
            monitor.onNode();
        }

        monitor.finished();

        ArgumentCaptor<Object[]> argumentsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(gcMonitor, times(1)).info(
                eq("compaction finished: compacted {} nodes, {} properties, {} binaries in {} at {} nodes/second."),
                argumentsCaptor.capture()
        );

        Object[] loggedArgs = argumentsCaptor.getValue();
        assertEquals(5, loggedArgs.length);
        assertEquals(7L, loggedArgs[0]); // nodes
        assertEquals(2L, loggedArgs[1]); // properties
        assertEquals(1L, loggedArgs[2]); // binaries
        assertTrue(loggedArgs[3] instanceof String); // elapsed formatted string

        // Calling finished() a second time should not log again
        monitor.finished();
        verify(gcMonitor, times(1)).info(
                eq("compaction finished: compacted {} nodes, {} properties, {} binaries in {} at {} nodes/second."),
                any(Object[].class)
        );
    }

    @Test
    public void testOnNodeLoggingOutputToLog() {
        // Uses LoggingGCMonitor to print the formatted log message to test output/logger
        GCMonitor loggingMonitor = new LoggingGCMonitor(LOG);
        long gcProgressLog = 10;
        GCNodeWriteMonitor monitor = new GCNodeWriteMonitor(gcProgressLog, loggingMonitor);

        monitor.init(1000, 50, 2000);

        for (int i = 1; i <= 25; i++) {
            monitor.onNode();
            if (i % 2 == 0) {
                monitor.onProperty();
            }
            if (i % 5 == 0) {
                monitor.onBinary();
            }
        }

        monitor.finished();

        assertEquals(25L, monitor.getCompactedNodes());
    }
}
