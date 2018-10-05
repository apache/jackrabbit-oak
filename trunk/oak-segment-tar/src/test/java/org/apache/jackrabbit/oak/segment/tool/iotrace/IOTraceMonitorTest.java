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

package org.apache.jackrabbit.oak.segment.tool.iotrace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class IOTraceMonitorTest {

    @Test
    public void testHeader() {
        TraceWriterAssertion traceWriter = new TraceWriterAssertion();
        IOTraceMonitor ioTraceMonitor = new IOTraceMonitor(traceWriter, "foo,bar");
        traceWriter.assertHeader("timestamp,file,segmentId,length,elapsed,foo,bar");
        traceWriter.assertNotFlushed();
    }

    @Test
    public void testEntry() {
        TraceWriterAssertion traceWriter = new TraceWriterAssertion();
        IOTraceMonitor ioTraceMonitor = new IOTraceMonitor(traceWriter);
        ioTraceMonitor.afterSegmentRead(new File("foo"), 1, 2, 3, 4);
        traceWriter.assertEntry(",foo,00000000-0000-0001-0000-000000000002,3,4,");
        traceWriter.assertFlushed();
    }

    @Test
    public void testFlush() {
        TraceWriterAssertion traceWriter = new TraceWriterAssertion();
        IOTraceMonitor ioTraceMonitor = new IOTraceMonitor(traceWriter, "foo,bar");
        traceWriter.assertNotFlushed();
        ioTraceMonitor.flush();
        traceWriter.assertFlushed();
    }

    private static class TraceWriterAssertion implements IOTraceWriter {
        private String header;
        private String entry;
        private boolean flushed;

        @Override
        public void writeHeader(@NotNull String header) {
            this.header = header;
        }

        @Override
        public void writeEntry(@NotNull String entry) {
            this.entry = entry;
        }

        @Override
        public void flush() {
            this.flushed = true;
        }

        public void assertHeader(String header) {
            assertEquals(header, this.header);
        }

        public void assertEntry(String entry) {
            assertTrue(
                "\"" + this.entry + "\" should end with \"" + entry + "\"",
                this.entry.endsWith(entry));
        }

        public void assertFlushed() {
            assertTrue(flushed);
        }

        public void assertNotFlushed() {
            assertFalse(flushed);
        }
    }
}
