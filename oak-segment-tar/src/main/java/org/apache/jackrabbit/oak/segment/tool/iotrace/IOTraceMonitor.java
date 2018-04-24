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

import static com.google.common.collect.Queues.newConcurrentLinkedQueue;
import static java.lang.String.join;

import java.io.File;
import java.io.Flushable;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;

/**
 * This implementation of a {@link IOMonitor} logs all io reads to an
 * underlying {@link IOTraceWriter}.
 */
public class IOTraceMonitor extends IOMonitorAdapter implements Flushable {
    @Nonnull
    private final AtomicReference<List<String>> context =
            new AtomicReference<>(ImmutableList.of());

    @Nonnull
    private final IOTraceWriter traceWriter;

    @Nonnull
    private final Lock ioLock = new ReentrantLock();

    @Nonnull
    private final ConcurrentLinkedQueue<IOEvent> ioEvents = newConcurrentLinkedQueue();

    /**
     * Create a new instance writing to {@code traceWriter} with additional context fields.
     * @param traceWriter   the {@code IOTraceWriter}
     * @param contextSpec   additional context fields. A comma separated string.
     */
    public IOTraceMonitor(@Nonnull IOTraceWriter traceWriter, @CheckForNull String contextSpec) {
        this.traceWriter = traceWriter;
        traceWriter.writeHeader(IOEvent.getFields(contextSpec));
    }

    /**
     * Create a new instance writing to {@code traceWriter} additional context fields context.
     * @param traceWriter   the {@code IOTraceWriter}
     */
    public IOTraceMonitor(@Nonnull IOTraceWriter traceWriter) {
        this(traceWriter, null);
    }

    /**
     * Set the current context.
     * @param context  a list of strings corresponding to the fields passed to the
     *                 {@code contextSpec} argument in the constructor.
     */
    public void setContext(@Nonnull List<String> context) {
        this.context.set(context);
    }

    @Override
    public void afterSegmentRead(@Nonnull File file, long msb, long lsb, int length,
                                 long elapsed) {
        ioEvents.add(new IOEvent(
                file.getName(), msb, lsb, length, elapsed, context.get()));
        if (ioLock.tryLock()) {
            try {
                flush();
            } finally {
                ioLock.unlock();
            }
        }
    }

    @Override
    public void flush() {
        ioLock.lock();
        try {
            while (!ioEvents.isEmpty()) {
                traceWriter.writeEntry(Objects.toString(ioEvents.poll()));
            }
            traceWriter.flush();
        } finally {
            ioLock.unlock();
        }
    }

    private static class IOEvent {
        private static final String FIELDS = "timestamp,file,segmentId,length,elapsed";

        @Nonnull
        private final String fileName;
        private final long msb;
        private final long lsb;
        private final int length;
        private final long elapsed;
        @Nonnull
        private final List<String> context;

        private IOEvent(
                @Nonnull String fileName,
                long msb,
                long lsb,
                int length,
                long elapsed,
                @Nonnull List<String> context) {
            this.fileName = fileName;
            this.msb = msb;
            this.lsb = lsb;
            this.length = length;
            this.elapsed = elapsed;
            this.context = context;
        }

        public static String getFields(@CheckForNull String contextSpec) {
            if (contextSpec == null || contextSpec.isEmpty()) {
                return FIELDS;
            } else {
                return FIELDS + "," + contextSpec;
            }
        }

        @Override
        public String toString() {
            return System.currentTimeMillis() + "," + fileName + "," +
                    new UUID(msb, lsb) + "," + length + "," + elapsed + "," +
                    join(",", context);
        }
    }
}
