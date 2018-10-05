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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Writer;
import java.util.List;
import java.util.function.Function;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This utility class allows collecting IO traces of read accesses to segments
 * caused by reading specific items.
 * <p>
 * An instance of {@link Trace} is used to specify a read pattern. Segment reads
 * are recorded in CSV format:
 <pre>
 timestamp,file,segmentId,length,elapsed
 1522147945084,data01415a.tar,f81378df-b3f8-4b25-0000-00000002c450,181328,171849
 1522147945096,data01415a.tar,f81378df-b3f8-4b25-0000-00000002c450,181328,131272
 1522147945097,data01415a.tar,f81378df-b3f8-4b25-0000-00000002c450,181328,142766
 ...
 </pre>
 * {@link Trace} implementations can specify an additional context, which is recorded
 * with each line of the CSV output. A context is simply a list of additional fields
 * as specified during instantiation of an {@code IOTracer}.
 */
public class IOTracer {
    @NotNull
    private final Function<IOMonitor, FileStore> fileStoreFactory;

    @NotNull
    private final IOTraceMonitor ioMonitor;

    private IOTracer(
            @NotNull Function<IOMonitor, FileStore> fileStoreFactory,
            @NotNull Writer output,
            @Nullable String contextSpec) {
        this.fileStoreFactory = checkNotNull(fileStoreFactory);
        ioMonitor = new IOTraceMonitor(new DefaultIOTraceWriter(output), contextSpec);
    }

    /**
     * Create a new {@code IOTracer} instance.
     * @param fileStoreFactory  A factory for creating a {@link FileStore} with the
     *                          passed {@link IOMonitor} for monitoring segment IO.
     * @param output            The target for the CSV formatted IO trace.
     * @param contextSpec       The specification of additional context provided by
     *                          the {@link Trace traces} being {@link IOTracer#collectTrace(Trace) run}.
     *                          A trace consists of a comma separated list of values, which must match
     *                          the list of values passed to {@link IOTracer#setContext(List)}.
     * @return A new {@code IOTracer} instance.
     */
    @NotNull
    public static IOTracer newIOTracer(
            @NotNull Function<IOMonitor, FileStore> fileStoreFactory,
            @NotNull Writer output,
            @Nullable String contextSpec) {
        return new IOTracer(fileStoreFactory, output, contextSpec);
    }

    /**
     * Collect a IO trace.
     * @param trace
     */
    public void collectTrace(@NotNull Trace trace) {
        checkNotNull(trace);
        try (FileStore fileStore = checkNotNull(fileStoreFactory).apply(checkNotNull(ioMonitor))) {
            trace.run(fileStore.getHead());
        } finally {
            ioMonitor.flush();
        }
    }

    /**
     * Set the {@code context} to be added to each line of the IOTrace going forward. The list
     * of values needs to match the context specification passed to
     * {@link IOTracer#newIOTracer(Function, Writer, String)}.
     * @param context
     */
    public void setContext(@NotNull List<String> context) {
        ioMonitor.setContext(context);
    }

}
