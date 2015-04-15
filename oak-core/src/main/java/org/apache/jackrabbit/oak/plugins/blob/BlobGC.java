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

package org.apache.jackrabbit.oak.plugins.blob;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.management.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.management.ManagementOperation.done;
import static org.apache.jackrabbit.oak.management.ManagementOperation.newManagementOperation;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.management.ManagementOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link BlobGCMBean} based on a {@link BlobGarbageCollector}.
 */
public class BlobGC implements BlobGCMBean {
    private static final Logger log = LoggerFactory.getLogger(BlobGC.class);

    public static final String OP_NAME = "Blob garbage collection";

    private final BlobGarbageCollector blobGarbageCollector;
    private final Executor executor;

    private ManagementOperation<String> gcOp = done(OP_NAME, "");

    /**
     * @param blobGarbageCollector  Blob garbage collector
     * @param executor              executor for running the garbage collection task
     */
    public BlobGC(
            @Nonnull BlobGarbageCollector blobGarbageCollector,
            @Nonnull Executor executor) {
        this.blobGarbageCollector = checkNotNull(blobGarbageCollector);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public CompositeData startBlobGC(final boolean markOnly) {
        if (gcOp.isDone()) {
            gcOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    blobGarbageCollector.collectGarbage(markOnly);
                    return "Blob gc completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(gcOp);
        }
        return getBlobGCStatus();
    }

    @Nonnull
    @Override
    public CompositeData getBlobGCStatus() {
        return gcOp.getStatus().toCompositeData();
    }
}
