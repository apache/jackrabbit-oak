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

package org.apache.jackrabbit.oak.spi.state;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.failed;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.initiated;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link RevisionGCMBean} based on a {@code Runnable}.
 */
public class RevisionGC implements RevisionGCMBean {
    private static final Logger log = LoggerFactory.getLogger(RevisionGC.class);

    public static final String OP_NAME = "Revision garbage collection";

    @Nonnull
    private ManagementOperation<Void> gcOp = done(OP_NAME, null);

    @Nonnull
    private final Runnable runGC;

    @Nonnull
    private final Runnable cancelGC;

    @Nonnull
    private final Supplier<String> statusMessage;

    @Nonnull
    private final Executor executor;

    /**
     * @param runGC          Revision garbage collector
     * @param cancelGC       Executor for cancelling the garbage collection task
     * @param statusMessage  an informal status message describing the status of the background
     *                       operation at the time of invocation.
     * @param executor       Executor for initiating the garbage collection task
     */
    public RevisionGC(
            @Nonnull Runnable runGC,
            @Nonnull Runnable cancelGC,
            @Nonnull Supplier<String> statusMessage,
            @Nonnull Executor executor) {
        this.runGC = checkNotNull(runGC);
        this.cancelGC = checkNotNull(cancelGC);
        this.statusMessage = checkNotNull(statusMessage);
        this.executor = checkNotNull(executor);
    }

    /**
     * @param runGC        Revision garbage collector
     * @param cancelGC     Executor for cancelling the garbage collection task
     * @param executor     Executor for initiating the garbage collection task
     */
    public RevisionGC(
            @Nonnull Runnable runGC,
            @Nonnull Runnable cancelGC,
            @Nonnull Executor executor) {
        this(runGC, cancelGC, Suppliers.ofInstance(""), executor);
    }

    @Nonnull
    @Override
    public CompositeData startRevisionGC() {
        if (gcOp.isDone()) {
            gcOp = newManagementOperation(OP_NAME, statusMessage, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    runGC.run();
                    return null;
                }
            });
            executor.execute(gcOp);
            return initiated(gcOp, OP_NAME + " started").toCompositeData();
        } else {
            return failed(OP_NAME + " already running").toCompositeData();
        }
    }

    @Nonnull
    @Override
    public CompositeData cancelRevisionGC() {
        if (!gcOp.isDone()) {
            executor.execute(newManagementOperation(OP_NAME, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    gcOp.cancel(false);
                    cancelGC.run();
                    return null;
                }
            }));
            return initiated(gcOp, "Revision garbage collection cancelled").toCompositeData();
        } else {
            return failed(OP_NAME + " not running").toCompositeData();
        }
    }

    @Nonnull
    @Override
    public CompositeData getRevisionGCStatus() {
        return gcOp.getStatus().toCompositeData();
    }
}
