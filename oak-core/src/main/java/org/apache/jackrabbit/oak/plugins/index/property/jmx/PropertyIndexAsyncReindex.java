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

package org.apache.jackrabbit.oak.plugins.index.property.jmx;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.nanoTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.Status.formatTime;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.done;
import static org.apache.jackrabbit.oak.commons.jmx.ManagementOperation.newManagementOperation;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.oak.commons.jmx.ManagementOperation;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;

/**
 * Default implementation of {@link PropertyIndexAsyncReindexMBean} based on a
 * {@code Runnable}.
 */
public class PropertyIndexAsyncReindex implements
        PropertyIndexAsyncReindexMBean {

    public static final String OP_NAME = "Property index asynchronous reindex";

    private final AsyncIndexUpdate async;
    private final Executor executor;

    private ManagementOperation<String> arOp = done(OP_NAME, "");

    /**
     * @param async
     * @param executor
     *            executor for running the garbage collection task
     */
    public PropertyIndexAsyncReindex(@Nonnull AsyncIndexUpdate async,
            @Nonnull Executor executor) {
        this.async = checkNotNull(async);
        this.executor = checkNotNull(executor);
    }

    @Nonnull
    @Override
    public CompositeData startPropertyIndexAsyncReindex() {
        if (arOp.isDone()) {
            arOp = newManagementOperation(OP_NAME, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    long t0 = nanoTime();
                    boolean done = false;
                    while (!done) {
                        async.run();
                        done = async.isFinished();
                    }
                    return "Reindex completed in " + formatTime(nanoTime() - t0);
                }
            });
            executor.execute(arOp);
        }
        return getPropertyIndexAsyncReindexStatus();
    }

    @Nonnull
    @Override
    public CompositeData getPropertyIndexAsyncReindexStatus() {
        return arOp.getStatus().toCompositeData();
    }
}
