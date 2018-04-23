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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;

/**
 * Represents the cancellation policy for the compaction phase. If the disk
 * space was considered insufficient at least once during compaction (or if the
 * space was never sufficient to begin with), compaction is considered canceled.
 * Furthermore when the file store is shutting down, compaction is considered
 * canceled. Finally the cancellation can be triggered by a timeout that can be
 * set at any time.
 */
class CancelCompactionSupplier implements Supplier<Boolean> {

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private final BooleanSupplier diskSpaceExhausted;

    private final BooleanSupplier memoryExhausted;

    private final BooleanSupplier shutDown;

    private String reason;

    private volatile long baseLine;

    private volatile long deadline;

    CancelCompactionSupplier(
        BooleanSupplier diskSpaceExhausted,
        BooleanSupplier memoryExhausted,
        BooleanSupplier shutDown
    ) {
        this.diskSpaceExhausted = diskSpaceExhausted;
        this.memoryExhausted = memoryExhausted;
        this.shutDown = shutDown;
    }

    /**
     * Set a timeout for cancellation. Setting a different timeout cancels a
     * previous one that did not yet elapse. Setting a timeout after
     * cancellation took place has no effect.
     */
    public void timeOutAfter(final long duration, @Nonnull final TimeUnit unit) {
        baseLine = currentTimeMillis();
        deadline = baseLine + MILLISECONDS.convert(duration, unit);
    }

    @Override
    public Boolean get() {
        // The outOfDiskSpace and shutdown flags can only transition from
        // false (their initial values), to true. Once true, there should
        // be no way to go back.
        if (diskSpaceExhausted.getAsBoolean()) {
            reason = "Not enough disk space";
            return true;
        }
        if (memoryExhausted.getAsBoolean()) {
            reason = "Not enough memory";
            return true;
        }
        if (shutDown.getAsBoolean()) {
            reason = "The FileStore is shutting down";
            return true;
        }
        if (cancelled.get()) {
            reason = "Cancelled by user";
            return true;
        }
        if (deadline > 0 && currentTimeMillis() > deadline) {
            long dt = SECONDS.convert(currentTimeMillis() - baseLine, MILLISECONDS);
            reason = "Timeout after " + dt + " seconds";
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return reason;
    }

    public void cancel() {
        cancelled.set(true);
    }

}
