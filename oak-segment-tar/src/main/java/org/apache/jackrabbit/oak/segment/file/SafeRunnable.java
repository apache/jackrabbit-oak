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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Thread.currentThread;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code Runnable} implementation that is safe to submit to an executor or
 * {@link Scheduler}.
 * <p>
 * When this implementation's {@link #run()} method is invoked, it will set the
 * name of the current thread to the name passed to {@link SafeRunnable}, run
 * the wrapped runnable and finally restore the initial thread name. When the
 * wrapped runnable throws any unhandled exception, this exception is logged at
 * error level and the exception is re-thrown.
 */
class SafeRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SafeRunnable.class);

    @Nonnull
    private final String name;

    @Nonnull
    private final Runnable runnable;

    /**
     * New instance with the given {@code name} wrapping the passed {@code
     * runnable}.
     *
     * @param name     The name of the background operation.
     * @param runnable The background operation.
     */
    SafeRunnable(@Nonnull String name, @Nonnull Runnable runnable) {
        this.name = checkNotNull(name);
        this.runnable = checkNotNull(runnable);
    }

    @Override
    public void run() {
        try {
            String n = currentThread().getName();
            currentThread().setName(name);
            try {
                runnable.run();
            } finally {
                currentThread().setName(n);
            }
        } catch (Throwable e) {
            log.error(String.format("Uncaught exception in %s", name), e);
        }
    }

}
