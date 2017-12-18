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

package org.apache.jackrabbit.oak.segment.file;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

/**
 * A {@link GCListener} that adds a prefix to every log message and delegates to
 * another {@link GCListener}. The message prefix includes a number, which can
 * be used as a unique counter to group together related log messages.
 */
class PrefixedGCListener implements GCListener {

    private final GCListener listener;

    private final AtomicLong counter;

    PrefixedGCListener(GCListener listener, AtomicLong counter) {
        this.listener = listener;
        this.counter = counter;
    }

    private String prefixed(String message) {
        return String.format("TarMK GC #%s: %s", counter, message);
    }

    @Override
    public void compactionSucceeded(@Nonnull GCGeneration newGeneration) {
        listener.compactionSucceeded(newGeneration);
    }

    @Override
    public void compactionFailed(@Nonnull GCGeneration failedGeneration) {
        listener.compactionFailed(failedGeneration);
    }

    @Override
    public void info(String message, Object... arguments) {
        listener.info(prefixed(message), arguments);
    }

    @Override
    public void warn(String message, Object... arguments) {
        listener.warn(prefixed(message), arguments);
    }

    @Override
    public void error(String message, Exception exception) {
        listener.error(prefixed(message), exception);
    }

    @Override
    public void skipped(String reason, Object... arguments) {
        listener.skipped(prefixed(reason), arguments);
    }

    @Override
    public void compacted() {
        listener.compacted();
    }

    @Override
    public void cleaned(long reclaimedSize, long currentSize) {
        listener.cleaned(reclaimedSize, currentSize);
    }

    @Override
    public void updateStatus(String status) {
        listener.updateStatus(status);
    }

}
