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

package org.apache.jackrabbit.oak.spi.gc;

import org.slf4j.Logger;

/**
 * This {@code GCMonitor} implementation logs all calls to its
 * {@link #info(String, Object...)}, {@link #warn(String, Object...)},
 * {@link #error(String, Exception)} and {@link #skipped(String, Object...)}
 * methods at the respective levels using the logger instance passed to the
 * constructor.
 */
public class LoggingGCMonitor implements GCMonitor {
    private final Logger log;

    /**
     * New instance logging to {@code log}
     * @param log
     */
    public LoggingGCMonitor(Logger log) {
        this.log = log;
    }

    @Override
    public void info(String message, Object... arguments) {
        log.info(message, arguments);
    }

    @Override
    public void warn(String message, Object... arguments) {
        log.warn(message, arguments);
    }

    @Override
    public void error(String message, Exception exception) {
        log.error(message, exception);
    }

    @Override
    public void skipped(String reason, Object... arguments) {
        log.info(reason, arguments);
    }

    @Override
    public void compacted() {
    }

    @Override
    public void cleaned(long reclaimedSize, long currentSize) {
    }
    
    @Override
    public void updateStatus(String status) {
        
    }
}
