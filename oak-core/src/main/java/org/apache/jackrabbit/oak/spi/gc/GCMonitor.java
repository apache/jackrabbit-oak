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

import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;

/**
 * {@code GCMonitor} instance are used to monitor garbage collection.
 * Instances of {@code GCMonitor} are registered to the {@link Whiteboard}
 * to receive notifications regarding garbage collection.
 */
public interface GCMonitor {

    GCMonitor EMPTY = new Empty();

    /**
     * Informal notification on the progress of garbage collection.
     * @param message  The message with {} place holders for the {@code arguments}
     * @param arguments
     */
    void info(String message, Object... arguments);

    /**
     * Warning about a condition that might have advert effects on the overall
     * garbage collection process but does not prevent the process from running.
     * @param message  The message with {} place holders for the {@code arguments}
     * @param arguments
     */
    void warn(String message, Object... arguments);

    /**
     * An error caused the garbage collection process to terminate prematurely.
     * @param message
     * @param exception
     */
    void error(String message, Exception exception);

    /**
     * A garbage collection cycle is skipped for a specific {@code reason}.
     * @param reason  The reason with {} place holders for the {@code arguments}
     * @param arguments
     */
    void skipped(String reason, Object... arguments);

    /**
     * The compaction phase of the garbage collection process terminated successfully.
     */
    void compacted();

    /**
     * The cleanup phase of the garbage collection process terminated successfully.
     * @param reclaimedSize  number of bytes reclaimed
     * @param currentSize    number of bytes after garbage collection
     */
    void cleaned(long reclaimedSize, long currentSize);
    
    /**
     * The garbage collection entered a new phase e.g. idle, estimation, etc.
     * @param status short summary of the GC phase
     */
    void updateStatus(String status);

    class Empty implements GCMonitor {
        @Override public void info(String message, Object[] arguments) { }
        @Override public void warn(String message, Object[] arguments) { }
        @Override public void error(String message, Exception e) { }
        @Override public void skipped(String reason, Object[] arguments) { }
        @Override public void compacted() { }
        @Override public void cleaned(long reclaimedSize, long currentSize) { }
        @Override public void updateStatus(String status) { }
    }
}
