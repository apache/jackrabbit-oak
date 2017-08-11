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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;

/**
 * Listener receiving notifications about the garbage collection process
 */
interface GCListener extends GCMonitor {

    /**
     * Notification of a successfully completed compaction resulting in
     * a new generation of segments
     * @param newGeneration  the new generation number
     */
    void compactionSucceeded(@Nonnull GCGeneration newGeneration);

    /**
     * Notification of a failed compaction. A new generation of
     * segments could not be created.
     * @param failedGeneration  the generation number that could not be created
     */
    void compactionFailed(@Nonnull GCGeneration failedGeneration);
}
