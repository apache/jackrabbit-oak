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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;

// FIXME OAK-4618: Align GCMonitorMBean MBean with new generation based GC
/**
 * MBean for monitoring the revision garbage collection process of the
 * {@link FileStore}.
 */
public interface GCMonitorMBean {
    String TYPE = "GC Monitor";

    /**
     * @return  timestamp of the last compaction or {@code null} if none.
     */
    @CheckForNull
    String getLastCompaction();

    /**
     * @return  timestamp of the last cleanup or {@code null} if none.
     */
    @CheckForNull
    String getLastCleanup();

    /**
     * @return  repository size after the last cleanup.
     */
    long getLastRepositorySize();

    /**
     * @return  reclaimed size during the last cleanup.
     */
    long getLastReclaimedSize();

    /**
     * @return  last error or {@code null} if none.
     */
    @CheckForNull
    String getLastError();

    /**
     * @return  current status.
     */
    @Nonnull
    String getStatus();

    /**
     * @return  time series of the repository size
     */
    @Nonnull
    CompositeData getRepositorySize();

    /**
     * @return  time series of the reclaimed space
     */
    @Nonnull
    CompositeData getReclaimedSize();
}
