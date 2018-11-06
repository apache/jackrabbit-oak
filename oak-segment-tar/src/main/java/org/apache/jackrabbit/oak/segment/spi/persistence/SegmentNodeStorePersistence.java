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
package org.apache.jackrabbit.oak.segment.spi.persistence;

import java.io.IOException;

import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

/**
 * This type is a main entry point for the segment node store persistence. It's
 * used every time the access to the underlying storage (eg. tar files) is required.
 */
public interface SegmentNodeStorePersistence {

    /**
     * Opens a new archive manager. It'll be used to access the archives containing
     * segments.
     *
     * @param memoryMapping whether the memory mapping should be used (if the given
     *                      persistence supports it)
     * @param offHeapAccess whether off heap access for segments should be used
     * @param ioMonitor object used to monitor segment-related IO access. The
     *                  implementation should call the appropriate methods when
     *                  accessing segments.
     * @param fileStoreMonitor object used to monitor the general IO usage.
     * @return segment archive manager
     * @throws IOException
     */
    SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor,
            FileStoreMonitor fileStoreMonitor) throws IOException;

    /**
     * Check if the segment store already contains any segments
     * @return {@code true} is some segments are available for reading
     */
    boolean segmentFilesExist();

    /**
     * Create the {@link JournalFile}.
     * @return object representing the segment journal file
     */
    JournalFile getJournalFile();

    /**
     * Create the {@link GCJournalFile}.
     * @return object representing the GC journal file
     * @throws IOException
     */
    GCJournalFile getGCJournalFile() throws IOException;

    /**
     * Create the {@link ManifestFile}.
     * @return object representing the manifest file
     * @throws IOException
     */
    ManifestFile getManifestFile() throws IOException;

    /**
     * Acquire the lock on the repository. During the lock lifetime it shouldn't
     * be possible to acquire it again, either by a local or by a remote process.
     * <p>
     * The lock can be released manually by calling {@link RepositoryLock#unlock()}.
     * If the segment node store is shut down uncleanly (eg. the process crashes),
     * it should be released automatically, so no extra maintenance tasks are
     * required to run the process again.
     * @return the acquired repository lock
     * @throws IOException
     */
    RepositoryLock lockRepository() throws IOException;

}
