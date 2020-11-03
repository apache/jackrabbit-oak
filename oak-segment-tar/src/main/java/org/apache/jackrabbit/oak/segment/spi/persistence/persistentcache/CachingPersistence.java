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
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

import java.io.IOException;

public class CachingPersistence implements SegmentNodeStorePersistence {

    private final SegmentNodeStorePersistence delegate;

    private final PersistentCache persistentCache;

    public CachingPersistence(PersistentCache persistentCache, SegmentNodeStorePersistence delegate) {
        this.delegate = delegate;
        this.persistentCache = persistentCache;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor,
            FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) throws IOException {
        return  new CachingArchiveManager(persistentCache, delegate.createArchiveManager(memoryMapping, offHeapAccess, ioMonitor, fileStoreMonitor, remoteStoreMonitor));
    }

    @Override
    public boolean segmentFilesExist() {
        return delegate.segmentFilesExist();
    }

    @Override
    public JournalFile getJournalFile() {
        return delegate.getJournalFile();
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return delegate.getGCJournalFile();
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return delegate.getManifestFile();
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return delegate.lockRepository();
    }

}