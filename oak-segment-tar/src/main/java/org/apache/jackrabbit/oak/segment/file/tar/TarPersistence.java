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
package org.apache.jackrabbit.oak.segment.file.tar;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.file.LocalGCJournalFile;
import org.apache.jackrabbit.oak.segment.file.LocalManifestFile;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

public class TarPersistence implements SegmentNodeStorePersistence {

    private static final String LOCK_FILE_NAME = "repo.lock";

    private static final String GC_JOURNAL = "gc.log";

    private static final String MANIFEST_FILE_NAME = "manifest";

    private static final String JOURNAL_FILE_NAME = "journal.log";

    private final File directory;

    private final File journal;

    public TarPersistence(File directory) {
        this(directory, new File(directory, JOURNAL_FILE_NAME));
    }

    public TarPersistence(File directory, File journal) {
        this.directory = directory;
        this.journal = journal;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor,
            FileStoreMonitor fileStoreMonitor) {
        return new SegmentTarManager(directory, fileStoreMonitor, ioMonitor, memoryMapping, offHeapAccess);
    }

    @Override
    public boolean segmentFilesExist() {
        Collection<File> entries = FileUtils.listFiles(directory, new String[] {"tar"}, false);
        return !entries.isEmpty();
    }

    @Override
    public JournalFile getJournalFile() {
        return new LocalJournalFile(journal);
    }

    @Override
    public GCJournalFile getGCJournalFile() {
        return new LocalGCJournalFile(directory, GC_JOURNAL);
    }

    @Override
    public ManifestFile getManifestFile() {
        return new LocalManifestFile(directory, MANIFEST_FILE_NAME);
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        RandomAccessFile lockFile = new RandomAccessFile(new File(directory, LOCK_FILE_NAME), "rw");
        try {
            FileLock lock = lockFile.getChannel().lock();
            return () -> {
                lock.release();
                lockFile.close();
            };
        } catch (OverlappingFileLockException ex) {
            throw new IllegalStateException(directory.getAbsolutePath() + " is in use by another store.", ex);
        }
    }

}