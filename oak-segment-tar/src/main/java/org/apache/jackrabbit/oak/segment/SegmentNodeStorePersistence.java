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
package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.tar.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.IOMonitor;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public interface SegmentNodeStorePersistence {

    SegmentArchiveManager createArchiveManager(boolean memoryMapping, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) throws IOException;

    boolean segmentFilesExist();

    JournalFile getJournalFile();

    GCJournalFile getGCJournalFile() throws IOException;

    ManifestFile getManifestFile() throws IOException;

    RepositoryLock lockRepository() throws IOException;

    interface JournalFile {

        JournalFileReader openJournalReader() throws IOException;

        JournalFileWriter openJournalWriter() throws IOException;

        String getName();

        boolean exists();
    }

    interface JournalFileReader extends Closeable {

        String readLine() throws IOException;

    }

    interface JournalFileWriter extends Closeable {

        void truncate() throws IOException;

        void writeLine(String line) throws IOException;

    }

    interface GCJournalFile {

        void writeLine(String line) throws IOException;

        List<String> readLines() throws IOException;

    }

    interface ManifestFile {

        boolean exists();

        Properties load() throws IOException;

        void save(Properties properties) throws IOException;

    }

    interface RepositoryLock {

        void unlock() throws IOException;

    }
}
