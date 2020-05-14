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
package org.apache.jackrabbit.oak.segment.aws;

import java.io.IOException;

import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsPersistence implements SegmentNodeStorePersistence {

    private static final Logger log = LoggerFactory.getLogger(AwsPersistence.class);

    protected final AwsContext awsContext;

    public AwsPersistence(AwsContext awsContext) {
        this.awsContext = awsContext;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor,
            FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        awsContext.setRemoteStoreMonitor(remoteStoreMonitor);
        return new AwsArchiveManager(awsContext.directory, ioMonitor, fileStoreMonitor);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            for (String prefix : awsContext.directory.listPrefixes()) {
                if (prefix.contains(".tar/")) {
                    return true;
                }
            }

            return false;
        } catch (IOException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AwsJournalFile(awsContext.dynamoDBClient, awsContext.getPath("journal.log"));
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new AwsGCJournalFile(awsContext.dynamoDBClient, awsContext.getPath("gc.log"));
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return new AwsManifestFile(awsContext.directory, "manifest");
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return new AwsRepositoryLock(awsContext.dynamoDBClient, awsContext.getPath("repo.lock")).lock();
    }
}
