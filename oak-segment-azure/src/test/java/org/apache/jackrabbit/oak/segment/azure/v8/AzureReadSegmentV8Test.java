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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureReadSegmentV8Test {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testReadNonExistentSegmentRepositoryReachable() throws URISyntaxException, IOException, InvalidFileStoreVersionException, StorageException {
        AzurePersistenceV8 p = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentId id = new SegmentId(fs, 0, 0);

        try {
            fs.readSegment(id);
        } finally {
            fs.close();
        }
    }

    @Test(expected = RepositoryNotReachableException.class)
    public void testReadExistentSegmentRepositoryNotReachable() throws URISyntaxException, IOException, InvalidFileStoreVersionException, StorageException {
        AzurePersistenceV8 p = new ReadFailingAzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();

        SegmentId id = new SegmentId(fs, 0, 0);
        byte[] buffer = new byte[2];

        try {
            fs.writeSegment(id, buffer, 0, 2);
            fs.readSegment(id);
        } finally {
            fs.close();
        }
    }

    static class ReadFailingAzurePersistenceV8 extends AzurePersistenceV8 {
        public ReadFailingAzurePersistenceV8(CloudBlobDirectory segmentStoreDirectory) {
            super(segmentStoreDirectory);
        }

        @Override
        public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor,
                FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
            return new AzureArchiveManagerV8(segmentstoreDirectory, ioMonitor, fileStoreMonitor, writeAccessController) {
                @Override
                public SegmentArchiveReader open(String archiveName) throws IOException {
                    CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
                    return new AzureSegmentArchiveReaderV8(archiveDirectory, ioMonitor) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(
                                    new RuntimeException("Cannot access Azure storage"));
                        }
                    };
                }

                @Override
                public SegmentArchiveWriter create(String archiveName) throws IOException {
                    CloudBlobDirectory archiveDirectory = getDirectory(archiveName);
                    return new AzureSegmentArchiveWriterV8(archiveDirectory, ioMonitor, fileStoreMonitor, writeAccessController) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(
                                    new RuntimeException("Cannot access Azure storage"));                        }
                    };
                }
            };
        }
    }
}
