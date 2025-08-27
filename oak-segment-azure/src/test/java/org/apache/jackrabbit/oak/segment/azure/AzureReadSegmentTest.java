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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
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

public class AzureReadSegmentTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;
    private BlobContainerClient noRetryBlobContainerClient;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
        noRetryBlobContainerClient = azurite.getNoRetryBlobContainerClient("oak-test");
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testReadNonExistentSegmentRepositoryReachable() throws IOException, InvalidFileStoreVersionException, BlobStorageException {
        AzurePersistence p = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, "oak");
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentId id = new SegmentId(fs, 0, 0);

        try {
            fs.readSegment(id);
        } finally {
            fs.close();
        }
    }

    @Test(expected = RepositoryNotReachableException.class)
    public void testReadExistentSegmentRepositoryNotReachable() throws IOException, InvalidFileStoreVersionException, BlobStorageException {
        AzurePersistence p = new ReadFailingAzurePersistence(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, "oak");
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

    static class ReadFailingAzurePersistence extends AzurePersistence {
        public ReadFailingAzurePersistence(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, BlobContainerClient noRetryBlobContainerClient, String rootPrefix) {
            super(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, rootPrefix);
        }

        @Override
        public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor,
                                                          FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
            return new AzureArchiveManager(readBlobContainerClient, writeBlobContainerClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController) {
                @Override
                public SegmentArchiveReader open(String archiveName) throws IOException {
                    return new AzureSegmentArchiveReader(readBlobContainerClient, rootPrefix, archiveName, ioMonitor) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(
                                    new RuntimeException("Cannot access Azure storage"));
                        }
                    };
                }

                @Override
                public SegmentArchiveWriter create(String archiveName) throws IOException {
                    return new AzureSegmentArchiveWriter(writeBlobContainerClient, rootPrefix, archiveName, ioMonitor, fileStoreMonitor, writeAccessController) {
                        @Override
                        public Buffer readSegment(long msb, long lsb) throws IOException {
                            throw new RepositoryNotReachableException(
                                    new RuntimeException("Cannot access Azure storage"));
                        }
                    };
                }
            };
        }
    }
}
