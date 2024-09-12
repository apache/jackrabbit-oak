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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.file.tar.TarFileTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class AzureTarFileTest extends TarFileTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;

    private BlobContainerClient writeBlobContainerClient;

    @Before
    @Override
    public void setUp() throws IOException {
        try {
            readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
            writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
            AzurePersistence azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
            WriteAccessController writeAccessController = new WriteAccessController();
            writeAccessController.enableWriting();
            azurePersistence.setWriteAccessController(writeAccessController);
            archiveManager = azurePersistence.createArchiveManager(true, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected long getWriteAndReadExpectedSize() {
        return 45;
    }

    @Test
    @Ignore
    @Override
    public void graphShouldBeTrimmedDownOnSweep() throws Exception {
        super.graphShouldBeTrimmedDownOnSweep();
    }
}