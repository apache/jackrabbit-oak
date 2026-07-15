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
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles;
import org.apache.jackrabbit.oak.segment.file.tar.TarFilesTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.junit.Before;
import org.junit.ClassRule;

public class AzureTarFilesTest extends TarFilesTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;
    private BlobContainerClient noRetryBlobContainerClient;

    @Before
    @Override
    public void setUp() throws Exception {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
        noRetryBlobContainerClient = azurite.getNoRetryBlobContainerClient("oak-test");

        AzurePersistence azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, "oak");
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        azurePersistence.setWriteAccessController(writeAccessController);
        tarFiles = TarFiles.builder()
                .withDirectory(folder.newFolder())
                .withTarRecovery((id, data, recovery) -> {
                    // Intentionally left blank
                })
                .withIOMonitor(new IOMonitorAdapter())
                .withFileStoreMonitor(new FileStoreMonitorAdapter())
                .withRemoteStoreMonitor(new RemoteStoreMonitorAdapter())
                .withMaxFileSize(MAX_FILE_SIZE)
                .withPersistence(azurePersistence)
                .build();
    }
}
