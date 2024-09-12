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
import org.apache.jackrabbit.oak.segment.file.tar.TarWriterTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

public class AzureTarWriterTest extends TarWriterTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;

    @Before
    public void setUp() throws Exception {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
    }

    @NotNull
    @Override
    protected SegmentArchiveManager getSegmentArchiveManager() throws Exception {
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(readBlobContainerClient, writeBlobContainerClient, "oak", new IOMonitorAdapter(), monitor, writeAccessController);
        return azureArchiveManager;
    }

    @NotNull
    @Override
    protected SegmentArchiveManager getFailingSegmentArchiveManager() throws Exception {
        final WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        return new AzureArchiveManager(readBlobContainerClient, writeBlobContainerClient, "oak", new IOMonitorAdapter(), monitor, writeAccessController) {
            @Override
            public SegmentArchiveWriter create(String archiveName) throws IOException {
                return new AzureSegmentArchiveWriter(writeBlobContainerClient, "oak", archiveName, ioMonitor, monitor, writeAccessController) {
                    @Override
                    public void writeGraph(@NotNull byte[] data) throws IOException {
                        throw new IOException("test");
                    }
                };
            }
        };
    }
}
