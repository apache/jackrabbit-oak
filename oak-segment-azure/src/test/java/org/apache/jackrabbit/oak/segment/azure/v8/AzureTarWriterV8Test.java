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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.file.tar.TarWriterTest;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

public class AzureTarWriterV8Test extends TarWriterTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setUp() throws Exception {
        container = azurite.getContainer("oak-test");
    }

    @NotNull
    @Override
    protected SegmentArchiveManager getSegmentArchiveManager() throws Exception {
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        AzureArchiveManagerV8 azureArchiveManagerV8 = new AzureArchiveManagerV8(container.getDirectoryReference("oak"), new IOMonitorAdapter(), monitor, writeAccessController);
        return azureArchiveManagerV8;
    }

    @NotNull
    @Override
    protected SegmentArchiveManager getFailingSegmentArchiveManager() throws Exception {
        final WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        return new AzureArchiveManagerV8(container.getDirectoryReference("oak"), new IOMonitorAdapter(), monitor, writeAccessController) {
            @Override
            public SegmentArchiveWriter create(String archiveName) throws IOException {
                return new AzureSegmentArchiveWriterV8(getDirectory(archiveName), ioMonitor, monitor, writeAccessController) {
                    @Override
                    public void writeGraph(@NotNull byte[] data) throws IOException {
                        throw new IOException("test");
                    }
                };
            }
        };
    }
}
