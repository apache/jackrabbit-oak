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
package org.apache.jackrabbit.oak.segment.azure.tar;

import org.apache.jackrabbit.oak.segment.azure.AzureHybridArchiveManager;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.tar.TarWriterTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;

import static org.apache.jackrabbit.oak.segment.azure.AzurePersistence.WriteMode.TAR_FILE;

public class AzureSegmentTarWriterTest extends TarWriterTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Override
    protected @NotNull SegmentArchiveManager getSegmentArchiveManager() throws Exception {
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        return new AzureHybridArchiveManager(
                TAR_FILE,
                azurite.getReadBlobContainerClient("oak-test"),
                azurite.getWriteBlobContainerClient("oak-test"),
                "oak",
                new IOMonitorAdapter(),
                monitor,
                writeAccessController);
    }
}
