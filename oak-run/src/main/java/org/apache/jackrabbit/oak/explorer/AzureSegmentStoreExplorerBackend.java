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
package org.apache.jackrabbit.oak.explorer;

import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

/**
 * Backend using a a remote Azure Segment Store.
 * <p>
 * The path must be in the form "{@code az:https://myaccount.blob.core.windows.net/container/repository}".
 * The secret key must be supplied as an environment variable {@code AZURE_SECRET_KEY}
 */
public class AzureSegmentStoreExplorerBackend extends AbstractSegmentTarExplorerBackend {
    private final String path;
    private SegmentNodeStorePersistence persistence;
    private final AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8;

    public AzureSegmentStoreExplorerBackend(String path) {
        this.path = path;
        this.azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();
    }

    @Override
    public void open() throws IOException {
        this.persistence = newSegmentNodeStorePersistence(ToolUtils.SegmentStoreType.AZURE, path, azureStorageCredentialManagerV8);

        try {
            this.store = fileStoreBuilder(Files.createTempDir())
                    .withCustomPersistence(persistence)
                    .buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        this.index = store.getTarReaderIndex();
    }

    @Override
    public void close() {
        super.close();
        azureStorageCredentialManagerV8.close();
    }

    @Override
    protected JournalFile getJournal() {
        return persistence.getJournalFile();
    }
}
