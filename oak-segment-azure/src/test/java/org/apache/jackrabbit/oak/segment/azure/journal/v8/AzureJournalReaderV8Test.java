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
package org.apache.jackrabbit.oak.segment.azure.journal.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.JournalReaderTest;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureJournalFileV8;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureJournalReaderV8Test extends JournalReaderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    protected JournalReader createJournalReader(String s) throws IOException {
        try {
            CloudAppendBlob blob = container.getAppendBlobReference("journal/journal.log.001");
            blob.createOrReplace();
            blob.appendText(s);
            return new JournalReader(new AzureJournalFileV8(container.getDirectoryReference("journal"), "journal.log", new WriteAccessController()));
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }
}
