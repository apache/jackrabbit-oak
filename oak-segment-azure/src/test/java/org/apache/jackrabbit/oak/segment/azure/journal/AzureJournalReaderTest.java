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
package org.apache.jackrabbit.oak.segment.azure.journal;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import org.apache.jackrabbit.oak.segment.azure.AzureJournalFile;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.JournalReaderTest;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

public class AzureJournalReaderTest extends JournalReaderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
    }

    protected JournalReader createJournalReader(String s) throws IOException {
        try {
            AppendBlobClient blob = writeBlobContainerClient.getBlobClient("journal/journal.log.001").getAppendBlobClient();
            blob.createIfNotExists();
            if (!s.equals("")){
                blob.appendBlock(new ByteArrayInputStream(s.getBytes()), s.length());
            }

            return new JournalReader(new AzureJournalFile(readBlobContainerClient, writeBlobContainerClient, "journal", new WriteAccessController()));
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
