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

import com.azure.storage.blob.AppendBlobClient;
import com.azure.storage.blob.models.StorageException;
import com.google.common.base.Charsets;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.JournalReaderTest;
import org.apache.jackrabbit.oak.segment.azure.AzureJournalFile;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;

public class AzureJournalReaderTest extends JournalReaderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException, com.azure.storage.blob.models.StorageException {
        container = azurite.getContainer("oak-test");
    }

    protected JournalReader createJournalReader(String s) throws IOException {
        AppendBlobClient blob = container.getAppendBlobReference("journal/journal.log.001");
        blob.create();
        blob.appendBlock(IOUtils.toBufferedInputStream(IOUtils.toInputStream(s, StandardCharsets.UTF_8)), s.length());
        return new JournalReader(new AzureJournalFile(container.getDirectoryReference("journal"), "journal.log"));
    }
}
