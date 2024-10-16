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

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

public class AzureGCJournalFile implements GCJournalFile {

    private final AppendBlobClient gcJournal;

    public AzureGCJournalFile(AppendBlobClient gcJournal) {
        this.gcJournal = gcJournal;
    }

    @Override
    public void writeLine(String line) throws IOException {
        try {
            String appendLine = line + "\n";
            gcJournal.createIfNotExists();
            gcJournal.appendBlock(new ByteArrayInputStream((appendLine).getBytes()), appendLine.length());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<String> readLines() throws IOException {
        try {
            if (!gcJournal.exists()) {
                return Collections.emptyList();
            }
            byte[] data = gcJournal.downloadContent().toBytes();
            return IOUtils.readLines(new ByteArrayInputStream(data), Charset.defaultCharset());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void truncate() throws IOException {
        try {
            gcJournal.deleteIfExists();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
