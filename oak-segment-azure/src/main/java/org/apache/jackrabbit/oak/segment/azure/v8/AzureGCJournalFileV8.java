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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class AzureGCJournalFileV8 implements GCJournalFile {

    private final CloudAppendBlob gcJournal;

    public AzureGCJournalFileV8(CloudAppendBlob gcJournal) {
        this.gcJournal = gcJournal;
    }

    @Override
    public void writeLine(String line) throws IOException {
        try {
            if (!gcJournal.exists()) {
                gcJournal.createOrReplace();
            }
            gcJournal.appendText(line + "\n", StandardCharsets.UTF_8.name(), null, null, null);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<String> readLines() throws IOException {
        try {
            if (!gcJournal.exists()) {
                return Collections.emptyList();
            }
            byte[] data = new byte[(int) gcJournal.getProperties().getLength()];
            gcJournal.downloadToByteArray(data, 0);
            return IOUtils.readLines(new ByteArrayInputStream(data), Charset.defaultCharset());
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void truncate() throws IOException {
        try {
            if (gcJournal.exists()) {
                gcJournal.delete();
            }
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
