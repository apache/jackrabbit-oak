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

import com.azure.storage.blob.AppendBlobClient;
import com.azure.storage.blob.BlobInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;

import java.io.BufferedInputStream;
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
        if (!gcJournal.exists()) {
            gcJournal.create();
        }
        byte[] lineBytes = (line + "\n").getBytes();
        try (ByteArrayInputStream in = new ByteArrayInputStream(lineBytes); BufferedInputStream data = new BufferedInputStream(in)) {
            gcJournal.appendBlock(data, lineBytes.length);
        }

    }

    @Override
    public List<String> readLines() throws IOException {
        if (!gcJournal.exists()) {
            return Collections.emptyList();
        }
        // TODO OAK-8413: verify try()
        try (BlobInputStream input = gcJournal.openInputStream()) {
            return IOUtils.readLines(input, Charset.defaultCharset());
        }
    }

    @Override
    public void truncate() throws IOException {
        if (gcJournal.exists()) {
            gcJournal.delete();
        }
    }
}
