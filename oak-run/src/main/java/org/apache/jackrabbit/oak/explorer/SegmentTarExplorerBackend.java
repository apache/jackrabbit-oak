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

package org.apache.jackrabbit.oak.explorer;

import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;

import java.io.File;
import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

/**
 * Backend using a local SegmentTar Node store.
 */
class SegmentTarExplorerBackend extends AbstractSegmentTarExplorerBackend  {

    private final File path;

    SegmentTarExplorerBackend(String path) throws IOException {
        this.path = new File(path);
    }

    @Override
    public void open() throws IOException {
        try {
            store = fileStoreBuilder(path).buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        index = store.getTarReaderIndex();
    }

    @Override
    protected JournalFile getJournal() {
        return new LocalJournalFile(path, "journal.log");
    }
}
