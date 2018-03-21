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
package org.apache.jackrabbit.oak.segment.file.tar;

import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import static java.nio.charset.Charset.defaultCharset;

public class LocalJournalFile implements JournalFile {

    private final File journalFile;

    public LocalJournalFile(File directory, String journalFile) {
        this.journalFile = new File(directory, journalFile);
    }

    public LocalJournalFile(File journalFile) {
        this.journalFile = journalFile;
    }

    @Override
    public JournalFileReader openJournalReader() throws IOException {
        return new LocalJournalFileReader(journalFile);
    }

    @Override
    public JournalFileWriter openJournalWriter() throws IOException {
        return new LocalJournalFileWriter(journalFile);
    }

    @Override
    public String getName() {
        return journalFile.getName();
    }

    @Override
    public boolean exists() {
        return journalFile.exists();
    }

    private static class LocalJournalFileReader implements JournalFileReader {

        private final ReversedLinesFileReader journal;

        public LocalJournalFileReader(File file) throws IOException {
            journal = new ReversedLinesFileReader(file, defaultCharset());
        }

        @Override
        public String readLine() throws IOException {
            return journal.readLine();
        }

        @Override
        public void close() throws IOException {
            journal.close();
        }
    }

    private static class LocalJournalFileWriter implements JournalFileWriter {

        private final RandomAccessFile journalFile;

        public LocalJournalFileWriter(File file) throws IOException {
            journalFile = new RandomAccessFile(file, "rw");
            journalFile.seek(journalFile.length());
        }

        @Override
        public void truncate() throws IOException {
            journalFile.setLength(0);
        }

        @Override
        public void writeLine(String line) throws IOException {
            journalFile.writeBytes(line + "\n");
            journalFile.getChannel().force(false);
        }

        @Override
        public void close() throws IOException {
            journalFile.close();
        }
    }
}
