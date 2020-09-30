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
package org.apache.jackrabbit.oak.segment.spi.persistence.split;

import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SplitJournalFileTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private JournalFile roFile;

    private JournalFile rwFile;

    @Before
    public void prepareJournals() throws IOException {
        roFile = new LocalJournalFile(temporaryFolder.newFile());
        rwFile = new LocalJournalFile(temporaryFolder.newFile());

        try (JournalFileWriter writer = roFile.openJournalWriter()) {
            writer.writeLine("line 1");
            writer.writeLine("line 2");
            writer.writeLine("line 3");
            writer.writeLine("line 3a");
            writer.writeLine("line 3b");
            writer.writeLine("line 3c");
        }

        try (JournalFileWriter writer = rwFile.openJournalWriter()) {
            writer.writeLine("line 4");
            writer.writeLine("line 5");
            writer.writeLine("line 6");
        }
    }

    @Test
    public void testIgnoreNewRoLines() throws IOException {
        SplitJournalFile splitJournalFile = new SplitJournalFile(roFile, rwFile, Optional.of("line 3"));
        try (JournalFileReader reader = splitJournalFile.openJournalReader()) {
            for (int i = 6; i >= 1; i--) {
                assertEquals("line " + i, reader.readLine());
            }
            assertNull(reader.readLine());
            assertNull(reader.readLine());
        }
    }

    @Test
    public void testIgnoreWholeRoJournal() throws IOException {
        SplitJournalFile splitJournalFile = new SplitJournalFile(roFile, rwFile, Optional.empty());
        try (JournalFileReader reader = splitJournalFile.openJournalReader()) {
            assertEquals("line 6", reader.readLine());
            assertEquals("line 5", reader.readLine());
            assertEquals("line 4", reader.readLine());
            assertNull(reader.readLine());
            assertNull(reader.readLine());
        }
    }
}
