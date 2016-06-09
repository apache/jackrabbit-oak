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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.commons.io.FileUtils.write;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JournalReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testEmpty() throws IOException {
        JournalReader journalReader = createJournalReader("");
        try {
            assertFalse(journalReader.iterator().hasNext());
        } finally {
            journalReader.close();
        }
    }

    @Test
    public void testSingleton() throws IOException {
        JournalReader journalReader = createJournalReader("one 1");
        try {
            Iterator<String> journal = journalReader.iterator();
            assertTrue(journal.hasNext());
            assertEquals("one", journal.next());
            assertFalse(journal.hasNext());
        } finally {
            journalReader.close();
        }
    }

    @Test
    public void testMultiple() throws IOException {
        JournalReader journalReader = createJournalReader("one 1\ntwo 2\nthree 3 456");
        try {
            Iterator<String> journal = journalReader.iterator();
            assertTrue(journal.hasNext());
            assertEquals("three", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("two", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("one", journal.next());
            assertFalse(journal.hasNext());
        } finally {
            journalReader.close();
        }
    }

    @Test
    public void testSpaces() throws IOException {
        JournalReader journalReader = createJournalReader("\n \n  \n   ");
        try {
            Iterator<String> journal = journalReader.iterator();
            assertTrue(journal.hasNext());
            assertEquals("", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("", journal.next());
            assertFalse(journal.hasNext());
        } finally {
            journalReader.close();
        }
    }

    @Test
    public void testIgnoreInvalid() throws IOException {
        JournalReader journalReader = createJournalReader("one 1\ntwo 2\ninvalid\nthree 3");
        try {
            Iterator<String> journal = journalReader.iterator();
            assertTrue(journal.hasNext());
            assertEquals("three", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("two", journal.next());
            assertTrue(journal.hasNext());
            assertEquals("one", journal.next());
            assertFalse(journal.hasNext());
        } finally {
            journalReader.close();
        }
    }

    private JournalReader createJournalReader(String s) throws IOException {
        File journalFile = folder.newFile("jrt");
        write(journalFile, s);
        return new JournalReader(journalFile);
    }

}
