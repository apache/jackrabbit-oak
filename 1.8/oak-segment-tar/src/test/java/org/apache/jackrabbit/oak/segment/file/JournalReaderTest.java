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

import com.google.common.collect.Iterators;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JournalReaderTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testEmpty() throws IOException {
        try (JournalReader journalReader = createJournalReader("")) {
            assertFalse(journalReader.hasNext());
        }
    }

    @Test
    public void testSingleton() throws IOException {
        try (JournalReader journalReader = createJournalReader("one 1 123")) {
            assertTrue(journalReader.hasNext());
            JournalEntry entry = journalReader.next();
            assertEquals("one", entry.getRevision());
            assertEquals("123", String.valueOf(entry.getTimestamp()));
            assertFalse(journalReader.hasNext());
        }
    }
    
    @Test
    public void testSingletonMalformedTimestamp() throws IOException {
        try (JournalReader journalReader = createJournalReader("one 1 123a")) {
            assertTrue(journalReader.hasNext());
            JournalEntry entry = journalReader.next();
            assertEquals("one", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            assertFalse(journalReader.hasNext());
        }
    }

    @Test
    public void testMultiple() throws IOException {
        try (JournalReader journalReader = createJournalReader("one 1\ntwo 2\nthree 3 456")) {
            assertTrue(journalReader.hasNext());
            
            JournalEntry entry = journalReader.next();
            assertEquals("three", entry.getRevision());
            assertEquals("456", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("two", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("one", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            assertFalse(journalReader.hasNext());
        }
    }

    @Test
    public void testSpaces() throws IOException {
        try (JournalReader journalReader = createJournalReader("\n \n  \n   ")) {
            assertTrue(journalReader.hasNext());
            
            JournalEntry entry = journalReader.next();
            assertEquals("", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            assertFalse(journalReader.hasNext());
        }
    }

    @Test
    public void testIgnoreInvalid() throws IOException {
        try (JournalReader journalReader = createJournalReader("one 1\ntwo 2\ninvalid\nthree 3 123")) {
            assertTrue(journalReader.hasNext());
            
            JournalEntry entry = journalReader.next();
            assertEquals("three", entry.getRevision());
            assertEquals("123", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("two", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            
            assertTrue(journalReader.hasNext());
            entry = journalReader.next();
            assertEquals("one", entry.getRevision());
            assertEquals("-1", String.valueOf(entry.getTimestamp()));
            assertFalse(journalReader.hasNext());
        }
    }

    @Test
    public void testIterable() throws IOException {
        try (JournalReader journalReader = createJournalReader("one 1\ntwo 2\ninvalid\nthree 3 123")) {
            assertTrue(Iterators.contains(journalReader, new JournalEntry("three", 123L)));
            assertTrue(Iterators.contains(journalReader, new JournalEntry("two", -1L)));
            assertTrue(Iterators.contains(journalReader, new JournalEntry("one", -1L)));
        }
    }

    private JournalReader createJournalReader(String s) throws IOException {
        File journalFile = folder.newFile("jrt");
        write(journalFile, s);
        return new JournalReader(journalFile);
    }

}
