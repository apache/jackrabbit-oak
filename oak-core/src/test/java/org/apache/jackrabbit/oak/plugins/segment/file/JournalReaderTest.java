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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.io.ByteStreams.newDataInput;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.IOException;
import java.util.LinkedList;

import org.junit.Test;

public class JournalReaderTest {

    @Test
    public void testEmpty() throws IOException {
        assertTrue(JournalReader.heads(getJournal("")).isEmpty());
    }

    @Test
    public void testSingleton() throws IOException {
        String journal = "one 1";
        LinkedList<String> heads = JournalReader.heads(getJournal(journal));
        assertEquals(1, heads.size());
        assertEquals("one", heads.get(0));
    }

    @Test
    public void testMultiple() throws IOException {
        String journal = "one 1\ntwo 2\nthree 3";
        LinkedList<String> heads = JournalReader.heads(getJournal(journal));
        assertEquals(3, heads.size());
        assertEquals("one", heads.get(0));
        assertEquals("two", heads.get(1));
        assertEquals("three", heads.get(2));
    }

    @Test
    public void testSpaces() throws IOException {
        String journal = "\n \n  \n   ";
        LinkedList<String> heads = JournalReader.heads(getJournal(journal));
        assertEquals(3, heads.size());
        assertEquals("", heads.get(0));
        assertEquals("", heads.get(1));
        assertEquals("", heads.get(2));
    }

    @Test
    public void testIgnoreInvalid() throws IOException {
        String journal = "one 1\ntwo 2\ninvalid\nthree 3";
        LinkedList<String> heads = JournalReader.heads(getJournal(journal));
        assertEquals(3, heads.size());
        assertEquals("one", heads.get(0));
        assertEquals("two", heads.get(1));
        assertEquals("three", heads.get(2));
    }

    private static DataInput getJournal(String s) {
        return newDataInput(s.getBytes());
    }

}
