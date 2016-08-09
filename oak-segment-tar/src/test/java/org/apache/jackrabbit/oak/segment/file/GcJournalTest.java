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

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

import org.apache.jackrabbit.oak.segment.file.GCJournal.GCJournalEntry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GcJournalTest {

    @Rule
    public final TemporaryFolder segmentFolder = new TemporaryFolder(new File(
            "target"));

    @Test
    public void tarGcJournal() throws Exception {
        File directory = segmentFolder.newFolder();
        GCJournal gc = new GCJournal(directory);

        gc.persist(100);
        GCJournalEntry e0 = gc.read();
        assertEquals(100, e0.getSize());

        gc.persist(250);
        GCJournalEntry e1 = gc.read();
        assertEquals(250, e1.getSize());

        Collection<GCJournalEntry> all = gc.readAll();
        assertEquals(all.size(), 2);

        File file = new File(directory, GCJournal.GC_JOURNAL);
        assertTrue(file.exists());
        List<String> allLines = Files.readAllLines(file.toPath(), UTF_8);
        assertEquals(allLines.size(), 2);
    }
}
