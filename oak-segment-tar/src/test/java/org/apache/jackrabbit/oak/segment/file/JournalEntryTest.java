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

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JournalEntryTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Test
    public void timestampInJournalEntry() throws Exception{
        FileStore fileStore = fileStoreBuilder(tempFolder.getRoot())
                .withMaxFileSize(5)
                .withSegmentCacheSize(0)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withMemoryMapping(true)
                .build();

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 5; i++) {
            NodeBuilder root = nodeStore.getRoot().builder();
            root.child("c"+i);
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            fileStore.flush();
        }

        fileStore.close();

        File journal = new File(tempFolder.getRoot(), "journal.log");
        List<String> lines = Files.readLines(journal, Charset.defaultCharset());
        assertFalse(lines.isEmpty());

        String line = lines.get(0);
        List<String> parts = journalParts(line);
        assertEquals(3, parts.size());

        long entryTime = Long.valueOf(parts.get(2));
        assertTrue(entryTime >= startTime);

        JournalReader jr = new JournalReader(journal);
        JournalEntry journalEntry = jr.next();
        assertEquals(journalParts(lines.get(lines.size() - 1)).get(0), journalEntry.getRevision());
        assertEquals(journalParts(lines.get(lines.size() - 1)).get(2), String.valueOf(journalEntry.getTimestamp()));
        jr.close();
    }

    private List<String> journalParts(String line){
        return Splitter.on(' ').splitToList(line);
    }

}
