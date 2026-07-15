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

package org.apache.jackrabbit.oak.segment.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class RecoverJournalTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void successfulRecovery() throws Exception {
        try (FileStore fileStore = FileStoreBuilder.fileStoreBuilder(folder.getRoot()).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // Assumption: a repository in the real world always has (or had at
            // least once in the past) a checkpoint. This means that the
            // super-root always has a `checkpoints` child node, which enables
            // the heuristic in `RecoverJournal` to recognize the head states we
            // are about to create as super-roots.

            nodeStore.checkpoint(Long.MAX_VALUE);

            for (int i = 0; i < 3; i++) {
                NodeBuilder root = nodeStore.getRoot().builder();
                root.setProperty("id", i);
                nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                fileStore.flush();
            }
        }

        String originalDigest = digest(file("journal.log"));
        Set<String> originalRevisions = revisions(file("journal.log"));

        int code = RecoverJournal.builder()
            .withPath(folder.getRoot())
            .withOut(new PrintStream(new NullOutputStream()))
            .withErr(new PrintStream(new NullOutputStream()))
            .build()
            .run();
        assertEquals(0, code);

        String backupDigest = digest(file("journal.log.bak.000"));
        assertEquals(originalDigest, backupDigest);

        Set<String> recoveredRevisions = revisions(file("journal.log"));
        assertTrue(recoveredRevisions.containsAll(originalRevisions));
    }

    private File file(String name) {
        return new File(folder.getRoot(), name);
    }

    private static Set<String> revisions(File journal) throws Exception {
        Set<String> revisions = new HashSet<>();
        try (JournalReader reader = new JournalReader(new LocalJournalFile(journal))) {
            while (reader.hasNext()) {
                revisions.add(reader.next().getRevision());
            }
        }
        return revisions;
    }

    private static String digest(File file) throws Exception {
        try (InputStream stream = new FileInputStream(file)) {
            return DigestUtils.md5Hex(stream);
        }
    }

}
