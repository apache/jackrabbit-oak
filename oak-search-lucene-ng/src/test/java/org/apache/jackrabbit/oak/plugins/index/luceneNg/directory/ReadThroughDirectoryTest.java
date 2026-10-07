/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReadThroughDirectoryTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void prefetchesDespiteReaderPrefetchBeingDisabled() throws Exception {
        NodeBuilder storage = EMPTY_NODE.builder();
        OakDirectory remote = new OakDirectory(storage, "test", false);
        write(remote, "old", "original");
        remote.close();
        LuceneNgIndexDefinition definition = definition();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, temporaryFolder.newFolder(), false);
             Directory writer = copier.wrapForWrite(definition, remote,
                     new OakDirectory(storage.getNodeState().builder(), "test", true), "luceneNg")) {
            assertEquals(1, copier.getDownloadCount());
            File local = copier.getIndexDir(definition, definition.getIndexPath(), "luceneNg");
            assertTrue(new File(local, "old").isFile());
            int before = copier.getReaderLocalReadCount();
            assertEquals("original", read(writer, "old"));
            assertTrue(copier.getReaderLocalReadCount() > before);
            write(writer, "new", "new content");
            assertEquals("new content", read(writer, "new"));
            assertFalse(new File(local, "new").exists());
        }
        assertTrue(storage.hasProperty("dirListing"));
    }

    @Test
    public void overwritesDeletesAndRenamesInvalidateCachedNames() throws Exception {
        NodeBuilder storage = EMPTY_NODE.builder();
        OakDirectory remote = new OakDirectory(storage, "test", false);
        write(remote, "old", "original");
        write(remote, "deleted", "remove");
        remote.close();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, temporaryFolder.newFolder(), false);
             Directory writer = copier.wrapForWrite(definition(), remote,
                     new OakDirectory(storage.getNodeState().builder(), "test", true), "luceneNg")) {
            write(writer, "old", "replacement");
            assertEquals("replacement", read(writer, "old"));
            writer.deleteFile("deleted");
            assertFalse(Arrays.asList(writer.listAll()).contains("deleted"));
            write(writer, "temp", "renamed");
            writer.rename("temp", "old");
            assertEquals("renamed", read(writer, "old"));
            try (IndexOutput temp = writer.createTempOutput("temp", "suffix", IOContext.DEFAULT)) {
                temp.writeString("temporary");
                assertTrue(Arrays.asList(writer.listAll()).contains(temp.getName()));
            }
        }
    }

    private LuceneNgIndexDefinition definition() {
        return new LuceneNgIndexDefinition.Builder().root(EMPTY_NODE).defn(EMPTY_NODE)
                .indexPath("/oak:index/test").uid("uid").build();
    }

    private void write(Directory directory, String name, String value) throws IOException {
        try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
            output.writeString(value);
        }
    }

    private String read(Directory directory, String name) throws IOException {
        try (IndexInput input = directory.openInput(name, IOContext.READ)) {
            return input.readString();
        }
    }
}
