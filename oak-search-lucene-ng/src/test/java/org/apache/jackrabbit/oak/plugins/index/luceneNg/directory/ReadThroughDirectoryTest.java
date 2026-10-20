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
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReadThroughDirectoryTest {
    @TempDir
    public Path tempDir;

    @Test
    public void copiesExistingFilesOnFirstReadAndWritesNewFilesRemotely() throws Exception {
        NodeBuilder storage = EMPTY_NODE.builder();
        OakDirectory remote = new OakDirectory(storage, "test", false);
        write(remote, "old", "original");
        remote.close();
        LuceneNgIndexDefinition definition = definition();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, Files.createTempDirectory(tempDir, "folder").toFile(), false);
             Directory writer = copier.wrapForWrite(definition, remote,
                     new OakDirectory(storage.getNodeState().builder(), "test", true), "luceneNg")) {
            File local = copier.getIndexDir(definition, definition.getIndexPath(), "luceneNg");
            assertEquals(0, copier.getDownloadCount());
            assertFalse(new File(local, "old").exists());

            assertEquals("original", read(writer, "old"));
            assertEquals(1, copier.getDownloadCount());
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
    public void overwritesFailAndDeletesAndRenamesInvalidateCachedNames() throws Exception {
        NodeBuilder storage = EMPTY_NODE.builder();
        OakDirectory remote = new OakDirectory(storage, "test", false);
        write(remote, "old", "original");
        write(remote, "deleted", "remove");
        remote.close();
        try (LuceneNgIndexCopier copier = new LuceneNgIndexCopier(Runnable::run, Files.createTempDirectory(tempDir, "folder").toFile(), false);
             Directory writer = copier.wrapForWrite(definition(), remote,
                     new OakDirectory(storage.getNodeState().builder(), "test", true), "luceneNg")) {
            assertThrows(FileAlreadyExistsException.class, () -> write(writer, "old", "replacement"));
            assertEquals("original", read(writer, "old"));
            writer.deleteFile("deleted");
            assertFalse(Arrays.asList(writer.listAll()).contains("deleted"));
            write(writer, "temp", "renamed");
            writer.deleteFile("old");
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
