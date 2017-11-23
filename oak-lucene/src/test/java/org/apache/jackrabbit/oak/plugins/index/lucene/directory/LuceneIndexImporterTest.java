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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.PROP_UID;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.STATUS_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.createFile;
import static org.junit.Assert.*;

public class LuceneIndexImporterTest {
    private NodeState rootState = InitialContent.INITIAL_CONTENT;
    private NodeBuilder idx = new IndexDefinitionBuilder().build().builder();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void exportAndImport() throws Exception{
        NodeState baseIndexState = idx.getNodeState();
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, baseIndexState, "/oak:index/fooIndex").build();

        LuceneIndexEditorContext.configureUniqueId(idx);

        String dirName = ":data";
        Directory dir = new OakDirectory(idx, dirName, defn, false);
        createFile(dir, "foo.txt", "Test content");
        dir.close();

        String dir2Name = ":data2" + MultiplexersLucene.INDEX_DIR_SUFFIX;
        Directory dir2 = new OakDirectory(idx, dir2Name, defn, false);
        createFile(dir2, "foo.txt", "Test content");
        dir2.close();

        NodeBuilder builder = rootState.builder();
        builder.child("oak:index").setChildNode("fooIndex", idx.getNodeState());
        NodeState indexState = builder.getNodeState();

        File out = temporaryFolder.newFolder();
        LuceneIndexDumper dumper = new LuceneIndexDumper(indexState, "/oak:index/fooIndex", out);
        dumper.dump();

        LuceneIndexImporter importer = new LuceneIndexImporter();
        NodeBuilder newIdxBuilder = indexState.builder().getChildNode("oak:index").getChildNode("fooIndex");

        //Add a file to builder to check if existing hidden nodes are removed or not
        Directory dir3 = new OakDirectory(newIdxBuilder, dirName, defn, false);
        createFile(dir3, "foo2.txt", "Test content");
        dir3.close();

        importer.importIndex(rootState, newIdxBuilder, dumper.getIndexDir());

        NodeState exportedIndexState = indexState.getChildNode("oak:index").getChildNode("fooIndex");
        NodeState importedIndexState = newIdxBuilder.getNodeState();

        assertDirectoryEquals(defn, exportedIndexState, importedIndexState, dirName);

        //The uid must be different for imported directory
        String exportedUid = exportedIndexState.getChildNode(STATUS_NODE).getString(PROP_UID);
        String importedUid = importedIndexState.getChildNode(STATUS_NODE).getString(PROP_UID);
        assertNotEquals(exportedUid, importedUid);
        assertNotNull(exportedUid);
        assertNotNull(importedUid);
    }

    private static void assertDirectoryEquals(IndexDefinition defn, NodeState expected, NodeState actual, String dirName) throws IOException {
        OakDirectory dir1 = new OakDirectory(new ReadOnlyBuilder(expected), dirName, defn, true);
        OakDirectory dir2 = new OakDirectory(new ReadOnlyBuilder(actual), dirName, defn, true);
        assertDirectoryEquals(dir1, dir2);
        dir1.close();
        dir2.close();
    }

    private static void assertDirectoryEquals(Directory expected, Directory actual) throws IOException {
        assertEquals(fileNameSet(expected), fileNameSet(actual));

        for (String fileName : expected.listAll()) {
            byte[] i1 = toBytes(expected.openInput(fileName, IOContext.DEFAULT));
            byte[] i2 = toBytes(actual.openInput(fileName, IOContext.DEFAULT));
            assertArrayEquals(i1, i2);
        }
    }

    private static Set<List<String>> fileNameSet(Directory expected) throws IOException {
        return ImmutableSet.of(Arrays.asList(expected.listAll()));
    }

    private static byte[] toBytes(IndexInput input) throws IOException {
        int length = (int) input.length();
        byte[] result = new byte[length];
        input.readBytes(result, 0, length);
        input.close();
        return result;
    }

}