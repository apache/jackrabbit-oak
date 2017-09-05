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

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.TestUtil.createFile;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory.INDEX_METADATA_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LuceneIndexDumperTest {
    private NodeState rootState = InitialContent.INITIAL_CONTENT;
    private NodeBuilder idx = new IndexDefinitionBuilder().build().builder();

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void directoryDump() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx.getNodeState(), "/fooIndex").build();

        long size = 0;

        Directory dir = new OakDirectory(idx, ":data", defn, false);
        createFile(dir, "foo.txt", "Test content");
        size += DirectoryUtils.dirSize(dir);

        Directory dir2 = new OakDirectory(idx, ":data2"+ MultiplexersLucene.INDEX_DIR_SUFFIX, defn, false);
        createFile(dir2, "foo.txt", "Test content");
        size += DirectoryUtils.dirSize(dir2);

        NodeBuilder builder = rootState.builder();
        builder.setChildNode("fooIndex", idx.getNodeState());
        NodeState indexState = builder.getNodeState();

        File out = temporaryFolder.newFolder();
        LuceneIndexDumper dumper = new LuceneIndexDumper(indexState, "/fooIndex", out);
        dumper.dump();

        File indexDir = dumper.getIndexDir();
        assertNotNull(indexDir);

        assertEquals(3, indexDir.listFiles().length); // 2 dir + 1 meta
        assertEquals(dumper.getSize(), size);

        IndexMeta meta = new IndexMeta(new File(indexDir, INDEX_METADATA_FILE_NAME));
        assertNotNull(meta.getFSNameFromJCRName(":data"));
        assertNotNull(meta.getFSNameFromJCRName(":data2"+ MultiplexersLucene.INDEX_DIR_SUFFIX));
    }
}