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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

public class OakDirectoryIT {
    private NodeState root = INITIAL_CONTENT;
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private FileStore store;
    private SegmentNodeStore nodeStore;
    private OakDirectory directory;
    private Random rnd = new Random();

    @Before
    public void setUpNodeStore() throws Exception {
        store = FileStore.newFileStore(tempFolder.newFolder()).withMemoryMapping(false).create();
        nodeStore = SegmentNodeStore.newSegmentNodeStore(store).create();
        IndexDefinition defn = new IndexDefinition(root, EmptyNodeState.EMPTY_NODE);
        directory = new OakDirectory(nodeStore.getRoot().builder(), defn, false);
    }

    @After
    public void closeStore(){
        store.close();
    }

//    @Ignore("OAK-3911")
    @Test
    public void largeFileSize() throws Exception{
        long expectedSize = FileUtils.ONE_GB * 3;
        long size = writeFile(expectedSize, 1097 );
        assertEquals(size, directory.fileLength("test"));
    }

    private long writeFile(long size, int buffSize) throws Exception{
        IndexOutput o = directory.createOutput("test", IOContext.DEFAULT);
        byte[] buff = new byte[buffSize];
        long length = 0;
        while (length < size){
            int byteCount = buffSize;
            rnd.nextBytes(buff);
            o.writeBytes(buff, byteCount);
            length += byteCount;
        }
        o.close();
        return length;
    }
}
