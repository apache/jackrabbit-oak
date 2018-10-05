/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReadOnlyStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private ReadOnlyFileStore fileStore;
    private SegmentNodeStore store;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        File path = folder.getRoot();
        initStoreAt(path);
        fileStore = fileStoreBuilder(path).buildReadOnly();
        store = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    private static void initStoreAt(File path) throws InvalidFileStoreVersionException, IOException {
        FileStore store = fileStoreBuilder(path).build();
        store.close();
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void getRoot() {
        NodeState root = store.getRoot();
        assertEquals(0, root.getChildNodeCount(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void setRoot() throws CommitFailedException {
        NodeBuilder root = store.getRoot().builder();
        root.setChildNode("foo");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
