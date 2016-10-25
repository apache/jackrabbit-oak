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
 */
package org.apache.jackrabbit.oak.console;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentTarFixtureTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    @Ignore("OAK-4999")  // FIXME OAK-4999
    public void testReadWrite() throws IOException, CommitFailedException {
        try (NodeStoreFixture fixture = SegmentTarFixture.create(folder.getRoot(), false, null)) {
            NodeStore store = fixture.getStore();
            NodeBuilder builder = store.getRoot().builder();
            builder.setChildNode("foo");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    @Test
    @Ignore("OAK-4998")  // FIXME OAK-4998
    public void testReadOnly()
    throws IOException, CommitFailedException, InvalidFileStoreVersionException {
        File directory = folder.getRoot();
        createStoreAt(directory);


        try (NodeStoreFixture fixture = SegmentTarFixture.create(directory, true, null)) {
            NodeStore s = fixture.getStore();
            NodeBuilder builder = s.getRoot().builder();
            builder.setChildNode("foo");
            s.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    private static void createStoreAt(File path) throws InvalidFileStoreVersionException, IOException {
        FileStore store = fileStoreBuilder(path).build();
        store.close();
    }
}
