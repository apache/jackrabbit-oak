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
package org.apache.jackrabbit.oak.run.cli;

import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;

import joptsimple.OptionParser;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentTarFixtureTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void testReadWrite() throws Exception {
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(createSegmentOptions(folder.getRoot()), false)) {
            NodeStore store = fixture.getStore();
            NodeBuilder builder = store.getRoot().builder();
            builder.setChildNode("foo");
            store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadOnly()
            throws Exception {
        File directory = folder.getRoot();
        createStoreAt(directory);


        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(createSegmentOptions(folder.getRoot()), true)) {
            NodeStore s = fixture.getStore();
            NodeBuilder builder = s.getRoot().builder();
            builder.setChildNode("foo");
            s.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
    }

    @Test
    public void customizerCalled() throws Exception{
        Options o = createSegmentOptions(folder.getRoot());
        FileStoreTarBuilderCustomizer customizer = mock(FileStoreTarBuilderCustomizer.class);
        o.getWhiteboard().register(FileStoreTarBuilderCustomizer.class, customizer, emptyMap());
        try (NodeStoreFixture fixture = NodeStoreFixtureProvider.create(o, false)) {

        }

        verify(customizer, times(1)).customize(any(FileStoreBuilder.class));
    }

    private static void createStoreAt(File path) throws InvalidFileStoreVersionException, IOException {
        FileStore store = fileStoreBuilder(path).build();
        store.close();
    }

    private Options createSegmentOptions(File storePath) throws IOException {
        OptionParser parser = new OptionParser();
        Options opts = new Options().withDisableSystemExit();
        opts.parseAndConfigure(parser, new String[] {storePath.getAbsolutePath()});
        return opts;
    }
}
