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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class LoggingHookTest {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore fileStore = new TemporaryFileStore(folder, false);

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
        .around(fileStore);

    @Test
    public void testChildNode() throws Exception {
        String result =
            "n+ child\n" +
                "n!\n" +
                "n!\n";
        assertCommitProduces(result, root -> root.setChildNode("child"));
    }

    private void assertCommitProduces(String expected, Consumer<NodeBuilder> committer) throws Exception {
        StringBuilder result = new StringBuilder();

        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore.fileStore())
            .withLoggingHook(s -> result.append(s).append("\n"))
            .build();

        NodeBuilder root = store.getRoot().builder();
        committer.accept(root);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(expected, result.toString());
    }

}
