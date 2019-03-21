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

import static org.apache.jackrabbit.oak.api.Type.DATE;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class LoggingHookTest {

    private final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final TemporaryFileStore fileStore = new TemporaryFileStore(folder, false);

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
        .around(fileStore);

    @Test
    public void testChildNodeAdded() throws Exception {
        assertCommitProduces(
            lines(
                "n+ chi%25:ld",
                "n!",
                "n!"
            ),
            root -> root.setChildNode("chi%:ld")
        );
    }

    @Test
    public void testChildNodeChanged() throws Exception {
        assertCommitProduces(
            lines(
                "n^ existing",
                "n+ child",
                "n!",
                "n!",
                "n!"
            ),
            root -> root.getChildNode("existing").setChildNode("child")
        );
    }

    @Test
    public void testChildNodeDeleted() throws Exception {
        assertCommitProduces(
            lines(
                "n- existing",
                "n!"
            ),
            root -> root.getChildNode("existing").remove()
        );
    }

    @Test
    public void testChildNodesAdded() throws Exception {
        assertCommitProduces(
            lines(
                "n+ child",
                "n+ childchild",
                "n+ childchildchild",
                "n!",
                "n!",
                "n!",
                "n!"
            ),
            root ->
                root.setChildNode("child")
                    .setChildNode("childchild")
                    .setChildNode("childchildchild")
        );
    }

    @Test
    public void testNoChange() throws Exception {
        assertCommitProduces("", root -> {
            // Do nothing
        });
        assertCommitProduces(
            lines(
                "n!"
            ),
            root -> {
                root.setChildNode("a");
                root.getChildNode("a").remove();
            }
        );
    }

    @Test
    public void testPropertyAdded() throws Exception {
        assertCommitProduces(
            lines(
                "p+ a+string <STRING> = a+string/slash:colon%25percent%24dollar%5Cbackslash%0Anewline",
                "n!"
            ),
            root -> root.setProperty("a string", "a string/slash:colon%percent$dollar\\backslash\nnewline")
        );
        assertCommitProduces(
            lines(
                "p+ strings <STRINGS> = [a+string,another+string]",
                "n!"
            ),
            root -> root.setProperty("strings", Arrays.asList("a string", "another string"), STRINGS)
        );
        assertCommitProduces(
            lines(
                "p+ a+long <LONG> = 42",
                "n!"
            ),
            root -> root.setProperty("a long", 42L)
        );
        assertCommitProduces(
            lines(
                "p+ longs <LONGS> = [42,99]",
                "n!"
            ),
            root -> root.setProperty("longs", Arrays.asList(42L, 99L), LONGS)
        );
        assertCommitProduces(
            lines(
                "p+ an+int <LONG> = 42",
                "n!"
            ),
            root -> root.setProperty("an int", 42)
        );
        assertCommitProduces(
            lines(
                "p+ a+date <DATE> = Jan+02+01:00:00+CET+1970",
                "n!"
            ),
            root -> root.setProperty("a date", "Jan 02 01:00:00 CET 1970", DATE)
        );
        assertCommitProduces(
            lines(
                "p+ a+binary <BINARY> = 68656C6C6F",
                "n!"
            ),
            root -> root.setProperty("a binary", "hello".getBytes())
        );
    }

    @Test
    public void testPropertyChanged() throws Exception {
        assertCommitProduces(
            lines(
                "p+ a+string <STRING> = a+string",
                "n!"
            ),
            root -> root.setProperty("a string", "a string")
        );
        assertCommitProduces(
            lines("p^ a+string <STRING> = a+different+string",
                "n!"
            ),
            root -> root.setProperty("a string", "a different string")
        );
    }

    @Test
    public void testPropertyDeleted() throws Exception {
        assertCommitProduces(
            lines(
                "p+ a+string <STRING> = a+string",
                "n!"
            ),
            root -> root.setProperty("a string", "a string")
        );
        assertCommitProduces(
            lines(
                "p- a+string <STRING> = a+string",
                "n!"
            ),
            root -> root.removeProperty("a string")
        );
    }

    private void assertCommitProduces(String expected, Consumer<NodeBuilder> committer) throws Exception {
        StringBuilder result = new StringBuilder();

        SegmentNodeStore store = SegmentNodeStoreBuilders.builder(fileStore.fileStore())
            .withLoggingHook(s -> result.append(s).append("\n"))
            .build();

        NodeBuilder root = store.getRoot().builder();
        root.setChildNode("existing");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        result.delete(0, result.length());

        committer.accept(root);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertEquals(expected, result.toString());
    }

    private static String lines(String... lines) {
        return String.join("\n", lines) + "\n";
    }

}
