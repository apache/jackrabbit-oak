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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class BranchStateTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;
    private DocumentStore store;
    private String testId;

    @Before
    public void before() {
        ns = builderProvider.newBuilder().getNodeStore();
        store = ns.getDocumentStore();
        testId = Utils.getIdFromPath("/test");
    }

    // OAK-4536
    @Test
    public void commitException() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 5; i++) {
            builder.child("test").setProperty("p", i);
            try {
                ns.merge(builder, new FailingHook(), CommitInfo.EMPTY);
                fail("must fail with " + CommitFailedException.class.getSimpleName());
            } catch (CommitFailedException e) {
                // expected
            }
            // must not create the document as part of a branch
            assertNull(store.find(Collection.NODES, testId));
        }

        for (int i = 0; i < DocumentMK.UPDATE_LIMIT * 2; i++) {
            builder.child("test").setProperty("p-" + i, i);
        }
        assertNotNull(store.find(Collection.NODES, testId));

    }

    private static final class FailingHook implements CommitHook {
        @Nonnull
        @Override
        public NodeState processCommit(NodeState before,
                                       NodeState after,
                                       CommitInfo info)
                throws CommitFailedException {
            throw new CommitFailedException(CommitFailedException.OAK, 0, "fail");
        }
    }
}
