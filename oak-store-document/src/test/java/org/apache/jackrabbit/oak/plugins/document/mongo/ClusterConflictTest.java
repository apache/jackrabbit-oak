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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import javax.annotation.Nonnull;

import com.mongodb.DB;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.AbstractMongoConnectionTest;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterConflictTest extends AbstractMongoConnectionTest {

    private DocumentNodeStore ns2;

    @Override
    public void setUpConnection() throws Exception {
        super.setUpConnection();
        ns2 = newBuilder(connectionFactory.getConnection().getDB()).setClusterId(2).getNodeStore();
    }

    @Override
    protected DocumentMK.Builder newBuilder(DB db) throws Exception {
        return super.newBuilder(db).setAsyncDelay(0).setLeaseCheck(false);
    }

    @Override
    public void tearDownConnection() throws Exception {
        ns2.dispose();
        super.tearDownConnection();
    }

    // OAK-3433
    @Test
    public void mergeRetryWhileBackgroundRead() throws Exception {
        DocumentNodeStore ns1 = mk.getNodeStore();
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("a").child("b").child("c").child("foo");
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        NodeBuilder b2 = ns2.getRoot().builder();
        // force cache fill
        assertNodeExists(b2, "/a/b/c/foo");

        // remove /a/b/c on ns1
        b1 = ns1.getRoot().builder();
        b1.child("a").child("b").child("c").remove();
        merge(ns1, b1);

        // perform some change on ns2
        b2.child("z");
        merge(ns2, b2);
        runBackgroundUpdate(ns2);

        // this will pickup changes done by ns2 and update
        // the head revision
        runBackgroundRead(ns1);
        // the next step is where the issue described
        // in OAK-3433 occurs.
        // the journal entry with changes done on ns1 is pushed
        // with the current head revision, which is newer
        // than the most recent change in the journal entry
        runBackgroundUpdate(ns1);

        // perform a background read after the rebase
        // the first merge attempt will fail with a conflict
        // because /a/b/c is seen as changed in the future
        // without the fix for OAK-3433:
        // the second merge attempt succeeds because now the
        // /a/b/c change revision is visible and happened before the commit
        // revision but before the base revision
        b2 = ns2.getRoot().builder();
        b2.child("z").setProperty("q", "v");
        try {
            ns2.merge(b2, new CommitHook() {
                @Nonnull
                @Override
                public NodeState processCommit(NodeState before,
                                               NodeState after,
                                               CommitInfo info)
                        throws CommitFailedException {
                    runBackgroundRead(ns2);

                    NodeBuilder builder = after.builder();
                    if (builder.getChildNode("a").getChildNode("b").hasChildNode("c")) {
                        builder.child("a").child("b").child("c").child("bar");
                    } else {
                        throw new CommitFailedException(
                                CommitFailedException.OAK, 0,
                                "/a/b/c does not exist anymore");
                    }
                    return builder.getNodeState();
                }
            }, CommitInfo.EMPTY);
            fail("Merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }
    }

    private void assertNodeExists(NodeBuilder builder, String path) {
        for (String name : PathUtils.elements(path)) {
            builder = builder.getChildNode(name);
            assertTrue("node '" + name + "' does not exist", builder.exists());
        }
    }

    private static void merge(NodeStore store,
                              NodeBuilder builder) throws Exception {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
