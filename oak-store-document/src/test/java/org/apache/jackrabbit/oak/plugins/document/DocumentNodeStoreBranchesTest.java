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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentNodeStoreBranchesTest {

    static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStoreBranchesTest.class);

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @BeforeClass
    public static void disableJournalDiff() {
        System.setProperty(DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL, "true");
    }

    @AfterClass
    public static void reset() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
        System.clearProperty(DocumentNodeStore.SYS_PROP_DISABLE_JOURNAL);
    }

    @Test
    public void commitHookChangesOnBranchWithInterference() throws Exception {
        Clock c = new Clock.Virtual();
        c.waitUntil(System.currentTimeMillis());
        Revision.setClock(c);
        ClusterNodeInfo.setClock(c);

        // enough nodes that diffManyChildren() is called
        final int NUM_NODES = DocumentMK.MANY_CHILDREN_THRESHOLD * 2;
        LOG.info("create new dns");
        Builder nsBuilder = builderProvider.newBuilder();
        nsBuilder.setAsyncDelay(0).clock(c);
        final DocumentNodeStore ns = nsBuilder.getNodeStore();

        // 0) initialization
        {
            LOG.info("initialization");
            NodeBuilder initBuilder = ns.getRoot().builder();
            for(int i=0; i<NUM_NODES; i++) {
                initBuilder.child("child"+i);
            }
            ns.merge(initBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }


        // 1) do more than UPDATE_LIMIT changes
        LOG.info("starting doing many changes to force a branch commit");
        NodeBuilder rootBuilder = ns.getRoot().builder();
        int totalUpdates = 5 * DocumentMK.UPDATE_LIMIT;
        int updateShare = totalUpdates / NUM_NODES;
        for(int i=0; i<NUM_NODES; i++) {
            NodeBuilder childBuilder = rootBuilder.child("child"+i);
            childBuilder.child("grandChild"+i);
            childBuilder.setProperty("p1", "originalValue");
            for(int j=0; j<updateShare; j++) {
                childBuilder.setProperty("someProperty" + j, "sameValue");
            }
        }

        // 2) wait 6 sec
        LOG.info("after purge was triggered above, 'waiting' 6sec");
        c.waitUntil(c.getTime() + 6000);

        // 3) now in another 'session', do another merge - to change the head
        LOG.info("in another session, do some unrelated changes to change the head");
        NodeBuilder parallelBuilder = ns.getRoot().builder();
        parallelBuilder.child("unrelated").setProperty("anyProp", "anywhere");
        ns.merge(parallelBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // 4) now merge the first session - this should now fail
        LOG.info("now merge the original builder - this should cause not all children to be visited");
        ns.merge(rootBuilder, CompositeHook.compose(Arrays.<CommitHook> asList(new TestHook("p"), new TestHook("q"))),
                CommitInfo.EMPTY);

        DocumentNodeState root = ns.getRoot();

        for(int i=0; i<NUM_NODES; i++) {
            NodeState child = root.getChildNode("child"+i);
            assertTrue(child.exists());
            assertEquals("test", child.getProperty("p1").getValue(Type.STRING));
        }
    }

    private static class TestEditor extends DefaultEditor {

        private final NodeBuilder builder;
        private final String prefix;

        TestEditor(NodeBuilder builder, String prefix) {
            this.builder = builder;
            this.prefix = prefix;
        }

        @Override
        public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return new TestEditor(builder.child(name), prefix);
        }

        @Override
        public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return new TestEditor(builder.child(name), prefix);
        }

        @Override
        public void propertyAdded(PropertyState after) throws CommitFailedException {
            if (after.getName().startsWith(prefix)) {
                builder.setProperty(after.getName(), "test");
            }
        }
    }

    private static class TestHook extends EditorHook {

        TestHook(final String prefix) {
            super(new EditorProvider() {
                @CheckForNull
                @Override
                public Editor getRootEditor(NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
                        throws CommitFailedException {
                    return new TestEditor(builder, prefix);
                }
            });
        }
    }
}
