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

package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalProperty;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyBuilder;
import org.apache.jackrabbit.oak.plugins.document.spi.JournalPropertyService;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ExternalChangesTest {
    @Rule
    public final DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    private CommitInfoCollector c1 = new CommitInfoCollector();
    private CommitInfoCollector c2 = new CommitInfoCollector();
    private JournalPropertyHandlerFactory tracker = new JournalPropertyHandlerFactory();
    private Whiteboard wb = new DefaultWhiteboard();

    @Before
    public void setUp() {
        tracker.start(wb);
        MemoryDocumentStore store = new MemoryDocumentStore();
        ns1 = newDocumentNodeStore(store, 1);
        ns2 = newDocumentNodeStore(store, 2);

        ns1.addObserver(c1);
        ns2.addObserver(c2);
    }

    @Test
    public void defaultConfig() throws Exception{
        assertEquals(50, ns1.getChangeSetMaxItems());
        assertEquals(9, ns1.getChangeSetMaxDepth());
    }

    @Test
    public void changeSetForExternalChanges() throws Exception{
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("a");
        b1.setProperty("foo1", "bar");
        ns1.merge(b1, newCollectingHook(), newCommitInfo());

        NodeBuilder b2 = ns1.getRoot().builder();
        b2.child("b");
        b2.setProperty("foo2", "bar");
        ns1.merge(b2, newCollectingHook(), newCommitInfo());

        ns1.runBackgroundUpdateOperations();

        c2.reset();
        ns2.runBackgroundReadOperations();

        CommitInfo ci = c2.getExternalChange();
        CommitContext cc = (CommitContext) ci.getInfo().get(CommitContext.NAME);
        assertNotNull(cc);
        ChangeSet cs = (ChangeSet) cc.get(ChangeSet.COMMIT_CONTEXT_OBSERVATION_CHANGESET);
        assertNotNull(cs);
        assertFalse(cs.anyOverflow());
        assertThat(cs.getPropertyNames(), containsInAnyOrder("foo1", "foo2"));
    }

    @Test
    public void missingChangeSetResultsInOverflow() throws Exception{
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("a");
        b1.setProperty("foo1", "bar");
        ns1.merge(b1, newCollectingHook(), newCommitInfo());

        NodeBuilder b2 = ns1.getRoot().builder();
        b2.child("b");
        b2.setProperty("foo2", "bar");
        //Commit without ChangeSet
        ns1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns1.runBackgroundUpdateOperations();

        c2.reset();
        ns2.runBackgroundReadOperations();

        CommitInfo ci = c2.getExternalChange();
        CommitContext cc = (CommitContext) ci.getInfo().get(CommitContext.NAME);
        assertNotNull(cc);
        ChangeSet cs = (ChangeSet) cc.get(ChangeSet.COMMIT_CONTEXT_OBSERVATION_CHANGESET);
        assertNotNull(cs);

        //ChangeSet should result in overflow
        assertTrue(cs.anyOverflow());
    }

    @Test
    public void changeSetForBranchCommit() throws Exception{
        final int NUM_NODES = DocumentMK.UPDATE_LIMIT / 2;
        final int NUM_PROPS = 10;

        Set<String> propNames = Sets.newHashSet();
        NodeBuilder b1 = ns1.getRoot().builder();
        for (int i = 0; i < NUM_NODES; i++) {
            NodeBuilder c = b1.child("n" + i);
            for (int j = 0; j < NUM_PROPS; j++) {
                c.setProperty("q" + j, "value");
                c.setProperty("p" + j, "value");
                propNames.add("q" + j);
                propNames.add("p" + j);
            }
        }

        ns1.merge(b1, newCollectingHook(), newCommitInfo());
        ns1.runBackgroundUpdateOperations();

        c2.reset();
        ns2.runBackgroundReadOperations();

        CommitInfo ci = c2.getExternalChange();
        CommitContext cc = (CommitContext) ci.getInfo().get(CommitContext.NAME);
        assertNotNull(cc);
        ChangeSet cs = (ChangeSet) cc.get(ChangeSet.COMMIT_CONTEXT_OBSERVATION_CHANGESET);
        assertNotNull(cs);
        assertTrue(cs.getPropertyNames().containsAll(propNames));
    }

    @Test
    public void journalService() throws Exception{
        wb.register(JournalPropertyService.class, new TestJournalService(), null);

        //Do a dummy write so that journal property handler gets refreshed
        //and picks our newly registered service
        NodeBuilder b0 = ns1.getRoot().builder();
        b0.child("0");
        ns1.merge(b0, newCollectingHook(), newCommitInfo());
        ns1.runBackgroundUpdateOperations();

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("a");
        CommitContext cc = new SimpleCommitContext();
        cc.set(TestProperty.NAME, new TestProperty("foo"));
        ns1.merge(b1, newCollectingHook(), newCommitInfo(cc));

        NodeBuilder b2 = ns1.getRoot().builder();
        b2.child("b");
        cc = new SimpleCommitContext();
        cc.set(TestProperty.NAME, new TestProperty("bar"));
        ns1.merge(b2, newCollectingHook(), newCommitInfo(cc));

        //null entry
        NodeBuilder b3 = ns1.getRoot().builder();
        b3.child("c");
        ns1.merge(b3, newCollectingHook(), newCommitInfo());

        ns1.runBackgroundUpdateOperations();

        c2.reset();
        ns2.runBackgroundReadOperations();

        CommitInfo ci = c2.getExternalChange();
        cc = (CommitContext) ci.getInfo().get(CommitContext.NAME);

        CumulativeTestProperty ct = (CumulativeTestProperty) cc.get(TestProperty.NAME);
        assertNotNull(ct);

        assertThat(ct.values, containsInAnyOrder("foo", "bar", "NULL"));
    }

    private CommitHook newCollectingHook(){
        return new EditorHook(new ChangeCollectorProvider());
    }

    private CommitInfo newCommitInfo(){
        return newCommitInfo(new SimpleCommitContext());
    }

    private CommitInfo newCommitInfo(CommitContext commitContext){
        Map<String, Object> info = ImmutableMap.<String, Object>of(CommitContext.NAME, commitContext);
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

    private DocumentNodeStore newDocumentNodeStore(DocumentStore store, int clusterId) {
        return builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setDocumentStore(store)
                .setJournalPropertyHandlerFactory(tracker)
                .setLeaseCheck(false) // disabled for debugging purposes
                .setClusterId(clusterId)
                .getNodeStore();
    }

    private static class CommitInfoCollector implements Observer {
        List<CommitInfo> infos = Lists.newArrayList();

        @Override
        public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
           infos.add(info);
        }

        public CommitInfo getExternalChange(){
            List<CommitInfo> result = Lists.newArrayList();
            for (CommitInfo info : infos){
                if (info.isExternal()) {
                    result.add(info);
                }
            }
            assertEquals(1, result.size());
            return result.get(0);
        }

        void reset(){
            infos.clear();
        }
    }

    private static class TestJournalService implements JournalPropertyService {

        @Override
        public JournalPropertyBuilder newBuilder() {
            return new TestJournalBuilder();
        }

        @Override
        public String getName() {
            return TestProperty.NAME;
        }
    }

    private static class TestProperty implements JournalProperty {
        static final String NAME = "test.props";
        final String value;

        public TestProperty(String value) {
            this.value = value;
        }
    }

    private static class CumulativeTestProperty implements JournalProperty {
        final Set<String> values = Sets.newHashSet();
    }

    private static class TestJournalBuilder implements JournalPropertyBuilder<TestProperty>{
        final CumulativeTestProperty allProps = new CumulativeTestProperty();

        @Override
        public void addProperty(@Nullable TestProperty journalProperty) {
            if (journalProperty != null) {
                allProps.values.add(journalProperty.value);
            } else {
                allProps.values.add("NULL");
            }
        }

        @Override
        public String buildAsString() {
            return Joiner.on(",").join(allProps.values);
        }

        @Override
        public void addSerializedProperty(@Nullable String s) {
            if (s != null){
                Iterables.addAll(allProps.values, Splitter.on(',').split(s));
            }
        }

        @Override
        public JournalProperty build() {
            return allProps;
        }
    }
}
