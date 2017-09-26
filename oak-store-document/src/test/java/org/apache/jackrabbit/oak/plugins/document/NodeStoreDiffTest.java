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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.UPDATE_LIMIT;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class NodeStoreDiffTest {

    private static final Logger LOG = LoggerFactory.getLogger(NodeStoreDiffTest.class);

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStore ns;
    private final TestDocumentStore tds = new TestDocumentStore();

    @Before
    public void setUp() throws IOException {
        ns = builderProvider.newBuilder()
                .setDocumentStore(tds)
                .setUseSimpleRevision(true) //To simplify debugging
                .setAsyncDelay(0)
                .memoryCacheSize(0) //Keep the cache size zero such that nodeCache is not used
                .getNodeStore();
    }

    @Test
    public void diffWithConflict() throws Exception{
        //Last rev on /var would be 1-0-1
        createNodes("/var/a", "/var/b/b1");

        //1. Dummy commits to bump the version no
        createNodes("/fake/b");
        createNodes("/fake/c");

        //Root rev = 3-0-1
        //Root rev = 3-0-1

        //2. Create a node under /var/a but hold on commit
        NodeBuilder b1 = ns.getRoot().builder();
        createNodes(b1, "/var/a/a1");

        //3. Remove a node under /var/b and commit it
        NodeBuilder b2 = ns.getRoot().builder();
        b2.child("var").child("b").child("b1").remove();
        merge(b2);

        //4. Now merge and commit the changes in b1 and include conflict hooks
        //For now exception would be thrown
        ns.merge(b1,
                new CompositeHook(
                        ConflictHook.of(new AnnotatingConflictHandler()),
                        new EditorHook(new ConflictValidatorProvider())
                ),
                CommitInfo.EMPTY);
    }

    /**
     * This testcase demonstrates that diff logic in merge part traverses node path
     * which are not affected by the commit
     * @throws Exception
     */
    @Test
    public void testDiff() throws Exception{
        createNodes("/oak:index/prop-a", "/oak:index/prop-b", "/etc/workflow");

        //1. Make some other changes so as to bump root rev=3
        createNodes("/fake/a");
        createNodes("/fake/b");

        //2 - Start change
        NodeBuilder b2 = ns.getRoot().builder();
        createNodes(b2, "/etc/workflow/instance1");

        tds.reset();
        //3. Merge which does a rebase
        ns.merge(b2, new CommitHook() {
            @Nonnull
            public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
                NodeBuilder rb = after.builder();
                createNodes(rb, "/oak:index/prop-a/a1");

                //2.1 Commit some change under prop-
                //This cause diff in lastRev of base node state in ModifiedNodeState for
                //oak:index due to which when the base state is compared in ModifiedNodeState
                //then it fetches the new DocumentNodeState whose lastRev is greater than this.base.lastRev
                //but less then lastRev of the of readRevision. Where readRevision is the rev of root node when
                //rebase was performed

                // remember paths accessed so far
                List<String> paths = Lists.newArrayList(tds.paths);

                //This is not to be done in actual cases as CommitHooks are invoked in critical sections
                //and creating nodes from within CommitHooks would cause deadlock. This is done here to ensure
                //that changes are done when rebase has been performed and merge is about to happen
                createNodes("/oak:index/prop-b/b1");

                // reset accessed paths
                tds.reset();
                tds.paths.addAll(paths);

                //For now we the cache is disabled (size 0) so this is not required
                //ns.nodeCache.invalidateAll();

                return rb.getNodeState();
            }
        }, CommitInfo.EMPTY);

        //Assert that diff logic does not traverse to /oak:index/prop-b/b1 as
        //its not part of commit
        assertFalse(tds.paths.contains("/oak:index/prop-b/b1"));
    }

    // OAK-4403
    @Test
    public void diffWithPersistedBranch() throws Exception{
        createNodes("/content/a", "/etc/x", "var/x", "/etc/y");

        //#1 - Start making some changes
        NodeBuilder b = ns.getRoot().builder();
        createNodes(b, "/content/b");

        //#2 - Do lots of change so as to trigger branch creation
        //BranchState > Unmodified -> InMemory
        for (int i = 0; i <= UPDATE_LIMIT; i++) {
            b.child("content").child("a").child("c" + i);
        }

        //#3 -  In between push some changes to NodeStore
        NodeBuilder b2 = ns.getRoot().builder();
        b2.child("etc").child("x").setProperty("foo", 1);
        b2.child("var").remove();
        merge(b2);
        ns.runBackgroundOperations();

        createNodes(b, "/content/e");

        tds.reset();

        merge(b);

        //With the merge the diff logic should not be accessing the
        //paths which are not part of the current commit like /etc and /var
        assertThat(tds.paths, not(hasItem("/etc/x")));
        assertThat(tds.paths, not(hasItem("/var/x")));
    }

    private NodeState merge(NodeBuilder nb) throws CommitFailedException {
        NodeState result = ns.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        prRev(result);
        ops();
        return result;
    }

    private void ops(){
        ns.runBackgroundOperations();
    }

    private NodeState createNodes(String... paths) throws CommitFailedException {
        NodeBuilder nb = ns.getRoot().builder();
        createNodes(nb, paths);
        return merge(nb);
    }

    private static void createNodes(NodeBuilder builder, String... paths) {
        for(String path : paths) {
            NodeBuilder cb = builder;
            for (String name : PathUtils.elements(path)) {
                cb = cb.child(name);
            }
        }
    }

    private void prRev(NodeState ns){
        if(ns instanceof DocumentNodeState){
            DocumentNodeState dns = ((DocumentNodeState) ns);
            LOG.info("Root at {} ({})", dns.getRootRevision(), dns.getLastRevision());
        }
    }


    private static class TestDocumentStore extends MemoryDocumentStore {
        final Set<String> paths = Sets.newHashSet();

        @Override
        public <T extends Document> T find(Collection<T> collection, String key) {
            if(collection == Collection.NODES){
                paths.add(Utils.getPathFromId(key));
            }
            return super.find(collection, key);
        }

        void reset(){
            paths.clear();
        }
    }
}
