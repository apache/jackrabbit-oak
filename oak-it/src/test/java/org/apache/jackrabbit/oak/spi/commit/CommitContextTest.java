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

package org.apache.jackrabbit.oak.spi.commit;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitContextTest extends OakBaseTest {
    private CommitInfoCapturingObserver observer = new CommitInfoCapturingObserver();
    private ContentSession session;
    private ContentRepository repository;

    public CommitContextTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @After
    public void closeRepository() throws IOException {
        if (session != null) {
            session.close();
        }

        if (repository instanceof Closeable) {
            ((Closeable) repository).close();
        }
    }

    @Test
    public void basicSetup() throws Exception {
        repository = new Oak(store)
                .with(new OpenSecurityProvider())
                .with(observer)
                .createContentRepository();

        session = newSession();
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        root.commit(ImmutableMap.<String, Object>of("foo", "bar"));

        assertNotNull(observer.info);
        assertTrue(observer.info.getInfo().containsKey("foo"));

        assertTrue(observer.info.getInfo().containsKey(CommitContext.NAME));

        try {
            observer.info.getInfo().put("x", "y");
            fail("Info map should be immutable");
        } catch (Exception ignore) {

        }
    }

    @Test
    public void attributeAddedByCommitHook() throws Exception{
        repository = new Oak(store)
                .with(new OpenSecurityProvider())
                .with(observer)
                .with(new CommitHook() {
                    @Nonnull
                    @Override
                    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
                            throws CommitFailedException {
                        CommitContext attrs = (CommitContext) info.getInfo().get(CommitContext.NAME);
                        assertNotNull(attrs);
                        attrs.set("foo", "bar");
                        return after;
                    }
                })
                .createContentRepository();

        session = newSession();
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        root.commit();

        assertNotNull(observer.info);

        CommitContext attrs = (CommitContext) observer.info.getInfo().get(CommitContext.NAME);
        assertNotNull(attrs.get("foo"));
    }

    @Test
    public void attributesBeingReset() throws Exception{
        //This test can only work with DocumentNodeStore as only that
        //reattempt a failed merge. SegmentNodeStore would not do another
        //attempt
        assumeDocumentStore();
        final AtomicInteger invokeCount = new AtomicInteger();
        repository = new Oak(store)
                .with(new OpenSecurityProvider())
                .with(observer)
                .with(new CommitHook() {
                    @Nonnull
                    @Override
                    public NodeState processCommit(NodeState before, NodeState after, CommitInfo info)
                            throws CommitFailedException {
                        CommitContext attrs = (CommitContext) info.getInfo().get(CommitContext.NAME);
                        int count = invokeCount.getAndIncrement();
                        if (count == 0) {
                            attrs.set("a", "1");
                            attrs.set("b", "2");
                        } else {
                            attrs.set("a", "3");
                        }
                        return after;
                    }
                })
                .with(new CommitHook() {
                    @Nonnull
                    @Override
                    public NodeState processCommit(NodeState before, NodeState after,
                                                   CommitInfo info) throws CommitFailedException {
                        if (invokeCount.get() == 1){
                            throw new CommitFailedException(MERGE, 0, "attribute reset test");
                        }
                        return after;
                    }
                })
                .createContentRepository();

        session = newSession();
        Root root = session.getLatestRoot();
        Tree tree = root.getTree("/");
        tree.setProperty("a", 1);
        root.commit();

        assertNotNull(observer.info);

        CommitContext attrs = (CommitContext) observer.info.getInfo().get(CommitContext.NAME);
        assertEquals("3", attrs.get("a"));
        assertNull(attrs.get("b"));
    }

    private void assumeDocumentStore() {
        Assume.assumeThat(fixture.toString(), containsString("DocumentNodeStore"));
    }

    private ContentSession newSession() throws LoginException, NoSuchWorkspaceException {
        return repository.login(null, null);
    }

    private static class CommitInfoCapturingObserver implements Observer {
        CommitInfo info;

        @Override
        public void contentChanged(@Nonnull NodeState root, @Nonnull CommitInfo info) {
            this.info = info;
        }
    }
}
