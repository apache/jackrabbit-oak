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
package org.apache.jackrabbit.oak;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.plugins.index.WhiteboardIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * OakTest... TODO
 */
public class OakTest {

    @Test
    public void testWithDefaultWorkspaceName() throws Exception {
        ContentRepository repo = new Oak().with("test").with(new OpenSecurityProvider()).createContentRepository();

        String[] valid = new String[] {null, "test"};
        for (String wspName : valid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                assertEquals("test", cs.getWorkspaceName());
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

        String[] invalid = new String[] {"", "another", Oak.DEFAULT_WORKSPACE_NAME};
        for (String wspName : invalid) {
            ContentSession cs = null;
            try {
                cs = repo.login(null, wspName);
                fail("invalid workspace nam");
            } catch (NoSuchWorkspaceException e) {
                // success
            } finally {
                if (cs != null) {
                    cs.close();
                }
            }
        }

    }

    @Test
    public void testContentRepositoryReuse() throws Exception {
        Oak oak = new Oak().with(new OpenSecurityProvider());
        ContentRepository r0 = null;
        ContentRepository r1 = null;
        try {
            r0 = oak.createContentRepository();
            r1 = oak.createContentRepository();
            assertEquals(r0, r1);
        } finally {
            if (r0 != null) {
                ((Closeable) r0).close();
            }
        }
    }

    @Test
    public void checkExecutorShutdown() throws Exception {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {

            }
        };
        Oak oak = new Oak().with(new OpenSecurityProvider());
        ContentRepository repo = oak.createContentRepository();
        WhiteboardUtils.scheduleWithFixedDelay(oak.getWhiteboard(), runnable, 1);
        ((Closeable) repo).close();

        try {
            WhiteboardUtils.scheduleWithFixedDelay(oak.getWhiteboard(), runnable, 1);
            fail("Executor should have rejected the task");
        } catch (RejectedExecutionException ignore) {

        }

        //Externally passed executor should not be shutdown upon repository close
        ScheduledExecutorService externalExecutor = Executors.newSingleThreadScheduledExecutor();
        Oak oak2 = new Oak().with(new OpenSecurityProvider()).with(externalExecutor);
        ContentRepository repo2 = oak2.createContentRepository();
        WhiteboardUtils.scheduleWithFixedDelay(oak2.getWhiteboard(), runnable, 1);

        ((Closeable) repo2).close();
        WhiteboardUtils.scheduleWithFixedDelay(oak2.getWhiteboard(), runnable, 1);
        externalExecutor.shutdown();
    }

    @Test
    public void closeAsyncIndexers() throws Exception{
        final AtomicReference<AsyncIndexUpdate> async = new AtomicReference<AsyncIndexUpdate>();
        Whiteboard wb = new DefaultWhiteboard(){
            @Override
            public <T> Registration register(Class<T> type, T service, Map<?, ?> properties) {
                if (service instanceof AsyncIndexUpdate){
                    async.set((AsyncIndexUpdate) service);
                }
                return super.register(type, service, properties);
            }
        };
        Oak oak = new Oak()
                .with(new OpenSecurityProvider())
                .with(wb)
                .withAsyncIndexing("foo-async", 5);
        ContentRepository repo = oak.createContentRepository();

        ((Closeable)repo).close();
        assertNotNull(async.get());
        assertTrue(async.get().isClosed());
        assertNull(WhiteboardUtils.getService(wb, AsyncIndexUpdate.class));
    }

    @Test(expected = CommitFailedException.class)
    public void checkMissingStrategySetting() throws Exception{
        Whiteboard wb = new DefaultWhiteboard();
        WhiteboardIndexEditorProvider wbProvider = new WhiteboardIndexEditorProvider();
        wbProvider.start(wb);

        Registration r1 = wb.register(IndexEditorProvider.class, new PropertyIndexEditorProvider(), null);
        Registration r2 = wb.register(IndexEditorProvider.class, new ReferenceEditorProvider(), null);

        Oak oak = new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(wb)
                .with(wbProvider)
                .withFailOnMissingIndexProvider();

        ContentRepository repo = oak.createContentRepository();

        ContentSession cs = repo.login(null, null);

        Root root = cs.getLatestRoot();
        Tree t = root.getTree("/");
        t.setProperty("foo", "u1", Type.REFERENCE);

        r1.unregister();

        root.commit();
        cs.close();
        ((Closeable)repo).close();
    }

    @Test
    public void commitContextInCommitInfo() throws Exception{
        CommitInfoCapturingStore store = new CommitInfoCapturingStore();
        Oak oak = new Oak(store);

        ContentRepository repo = oak.with(new OpenSecurityProvider()).createContentRepository();
        assertThat(store.infos, is(not(empty())));
        for (CommitInfo ci : store.infos){
            assertNotNull(ci.getInfo().get(CommitContext.NAME));
        }
        ((Closeable)repo).close();
    }

    private static class CommitInfoCapturingStore extends MemoryNodeStore {
        List<CommitInfo> infos = Lists.newArrayList();

        @Override
        public synchronized NodeState merge(@Nonnull NodeBuilder builder, @Nonnull CommitHook commitHook,
                                            @Nonnull CommitInfo info) throws CommitFailedException {
            if (info.getSessionId().equals(OakInitializer.SESSION_ID)) {
                this.infos.add(info);
            }
            return super.merge(builder, commitHook, info);
        }


    }

}