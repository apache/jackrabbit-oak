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
package org.apache.jackrabbit.oak.plugins.index.counter;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MountsNodeCounterTest {

    private NodeStore nodeStore;

    private Root root;

    private MountInfoProvider mip;

    private Whiteboard wb;

    @Before
    public void before() throws Exception {
        ContentSession session = createRepository().login(null, null);
        root = session.getLatestRoot();
    }

    @Test
    public void testMultipleMounts() throws CommitFailedException {
        root.getTree("/oak:index/counter").setProperty("resolution", 1);
        root.commit();

        Tree rootTree = root.getTree("/");

        Tree apps = rootTree.addChild("apps");
        Tree libs = rootTree.addChild("libs");
        Tree content = rootTree.addChild("content");
        Tree nested = rootTree.addChild("nested");
        Tree nestedMount = nested.addChild("mount");
        Tree fragments = rootTree.addChild("var").addChild("fragments").addChild("oak:mount-libs");

        addChildren(apps, 100);
        addChildren(libs, 200);
        addChildren(content, 400);
        addChildren(nested, 800);
        addChildren(nestedMount, 1600);
        addChildren(fragments, 3200);

        root.commit();
        runAsyncIndex();

        // leaves:
        Mount defaultMount = mip.getDefaultMount();
        Mount libsMount = mip.getMountByName("libs");

        assertCountEquals(100, libsMount, "apps");
        assertCountEquals(200, libsMount, "libs");
        assertCountEquals(400, defaultMount, "content");
        assertCountEquals(800, defaultMount, "nested");
        assertCountEquals(1600, libsMount, "nested/mount");
        assertCountEquals(3200, libsMount, "var");
        assertCountEquals(3200, libsMount, "var/fragments");
        assertCountEquals(3200, libsMount, "var/fragments/oak:mount-libs");
        assertCountEquals(0, defaultMount, "var");
        assertCountEquals(0, defaultMount, "var/fragments");

        assertCountEquals(100 + 200 + 1600 + 3200, libsMount, "");
        assertCountEquals(1600, libsMount, "nested");
    }

    private void assertCountEquals(int expectedCount, Mount mount, String path) {
        String p = PathUtils.concat("/oak:index/counter", Multiplexers.getNodeForMount(mount, ":index"), path);
        NodeState s = nodeStore.getRoot();
        for (String element : PathUtils.elements(p)) {
            s = s.getChildNode(element);
            if (s == null) {
                if (expectedCount == 0) {
                    return;
                }
                fail("Can't find node " + p);
            }
        }
        PropertyState ps = s.getProperty(":cnt");
        if (ps == null) {
            if (expectedCount == 0) {
                return;
            }
            fail("There's no :cnt property on " + p);
        }
        long v = ps.getValue(Type.LONG);


        assertTrue("expected:<" + expectedCount + "> but was:<" + v + ">", Math.abs(expectedCount - v) < 10);
    }

    private static void addChildren(Tree tree, int count) {
        for (int i = 0; i < count; i++) {
            tree.addChild("n-" + i);
        }
    }

    protected ContentRepository createRepository() {
        Mounts.Builder builder = Mounts.newBuilder();
        builder.mount("libs", false, Arrays.asList("/var/fragments"), Arrays.asList("/apps", "/libs", "/nested/mount"));
        mip = builder.build();

        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider().with(mip))
                .with(new NodeCounterEditorProvider().with(mip))
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        wb = oak.getWhiteboard();
        return oak.createContentRepository();
    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean apply(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
        root.refresh();
    }

}
