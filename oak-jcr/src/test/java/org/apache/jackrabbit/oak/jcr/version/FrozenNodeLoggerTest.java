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
package org.apache.jackrabbit.oak.jcr.version;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.TreeProviderService;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.JcrConstants.NT_FROZENNODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class FrozenNodeLoggerTest {

    private final TreeProvider treeProvider = new TreeProviderService();

    private final Whiteboard whiteboard = new DefaultWhiteboard();

    private final Clock clock = new Clock.Virtual();

    private final AtomicLong numMessages = new AtomicLong();

    private final FrozenNodeLogger logger = new FrozenNodeLogger(clock, whiteboard) {
        @Override
        protected void logFrozenNode(Tree tree) {
            numMessages.incrementAndGet();
            super.logFrozenNode(tree);
        }
    };

    private final Tree emptyTree = treeProvider.createReadOnlyTree(EmptyNodeState.EMPTY_NODE);

    private final Tree folderTree = treeProvider.createReadOnlyTree(
            new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME)
                    .getNodeState()
    );

    private final Tree frozenNodeTree = treeProvider.createReadOnlyTree(
            new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
                    .setProperty(JCR_PRIMARYTYPE, NT_FROZENNODE, Type.NAME)
                    .getNodeState()
    );

    @Before
    public void enableLogger() throws Exception {
        FeatureToggle toggle = WhiteboardUtils.getService(
                whiteboard, FeatureToggle.class);
        assertNotNull(toggle);
        toggle.setEnabled(true);
        Field f = FrozenNodeLogger.class.getDeclaredField("NO_MESSAGE_UNTIL");
        f.setAccessible(true);
        f.set(FrozenNodeLogger.class, 0);
    }

    @After
    public void after() {
        logger.close();
    }

    @Test
    public void emptyNode() {
        assertMessages(() -> { logger.lookupById(emptyTree); return 0; });
    }

    @Test
    public void folderNode() {
        assertMessages(() -> { logger.lookupById(folderTree); return 0; });
    }

    @Test
    public void frozenNode() {
        assertMessages(() -> { logger.lookupById(frozenNodeTree); return 1; });
    }

    @Test
    public void atMostOncePerSecond() throws Exception {
        assertMessages(() -> { logger.lookupById(frozenNodeTree); return 1; });
        assertMessages(() -> { logger.lookupById(frozenNodeTree); return 0; });
        clock.waitUntil(clock.getTime() + 1000);
        assertMessages(() -> { logger.lookupById(frozenNodeTree); return 1; });
    }

    private void assertMessages(Callable<Integer> r) {
        long num = numMessages.get();
        int expected;
        try {
            expected = r.call();
        } catch (Exception e) {
            fail(e.getMessage());
            expected = -1;
        }
        assertEquals(expected, numMessages.get() - num);
    }
}
