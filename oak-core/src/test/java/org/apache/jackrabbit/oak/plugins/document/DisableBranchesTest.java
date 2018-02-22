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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DisableBranchesTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private static final String REGEX = "^Background operations stats.* num:([^0]\\d*).*";

    private LogCustomizer logCustomizer = LogCustomizer.forLogger(
            DocumentNodeStore.class.getName())
            .enable(Level.DEBUG).matchesRegex(REGEX).create();

    private DocumentNodeStore ns;

    @Before
    public void before() {
        ns = builderProvider.newBuilder().setAsyncDelay(0)
                .disableBranches().getNodeStore();
    }

    @After
    public void after() {
        logCustomizer.finished();
    }

    @Test
    public void backgroundWrite() throws Exception {
        final int NUM_UPDATES = DocumentRootBuilder.UPDATE_LIMIT * 3 / 2;
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < NUM_UPDATES; i++) {
            builder.child("node-" + i).child("test");
        }
        merge(ns, builder);
        ns.runBackgroundOperations();
        logCustomizer.starting();
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread bgThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    ns.runBackgroundOperations();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        });
        bgThread.start();

        for (int j = 0; j < 20; j++) {
            builder = ns.getRoot().builder();
            for (int i = 0; i < NUM_UPDATES; i++) {
                builder.child("node-" + i).child("test").setProperty("p", j);
            }
            merge(ns, builder);
        }
        running.set(false);
        bgThread.join();

        Iterable<Integer> updates = getUpdates();

        // background thread must always update _lastRev from an entire
        // branch commit and never partially
        assertTrue(updates.iterator().hasNext()); // at least one
        assertThat(updates, everyItem(is(NUM_UPDATES * 2 + 1)));
    }

    private Iterable<Integer> getUpdates() {
        Pattern p = Pattern.compile(REGEX);
        List<Integer> updates = new ArrayList<Integer>();
        for (String msg : logCustomizer.getLogs()) {
            Matcher m = p.matcher(msg);
            if (m.find()) {
                updates.add(Integer.parseInt(m.group(1)));
            }
        }
        return updates;
    }

    private static NodeState merge(NodeStore store, NodeBuilder root)
            throws CommitFailedException {
        return store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
