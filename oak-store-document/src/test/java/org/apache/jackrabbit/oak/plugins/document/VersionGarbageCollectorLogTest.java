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

import java.lang.reflect.Field;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import ch.qos.logback.classic.Level;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class VersionGarbageCollectorLogTest {

    private static final int BATCH_SIZE;

    static {
        try {
            Field f = VersionGarbageCollector.class.getDeclaredField("DELETE_BATCH_SIZE");
            f.setAccessible(true);
            BATCH_SIZE = (int) f.get(null);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private LogCustomizer logCustomizer = LogCustomizer.forLogger(
            VersionGarbageCollector.class.getName()).enable(Level.INFO).create();

    private Clock clock;

    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ns = new DocumentMK.Builder().setAsyncDelay(0).clock(clock).getNodeStore();
        logCustomizer.starting();
    }

    @After
    public void after() {
        logCustomizer.finished();
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
    }

    @Test
    public void gc() throws Exception {
        createGarbage();

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        VersionGarbageCollector gc = ns.getVersionGarbageCollector();
        gc.gc(30, TimeUnit.MINUTES);
        List<String> messages = getDeleteMessages();
        assertThat(messages.size(), greaterThan(0));
        for (String msg : messages) {
            assertThat(getNumDeleted(msg), lessThan(BATCH_SIZE + 1));
        }
    }

    private int getNumDeleted(String msg) {
        int idx = msg.indexOf('[');
        return Integer.parseInt(msg.substring(idx + 1, msg.indexOf(']')));
    }

    private void createGarbage() throws Exception {
        Random r = new Random(42);
        String path = "/";
        for (int i = 0; i < 1000; i++) {
            int v = r.nextInt(10);
            if (v == 0 || path.equals("/")) {
                // create new top level node
                path = "/node-" + i;
                addNode(path);
            } else {
                addNode(path + "/node-" + i);
            }
        }
        for (String name : ns.getRoot().getChildNodeNames()) {
            if (name.startsWith("node-")) {
                if (r.nextBoolean()) {
                    recreate(name);
                } else {
                    remove(name);
                }
            }
        }
    }

    private List<String> getDeleteMessages() {
        List<String> messages = Lists.newArrayList();
        for (String msg : logCustomizer.getLogs()) {
            if (msg.startsWith("Proceeding to delete [")) {
                messages.add(msg);
            }
        }
        return messages;
    }

    private void remove(String name) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(name).remove();
        TestUtils.merge(ns, builder);
    }

    private void recreate(String name) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(name).remove();
        builder.child(name);
        TestUtils.merge(ns, builder);
    }

    private void addNode(String path) throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder b = builder;
        for (String name : PathUtils.elements(path)) {
            b = b.child(name);
        }
        TestUtils.merge(ns, builder);
    }
}
