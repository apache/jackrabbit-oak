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
package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

public class QueryOptimizerTest {

    private final MemoryNodeStore store = new MemoryNodeStore();
    private final AtomicLong clock = new AtomicLong();
    private final QueryOptimizer optimizer = new QueryOptimizer(clock::get);

    @Before
    public void init() {
        optimizer.init(store);
    }

    @Test
    public void absentConfiguration() throws Exception {
        assertEquals("a", new QueryOptimizer().rewrite("a", "xpath", 100));
        assertEquals("a", rewrite("a"));
        writeRules("[" + rule("a", "b") + "]");
        clock.set(10_000);
        assertEquals("b", rewrite("a"));
    }

    @Test
    public void orderedRulesAndDefaultOptions() throws Exception {
        writeRules("[" + rule("a", "b") + ","
                + "{\"find\":\"b\",\"replace\":\"c\",\"options\":{}},"
                + "{\"find\":\"c\",\"replace\":\"aa\",\"options\":{\"stop\":false}}]");
        assertEquals("aa aa", rewrite("a a"));
        assertEquals("xyz", rewrite("xyz"));
    }

    @Test
    public void stopOnlyAfterMatching() throws Exception {
        writeRules("[{\"find\":\"missing\",\"replace\":\"x\",\"options\":{\"stop\":true}},"
                + "{\"find\":\"a\",\"replace\":\"b\",\"options\":{\"stop\":true}},"
                + rule("b", "c") + "]");
        assertEquals("b b", rewrite("a a"));
    }

    @Test
    public void unchangedReplacementCanExcludeQuery() throws Exception {
        writeRules("[{\"find\":\"a\",\"replace\":\"a\",\"options\":{\"stop\":true}},"
                + rule("a", "b") + "]");
        assertEquals("a", rewrite("a"));
    }

    @Test
    public void optionalLanguageAndNoLiterals() throws Exception {
        writeRules("[{\"find\":\"a\",\"replace\":\"b\",\"language\":\"xpath\","
                + "\"options\":{\"stop\":true}}," + rule("a", "c") + "]");
        assertEquals("b", optimizer.rewrite("a", "xpath", 100));
        assertEquals("b", optimizer.rewrite("a", "xpath-noLiterals", 100));
        for (String language : new String[] {"JCR-SQL2", "sql", "JCR-JQOM", "JCR-SQL2-noLiterals"}) {
            assertEquals("c", optimizer.rewrite("a", language, 100));
        }
    }

    @Test
    public void captureGroupsEscapingAndMultiline() throws Exception {
        writeRules("[" + rule("(?m)^\\[([a-z]+)\\]$", "$1\\$") + "]");
        assertEquals("first$\nsecond$", rewrite("[first]\n[second]"));
    }

    @Test
    public void regexBudgetAbortsBacktrackingAndDiscardsPartialRewrites() throws Exception {
        writeRules("[" + rule("x", "y") + "," + rule("a+a+a+a+a+b", "unreachable") + "]");
        String original = "x" + "a".repeat(50);
        LogCustomizer logs = LogCustomizer.forLogger(QueryOptimizer.class).enable(Level.WARN).create();
        try {
            logs.starting();
            assertEquals(original, rewrite(original));
            assertEquals("Query rule 2 exceeded regex character-access budget 51000; using the original query",
                    logs.getLogs().get(0));
            assertEquals(original, rewrite(original));
            assertEquals(1, logs.getLogs().size());
            assertEquals("yabc", rewrite("xabc"));
            clock.addAndGet(10_000);
            assertEquals(original, rewrite(original));
            assertEquals(2, logs.getLogs().size());
        } finally {
            logs.finished();
        }
    }

    @Test
    public void regexBudgetIsSharedAcrossRules() throws Exception {
        String repeatedRule = rule("(?=a{20})a", "$0");
        writeRules("[" + rule("x", "y") + ","
                + String.join(",", Collections.nCopies(99, repeatedRule)) + "]");
        String original = "x" + "a".repeat(50);
        LogCustomizer logs = LogCustomizer.forLogger(QueryOptimizer.class).enable(Level.WARN).create();
        try {
            logs.starting();
            assertEquals(original, rewrite(original));
            assertEquals(1, logs.getLogs().size());
            assertTrue(logs.getLogs().get(0).contains("exceeded regex character-access budget 51000"));
        } finally {
            logs.finished();
        }
    }

    @Test
    public void internalQueriesBypassRules() throws Exception {
        NodeStore unreadStore = mock(NodeStore.class);
        QueryOptimizer internalOptimizer = new QueryOptimizer(clock::get);
        internalOptimizer.init(unreadStore);
        String statement = "a " + QueryEngine.INTERNAL_SQL2_QUERY;
        assertEquals(statement, internalOptimizer.rewrite(statement, "JCR-SQL2", 1000));
        verify(unreadStore, times(0)).getRoot();
    }

    @Test
    public void refreshAndDeletion() throws Exception {
        NodeStore countedStore = mock(NodeStore.class);
        when(countedStore.getRoot()).thenAnswer(invocation -> store.getRoot());
        optimizer.init(countedStore);
        writeRules("[" + rule("a", "b") + "]");
        assertEquals("b", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]");
        clock.set(9_999);
        assertEquals("b", rewrite("a"));
        verify(countedStore, times(1)).getRoot();
        clock.set(10_000);
        assertEquals("c", rewrite("a"));
        clock.set(20_000);
        assertEquals("c", rewrite("a"));
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("diff.index.optimizer")
                .getChildNode(QueryOptimizer.RULES_FILE).remove();
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertEquals("c", rewrite("a"));
        clock.set(30_000);
        assertEquals("a", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]");
        clock.set(40_000);
        assertEquals("c", rewrite("a"));
        writeRules("[]");
        clock.set(50_000);
        assertEquals("a", rewrite("a"));
    }

    @Test
    public void lastModifiedSkipsUnchangedFiles() throws Exception {
        String first = "2026-10-28T10:00:00.000Z";
        String second = "2026-10-28T10:00:10.000Z";
        String third = "2026-10-28T10:00:20.000Z";
        writeRules("[" + rule("a", "b") + "]", first);
        assertEquals("b", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]", first);
        clock.addAndGet(10_000);
        assertEquals("b", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]", second);
        clock.addAndGet(10_000);
        assertEquals("c", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]", third);
        clock.addAndGet(10_000);
        assertEquals("c", rewrite("a"));
        writeRules("[" + rule("a", "d") + "]", third);
        clock.addAndGet(10_000);
        assertEquals("c", rewrite("a"));
    }

    @Test
    public void failedRefreshDoesNotCacheLastModified() throws Exception {
        String first = "2026-10-28T10:00:00.000Z";
        String second = "2026-10-28T10:00:10.000Z";
        writeRules("[" + rule("a", "b") + "]", first);
        assertEquals("b", rewrite("a"));
        writeRules("{broken", second);
        clock.addAndGet(10_000);
        assertEquals("b", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]", second);
        clock.addAndGet(10_000);
        assertEquals("c", rewrite("a"));
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("diff.index.optimizer")
                .getChildNode(QueryOptimizer.RULES_FILE).remove();
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        clock.addAndGet(10_000);
        assertEquals("a", rewrite("a"));
        writeRules("[" + rule("a", "d") + "]", second);
        clock.addAndGet(10_000);
        assertEquals("d", rewrite("a"));
    }

    @Test
    public void invalidConfigurationsKeepLastValidRulesAndRecover() throws Exception {
        writeRules("[" + rule("a", "b") + "]");
        assertEquals("b", rewrite("a"));
        String[] invalid = {
                "{broken", "{}", "[{}]", "[{\"find\":\"a\"}]", "[{\"replace\":\"b\"}]",
                "[{\"find\":false,\"replace\":\"b\"}]", "[" + rule("[", "b") + "]",
                "[{\"find\":\"a\",\"replace\":\"b\",\"unknown\":true}]",
                "[{\"find\":\"a\",\"replace\":\"b\",\"options\":{\"unknown\":true}}]",
                "[{\"find\":\"a\",\"replace\":\"b\",\"options\":{\"stop\":\"true\"}}]",
                "[] trailing",
                "[" + String.join(",", Collections.nCopies(QueryOptimizer.MAX_RULES + 1, rule("a", "b"))) + "]",
                " ".repeat(QueryOptimizer.MAX_CONFIG_BYTES + 1)
        };
        for (String json : invalid) {
            writeRules(json);
            clock.addAndGet(10_000);
            assertEquals(json.substring(0, Math.min(json.length(), 100)), "b", rewrite("a"));
        }
        writeRules("[" + rule("a", "c") + "]");
        clock.addAndGet(10_000);
        assertEquals("c", rewrite("a"));
    }

    @Test
    public void missingDataAndReadFailuresKeepLastValidRules() throws Exception {
        writeRules("[" + rule("a", "b") + "]");
        assertEquals("b", rewrite("a"));
        NodeBuilder root = store.getRoot().builder();
        root.getChildNode("oak:index").getChildNode("diff.index.optimizer")
                .getChildNode(QueryOptimizer.RULES_FILE).getChildNode("jcr:content").removeProperty("jcr:data");
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        clock.addAndGet(10_000);
        assertEquals("b", rewrite("a"));

        NodeStore failingStore = mock(NodeStore.class);
        when(failingStore.getRoot()).thenThrow(new IllegalStateException("Unavailable"));
        optimizer.init(failingStore);
        clock.addAndGet(10_000);
        assertEquals("b", rewrite("a"));

        Blob blob = mock(Blob.class);
        when(blob.getNewStream()).thenReturn(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Unavailable blob");
            }
        });
        PropertyState data = BinaryPropertyState.binaryProperty("jcr:data", blob);
        root = store.getRoot().builder();
        root.child("oak:index").child("diff.index.optimizer").child(QueryOptimizer.RULES_FILE)
                .child("jcr:content").setProperty(data);
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        optimizer.init(store);
        clock.addAndGet(10_000);
        assertEquals("b", rewrite("a"));
    }

    @Test
    public void invalidReplacementAndExpansionLimit() throws Exception {
        writeRules("[" + rule("a", "$1") + "]");
        assertThrows(ParseException.class, () -> rewrite("a"));
        writeRules("[" + rule("a", "$") + "]");
        clock.addAndGet(10_000);
        assertThrows(ParseException.class, () -> rewrite("a"));
        writeRules("[" + rule("a", "bbbb") + "]");
        clock.addAndGet(10_000);
        assertThrows(ParseException.class, () -> optimizer.rewrite("a", "xpath", 3));
        assertThrows(ParseException.class, () -> optimizer.rewrite("axx", "xpath", 5));
        assertEquals("bbbb", optimizer.rewrite("a", "xpath", 4));
    }

    @Test
    public void concurrentRefreshUsesPreviousSnapshot() throws Exception {
        writeRules("[" + rule("a", "b") + "]");
        assertEquals("b", rewrite("a"));
        writeRules("[" + rule("a", "c") + "]");
        NodeStore blockingStore = mock(NodeStore.class);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(blockingStore.getRoot()).thenAnswer(invocation -> {
            reading.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return store.getRoot();
        });
        optimizer.init(blockingStore);
        clock.addAndGet(10_000);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> refreshed = executor.submit(() -> rewrite("a"));
            assertTrue(reading.await(10, TimeUnit.SECONDS));
            assertEquals("b", rewrite("a"));
            release.countDown();
            assertEquals("c", refreshed.get(10, TimeUnit.SECONDS));
            assertEquals("c", rewrite("a"));
            verify(blockingStore, times(1)).getRoot();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private String rewrite(String statement) throws ParseException {
        return optimizer.rewrite(statement, "JCR-SQL2", 10_000);
    }

    static String rule(String find, String replace) {
        return new JsopBuilder().object().key("find").value(find)
                .key("replace").value(replace).endObject().toString();
    }

    private void writeRules(String json) throws Exception {
        writeRules(json, null);
    }

    private void writeRules(String json, String lastModified) throws Exception {
        NodeBuilder root = store.getRoot().builder();
        NodeBuilder content = root.child("oak:index").child("diff.index.optimizer")
                .child(QueryOptimizer.RULES_FILE).child("jcr:content");
        content.setProperty(BinaryPropertyState.binaryProperty("jcr:data", json.getBytes(StandardCharsets.UTF_8)));
        if (lastModified != null) {
            content.setProperty("jcr:lastModified", lastModified, Type.DATE);
        }
        store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
