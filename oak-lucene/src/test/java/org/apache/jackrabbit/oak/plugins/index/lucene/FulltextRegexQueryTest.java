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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.text.ParseException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Explores LUCENE-9981: a Lucene regex query is parsed into a {@code RegexpQuery} by
 * converting the regex to an automaton ({@code RegExp.toAutomaton()}). For certain
 * pathological regexes (e.g. repeated {@code .*} groups), building/determinizing that
 * automaton can take catastrophic amounts of time and memory.
 * <p>
 * A slash-delimited regex passed to a plain {@code contains(*, '/regex/')} clause does
 * <b>not</b> reach this code: {@code FulltextIndex.rewriteQueryText()} unconditionally
 * escapes the delimiting slashes first, so the term is never recognized as a regex (see
 * {@link #fuzzForEncodingThatReachesGenuineRegexParsing}). A {@code native('lucene', ...)}
 * clause, however, bypasses that escaping and does genuinely reach
 * {@code RegExp.toAutomaton()} (see {@link #nativeLuceneQueryReachesGenuineRegexParsing}
 * and {@link #pathologicalRegexCanCauseCatastrophicSlowdown}).
 * <p>
 * Note oak-lucene bundles its own (very old, Lucene 4.7.2 based, see OAK-10786) copy of
 * the automaton classes, so this needs to be verified against oak-lucene's actual runtime
 * behaviour rather than assumed from the upstream Lucene ticket.
 */
public class FulltextRegexQueryTest {

    private static final String INDEX_PATH = "/oak:index/textIndex";

    private LuceneIndexProvider luceneIndexProvider;
    private Whiteboard whiteboard;
    private QueryEngine queryEngine;
    private Root root;

    @Before
    public void setUp() throws Exception {
        NodeStore nodeStore = new MemoryNodeStore();
        luceneIndexProvider = new LuceneIndexProvider();
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider();

        Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with((QueryIndexProvider) luceneIndexProvider)
                .with((Observer) luceneIndexProvider)
                .with(editorProvider)
                .with(new NodeCounterEditorProvider())
                //Effectively disable async indexing auto run
                //such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

        whiteboard = oak.getWhiteboard();
        ContentRepository repository = oak.createContentRepository();
        ContentSession session = repository.login(null, null);
        root = session.getLatestRoot();
        queryEngine = root.getQueryEngine();

        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.async("async");
        defnb.indexRule("nt:base").property("text").analyzed().nodeScopeIndex();
        defnb.build(createPath(INDEX_PATH));
        root.commit();
    }

    @After
    public void tearDown() {
        luceneIndexProvider.close();
    }

    /**
     * Sanity check: a plain fulltext query is served by the Lucene index (not
     * traversal), so we know a pathological regex would actually reach Lucene's
     * regex-to-automaton code, rather than being short-circuited earlier.
     */
    @Test
    public void containsQueryUsesLuceneIndex() throws Exception {
        root.getTree("/").addChild("test").setProperty("text", "hello world");
        root.commit();
        runAsyncIndex();

        String plan = explain("select * from [nt:base] where contains(*, 'hello')");
        assertTrue("expected the query to be served by " + INDEX_PATH + ", but got plan: " + plan,
                plan.contains(INDEX_PATH));

        assertEquals(1, countRows("select * from [nt:base] where contains(*, 'hello')"));
    }

    /**
     * Unlike {@code contains(*, ...)} (see {@link #fuzzForEncodingThatReachesGenuineRegexParsing}),
     * a {@code native('lucene', '<raw Lucene query>')} clause bypasses
     * {@code FulltextIndex.rewriteQueryText()} entirely (it is parsed directly by Lucene's
     * classic {@code QueryParser}, see {@code LucenePropertyIndex.getLuceneRequest()}), so
     * a slash-delimited regex genuinely reaches {@code RegexpQuery}/{@code RegExp.toAutomaton()}.
     * <p>
     * Two details are required to make this work:
     * <ul>
     *     <li>the function name for a plain Lucene index defaults to "lucene" (see
     *     {@code LuceneIndexDefinition.getDefaultFunctionName()}), not "fulltext" (the
     *     {@code FulltextIndex}/oak-search default) - the query's {@code native(...)} first
     *     argument must match it;</li>
     *     <li>the analyzed property field is actually named {@code full:<property>} (see
     *     {@code FieldNames.ANALYZED_FIELD_PREFIX}), not just {@code <property>}, and the
     *     colon needs escaping so Lucene's classic parser doesn't treat it as the
     *     field:value separator.</li>
     * </ul>
     */
    @Test
    public void nativeLuceneQueryReachesGenuineRegexParsing() throws Exception {
        root.getTree("/").addChild("test").setProperty("text", "hello world");
        root.commit();
        runAsyncIndex();

        // Only matches "hello" under genuine regex semantics (any-char '.', kleene '*'),
        // not under a literal or wildcard-query interpretation of the same characters.
        String query = "select * from [nt:base] where native('lucene', 'full\\:text:/h.*o/')";
        String plan = explain(query);
        assertTrue("expected the query to be served by " + INDEX_PATH + ", but got plan: " + plan,
                plan.contains(INDEX_PATH));
        assertEquals(1, countRows(query));

        // Control: a pattern that cannot match "hello" under any interpretation.
        String controlQuery = "select * from [nt:base] where native('lucene', 'full\\:text:/xyz.*/')";
        assertEquals(0, countRows(controlQuery));
    }

    /**
     * A "contains(*, ...)" query using a slash-delimited regex value unexpectedly returns
     * 0 rows even though "hello" is indexed: {@code FulltextIndex.rewriteQueryText()}
     * (oak-search) unconditionally escapes every slash character (it is in
     * {@code QUERY_OPERATORS}) before handing the text to Lucene's
     * {@code StandardQueryParser}, so the delimiters that are meant to mark a
     * slash-delimited regex term are turned into literal, escaped slashes - the term is
     * no longer recognized as a regex at all, and never reaches
     * {@code RegExp.toAutomaton()}.
     * <p>
     * This fuzzes many ways of wrapping/escaping a discriminating regex payload (which
     * only matches the indexed term "hello" if the dot and star are interpreted with
     * genuine regex semantics - a literal or wildcard-query interpretation of the same
     * characters does not match "hello") with combinations of backslash, slash, single
     * and double quotes, to look for any encoding that survives {@code rewriteQueryText}
     * and still reaches a real {@code RegexpQuery}. Only cheap, bounded candidates are
     * used, so this runs safely (fast, no catastrophic regexes).
     */
    @Test
    public void fuzzForEncodingThatReachesGenuineRegexParsing() throws Exception {
        root.getTree("/").addChild("test").setProperty("text", "hello world");
        root.commit();
        runAsyncIndex();

        // Only matches "hello" if '.' means "any character" and '*' means "zero or more" -
        // i.e. only under genuine regex semantics, not under a literal or wildcard fallback.
        String payload = "h.*o";
        char[] wrapChars = "\\/\"':!&|=".toCharArray();
        Random random = new Random(42);

        String found = null;
        int attempts = 20000;
        for (int attempt = 0; attempt < attempts && found == null; attempt++) {
            String candidate = randomWrap(random, wrapChars, payload);
            String sqlLiteral = candidate.replace("'", "''");
            String query = "select * from [nt:base] where contains(*, '" + sqlLiteral + "')";
            try {
                if (countRows(query) == 1) {
                    found = candidate;
                }
            } catch (Exception e) {
                // Most random wrappings are not valid fulltext/regex syntax - expected,
                // just try another one.
            }
        }

        if (found != null) {
            fail("Found an encoding that reaches genuine regex parsing after escaping: "
                    + found + " - this can be used to reproduce LUCENE-9981");
        }
        // Otherwise: within these attempts, no encoding was found that survives
        // rewriteQueryText's unconditional '/' escaping - consistent with contains(*, ...)
        // not being a viable path to a genuine Lucene RegexpQuery in this code path.
    }

    private static String randomWrap(Random random, char[] wrapChars, String payload) {
        StringBuilder sb = new StringBuilder();
        int prefixLen = random.nextInt(5);
        for (int i = 0; i < prefixLen; i++) {
            sb.append(wrapChars[random.nextInt(wrapChars.length)]);
        }
        sb.append(payload);
        int suffixLen = random.nextInt(5);
        for (int i = 0; i < suffixLen; i++) {
            sb.append(wrapChars[random.nextInt(wrapChars.length)]);
        }
        return sb.toString();
    }

    /**
     * Searches for a pathological regex that causes a catastrophic slowdown when Lucene
     * builds its automaton (LUCENE-9981), via the {@code native('lucene', ...)} path (see
     * {@link #nativeLuceneQueryReachesGenuineRegexParsing}) that genuinely reaches
     * {@code RegExp.toAutomaton()} - unlike {@code contains(*, '/regex/')}, which does not
     * (see {@link #fuzzForEncodingThatReachesGenuineRegexParsing}). This is inherently
     * probabilistic (random regex generation) and can consume large amounts of CPU/memory
     * when it succeeds, so it is not run automatically - remove the {@code @Ignore} to
     * reproduce locally.
     */
    @Ignore("LUCENE-9981: reproduction is probabilistic and can consume large amounts of "
            + "CPU/memory - run manually only")
    @Test
    public void pathologicalRegexCanCauseCatastrophicSlowdown() throws Exception {
        root.getTree("/").addChild("test").setProperty("text", "hello world");
        root.commit();
        runAsyncIndex();

        // Characters that are meaningful in Lucene/Java regex syntax, plus a couple of
        // plain letters, so that a decent fraction of generated strings actually parse
        // as valid regexes instead of immediately failing.
        String alphabet = "abc.*+?{}()|[]^$\\0123456789";
        Random random = new Random();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            for (int attempt = 0; attempt < 5000; attempt++) {
                String regex = randomString(random, alphabet, 20);
                String query = "select * from [nt:base] where native('lucene', 'full\\:text:/"
                        + regex.replace("'", "''") + "/')";

                long start = System.currentTimeMillis();
                Future<Integer> future = executor.submit(() -> countRows(query));
                try {
                    future.get(2, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    long elapsed = System.currentTimeMillis() - start;
                    future.cancel(true);
                    fail("Found a pathological regex causing a catastrophic slowdown after "
                            + elapsed + " ms: /" + regex + "/");
                } catch (Exception e) {
                    // Most random strings are not valid regex syntax (or fail for other
                    // reasons) - that is expected, just try another one.
                }
            }
            // Not finding a pathological case in the given number of attempts can happen,
            // since the search is random; increase the attempt count or alphabet if needed.
        } finally {
            executor.shutdownNow();
        }
    }

    private static String randomString(Random random, String alphabet, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    private int countRows(String query) throws ParseException {
        Result result = queryEngine.executeQuery(query, "JCR-SQL2",
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        int count = 0;
        for (ResultRow row : result.getRows()) {
            count++;
        }
        return count;
    }

    private String explain(String query) throws ParseException {
        Result result = queryEngine.executeQuery("explain " + query, "JCR-SQL2",
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        return result.getRows().iterator().next().getValue("plan").getValue(Type.STRING);
    }

    private void runAsyncIndex() {
        AsyncIndexUpdate async = (AsyncIndexUpdate) WhiteboardUtils.getService(whiteboard,
                Runnable.class, (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate);
        assertNotNull(async);
        async.run();
        if (async.isFailing()) {
            fail("AsyncIndexUpdate failed");
        }
        root.refresh();
    }

    private Tree createPath(String path) {
        Tree base = root.getTree("/");
        for (String name : path.substring(1).split("/")) {
            base = base.hasChild(name) ? base.getChild(name) : base.addChild(name);
        }
        return base;
    }
}
