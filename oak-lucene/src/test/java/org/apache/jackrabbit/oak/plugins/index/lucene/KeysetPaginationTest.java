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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
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
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reproduces a bug in keyset (seek-based) pagination: paging through query results
 * ordered by a property that is served by an asynchronous index can silently skip
 * entries.
 * <p>
 * The pagination cursor for the next page is normally derived from the current
 * (live) value of the last row returned by the previous page. If that value changes
 * concurrently with paging - before the asynchronous index has caught up with the
 * change - the cursor can jump far ahead in the keyspace, skipping every entry in
 * between, even entries that were never modified.
 * <p>
 * The solution is to page on an immutable key such as {@code path()}, and, if only
 * a subset of the nodes is needed (for example nodes with a "sling:alias"
 * property), on a conditional path such as
 * {@code if(exists([sling:alias]), path(), null)}, which keeps the index sparse.
 */
public class KeysetPaginationTest {

    private static final String KEY_INDEX_PATH = "/oak:index/keyIndex";
    private static final String PATH_INDEX_PATH = "/oak:index/pathIndex";
    private static final String ALIAS_PATH_INDEX_PATH = "/oak:index/aliasPathIndex";
    private static final long PAGE_SIZE = 2;
    private static final String INDEX_TAG = "keyset";
    private static final String CONDITIONAL_PATH_QUERY =
            "select [jcr:path] from [nt:base] " +
            "where if(exists([alias]), path(), null) > $lastPath " +
            "order by if(exists([alias]), path(), null) " +
            "option(traversal fail, index tag [" + INDEX_TAG + "], limit " + PAGE_SIZE + ")";

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
    }

    @After
    public void tearDown() {
        luceneIndexProvider.close();
    }

    @Test
    public void keysetPaginationCanSkipEntries() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.async("async");
        defnb.indexRule("nt:base").property("key").propertyIndex().ordered();
        defnb.build(createPath(KEY_INDEX_PATH));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("key", "a");
        test.addChild("b").setProperty("key", "b");
        test.addChild("c").setProperty("key", "c");
        root.commit();

        // Bring the index fully up to date: it now reflects key = a, b, c.
        runAsyncIndex();

        // The "key" property of /test/b is changed, but the (asynchronous) index is
        // not updated yet - it still reflects the old value "b".
        root.getTree("/test/b").setProperty("key", "z");
        root.commit();

        // Page 1 is served from the still-stale index, which orders entries as a, b, c.
        List<String> page1 = queryPage("");
        assertEquals(Arrays.asList("/test/a", "/test/b"), page1);

        // The application derives the cursor for the next page from the *current*
        // (live) value of the last returned node, not from the stale index entry.
        String lastPath = page1.get(page1.size() - 1);
        String cursor = root.getTree(lastPath).getProperty("key").getValue(Type.STRING);
        assertEquals("z", cursor);

        // Page 2 queries "key >= 'z'" against the still-stale index (a, b, c), which
        // matches nothing - even though /test/c ("c") was never modified. It is
        // skipped entirely, and no later page will ever return it.
        List<String> page2 = queryPage(cursor);
        assertTrue("expected /test/c to be skipped due to the pagination bug, but got " + page2,
                page2.isEmpty());
    }

    /**
     * Same scenario as {@link #keysetPaginationCanSkipEntries}, but paging is keyed
     * on {@code path()} (via a function-based index) instead of on the mutable "key"
     * property. The cursor for the next page is now the path of the last returned
     * row, which cannot be changed by a concurrent, unrelated property update - so
     * no entry is skipped even though the index is equally stale.
     */
    @Test
    public void keysetPaginationByPathDoesNotSkipEntries() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.async("async");
        defnb.indexRule("nt:base").property("byPath", null).propertyIndex().ordered().function("path()");
        defnb.build(createPath(PATH_INDEX_PATH));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("key", "a");
        test.addChild("b").setProperty("key", "b");
        test.addChild("c").setProperty("key", "c");
        root.commit();

        // Bring the index fully up to date.
        runAsyncIndex();

        // The "key" property of /test/b changes, exactly as before, but this time it
        // has no bearing on the pagination cursor.
        root.getTree("/test/b").setProperty("key", "z");
        root.commit();

        List<String> page1 = queryPageByPath("");
        assertEquals(Arrays.asList("/test/a", "/test/b"), page1);

        // The cursor is the path of the last row, which is stable regardless of any
        // concurrent property change.
        String cursor = page1.get(page1.size() - 1);

        List<String> page2 = queryPageByPath(cursor);
        assertEquals(Collections.singletonList("/test/c"), page2);
    }

    /**
     * Like {@link #keysetPaginationByPathDoesNotSkipEntries}, but only for nodes
     * that have an "alias" property (as for "sling:alias"). An index on
     * {@code path()} for all nodes would be far too large, so the function
     * {@code if(exists([alias]), path(), null)} is indexed instead: it is the path
     * for nodes with an "alias" property, and null (not indexed) for all others,
     * keeping the index sparse. Paging is keyed on this function, so the cursor is
     * again the (immutable) path of the last returned row.
     */
    @Test
    public void keysetPaginationByConditionalPathDoesNotSkipEntries() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.async("async");
        defnb.indexRule("nt:base").property("aliasPath", null).propertyIndex().ordered()
                .function("if(exists([alias]), path(), null)");
        defnb.tags(INDEX_TAG);
        defnb.build(createPath(ALIAS_PATH_INDEX_PATH));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("alias", "a");
        test.addChild("b").setProperty("alias", "b");
        test.addChild("c");
        test.addChild("d").setProperty("alias", "d");
        root.commit();

        runAsyncIndex();

        // The "alias" property of /test/b changes, but the index is not updated yet.
        root.getTree("/test/b").setProperty("alias", "z");
        root.commit();

        String plan = explain(CONDITIONAL_PATH_QUERY.replace("$lastPath", "''"));
        assertTrue("expected the query to be served by " + ALIAS_PATH_INDEX_PATH + ", but got plan: " + plan,
                plan.contains(ALIAS_PATH_INDEX_PATH));

        List<String> page1 = queryPageByConditionalPath("");
        assertEquals(Arrays.asList("/test/a", "/test/b"), page1);

        // /test/c has no "alias" property, so it is not part of the result.
        String cursor = page1.get(page1.size() - 1);
        List<String> page2 = queryPageByConditionalPath(cursor);
        assertEquals(Collections.singletonList("/test/d"), page2);
    }

    /**
     * Explores what "order by" does against a stale asynchronous index, without any
     * keyset pagination involved: does the query re-sort by the current value, or
     * does it keep the (possibly outdated) order the index happens to have?
     * <p>
     * Observed result: no re-sorting happens. The row for the changed node stays at
     * the *position* the index assigned it (i.e. where "b" used to sort), while the
     * *value* returned for that row's [key] column is read live and already shows
     * "z" - the query result is neither in index order nor in current-value order,
     * it is a mix of both. This confirms that keyset pagination cannot rely on
     * "order by" being consistent with current property values while the index is
     * catching up, and that the cursor for the next page must be chosen carefully
     * (see {@link #keysetPaginationCanSkipEntries}).
     */
    @Test
    public void orderByOnStaleIndexObservedOrder() throws Exception {
        LuceneIndexDefinitionBuilder defnb = new LuceneIndexDefinitionBuilder();
        defnb.async("async");
        defnb.indexRule("nt:base").property("key").propertyIndex().ordered();
        defnb.build(createPath(KEY_INDEX_PATH));
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("key", "a");
        test.addChild("b").setProperty("key", "b");
        test.addChild("c").setProperty("key", "c");
        test.addChild("d").setProperty("key", "d");
        root.commit();

        // Bring the index fully up to date: it now reflects key = a, b, c, d.
        runAsyncIndex();

        // The "key" property of /test/b is changed, but the index is not updated -
        // it still reflects the old value "b" for that node.
        root.getTree("/test/b").setProperty("key", "z");
        root.commit();

        String plan = explain("select * from [nt:base] where [key] >= '' order by [key]");
        assertTrue("expected the query to be served by " + KEY_INDEX_PATH + ", but got plan: " + plan,
                plan.contains(KEY_INDEX_PATH));

        // Not sorted by current value (which would be [a, c, d, z]), and not the
        // stale index's own key values either (which would be [a, b, c, d]): "z" is
        // returned at the position "b" used to occupy in the stale index.
        List<String> keys = queryOrderedKeys();
        assertEquals(Arrays.asList("a", "z", "c", "d"), keys);
    }

    private String explain(String query) throws ParseException {
        Result result = queryEngine.executeQuery("explain " + query, "JCR-SQL2",
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        return result.getRows().iterator().next().getValue("plan").getValue(Type.STRING);
    }

    private List<String> queryOrderedKeys() throws ParseException {
        Result result = queryEngine.executeQuery(
                "select [jcr:path], [key] from [nt:base] where [key] >= '' order by [key]",
                "JCR-SQL2", QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        List<String> keys = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            keys.add(row.getValue("key").getValue(Type.STRING));
        }
        return keys;
    }

    private List<String> queryPage(String lastKey) throws ParseException {
        Map<String, PropertyValue> bindings = Collections.singletonMap("key", PropertyValues.newString(lastKey));
        Result result = queryEngine.executeQuery(
                "select [jcr:path] from [nt:base] where [key] >= $key order by [key] option(traversal fail)",
                "JCR-SQL2", PAGE_SIZE, 0, bindings, QueryEngine.NO_MAPPINGS);
        List<String> paths = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            paths.add(row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        }
        return paths;
    }

    private List<String> queryPageByPath(String lastPath) throws ParseException {
        Map<String, PropertyValue> bindings = Collections.singletonMap("lastPath", PropertyValues.newString(lastPath));
        Result result = queryEngine.executeQuery(
                "select [jcr:path] from [nt:base] where isdescendantnode('/test') and path() > $lastPath " +
                        "order by path() option(traversal fail)",
                "JCR-SQL2", PAGE_SIZE, 0, bindings, QueryEngine.NO_MAPPINGS);
        List<String> paths = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            paths.add(row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        }
        return paths;
    }

    private List<String> queryPageByConditionalPath(String lastPath) throws ParseException {
        Map<String, PropertyValue> bindings = Collections.singletonMap("lastPath", PropertyValues.newString(lastPath));
        // the limit is set in the query itself, using "option(limit ...)"
        Result result = queryEngine.executeQuery(CONDITIONAL_PATH_QUERY, "JCR-SQL2",
                Optional.empty(), Optional.empty(), bindings, QueryEngine.NO_MAPPINGS);
        List<String> paths = new ArrayList<>();
        for (ResultRow row : result.getRows()) {
            paths.add(row.getValue(QueryConstants.JCR_PATH).getValue(Type.STRING));
        }
        return paths;
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
