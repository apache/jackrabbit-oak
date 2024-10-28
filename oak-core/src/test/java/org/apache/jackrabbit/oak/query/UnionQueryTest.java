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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.GlobalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.ast.ColumnImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.DescendantNodeImpl;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Order;
import org.apache.jackrabbit.oak.query.ast.OrderingImpl;
import org.apache.jackrabbit.oak.query.ast.PropertyValueImpl;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.ast.SourceImpl;
import org.apache.jackrabbit.oak.query.stats.QueryStatsData;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class UnionQueryTest extends AbstractQueryTest {

    MemoryNodeStore store;
    QueryEngineSettings qeSettings;

    @Override
    protected ContentRepository createRepository() {
        store = new MemoryNodeStore();
        qeSettings = new QueryEngineSettings();

        return new Oak(store)
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
                .with(qeSettings)
                .createContentRepository();
    }

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        // Create tree for this test
        Tree t = root.getTree("/").addChild("UnionQueryTest");

        // Create tree /a/b/c/d/e
        t = t.addChild("a");
        t = t.addChild("b");
        t = t.addChild("c");
        t = t.addChild("d");
        t = t.addChild("e");

        Tree t2 = root.getTree("/").addChild("UnionQueryTest2");
        Tree t3 = t2.addChild("a");

        root.commit();
    }

    @After
    public void after() throws Exception {
        // Remove test tree
        root.getTree("/UnionQueryTest").remove();
        root.commit();
    }

    @Test
    public void testMergeSortedVsConcat() throws  Exception {
        String left = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest2')";
        String right = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest/a')";
        String order = "ORDER BY [jcr:path]";
        String union = String.format("%s UNION %s %s", left, right, order);
        final int limit = 10;
        final int offset = 0;
        // Execute query with ORDER BY clause - This should use mergeSorted and the final result should be sorted across both the subqueries.
        String[] expected = {
                "/UnionQueryTest/a/b",
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e",
                "/UnionQueryTest2/a"
        };

        Result result = qe.executeQuery(union, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);

        }

        // Now we execute the same union query without the order by clause. Expectation is the subqueries results should simply be concatenated without
        // sorting of the overall result
        String[] expected2 = {
                "/UnionQueryTest2/a",
                "/UnionQueryTest/a/b",
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e"
        };

        union = String.format("%s UNION %s", left, right);

        result = qe.executeQuery(union, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);
         i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected2[i++]);
        }

    }
    // TODO - Write a similar test that might fail with guava's merge sort on some conditions
    @Test
    public void testSortWithOneSubquerySortedByIndexAndOtherNot() throws Exception {

        String left  = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest2')";
        String right  = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest/a')";

        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(store.getRoot());
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo("nt:base");
        SourceImpl sImpl = new SelectorImpl(type, "a");
        SourceImpl sImpl2 = new SelectorImpl(type, "a");
        SourceImpl sImpl3 = new SelectorImpl(type, "a");

        QueryImpl qImplLeft = createQuery(left, new DescendantNodeImpl("a","/UnionQueryTest2"), sImpl);
        qImplLeft.setExecutionContext(((QueryEngineImpl)root.getQueryEngine()).getExecutionContext());
        QueryImpl qImplRight = createQuery(right, new DescendantNodeImpl("a","/UnionQueryTest/a"), sImpl2);
        qImplRight.setExecutionContext(((QueryEngineImpl)root.getQueryEngine()).getExecutionContext());

        PropertyValueImpl propValImpl = new PropertyValueImpl("a", "jcr:path");
        propValImpl.bindSelector(sImpl);

        PropertyValueImpl propValImpl2 = new PropertyValueImpl("a", "jcr:path");
        propValImpl2.bindSelector(sImpl2);

        PropertyValueImpl propValImpl3 = new PropertyValueImpl("a", "jcr:path");
        propValImpl2.bindSelector(sImpl3);

        /// One subquery sorted by index and the other not and orderBY != null in UnionQueryImpl
        QueryImpl spyLeft = Mockito.spy(qImplLeft);
        QueryImpl spyRight = Mockito.spy(qImplRight);
        Mockito.doReturn(true).when(spyLeft).isSortedByIndex();
        Mockito.doReturn(false).when(spyRight).isSortedByIndex();

        spyLeft.setOrderings(new OrderingImpl[] {new OrderingImpl(propValImpl, Order.DESCENDING)});
        spyRight.setOrderings(new OrderingImpl[] {new OrderingImpl(propValImpl2, Order.DESCENDING)});
        UnionQueryImpl unionImpl = new UnionQueryImpl(true, spyLeft, spyRight, new QueryEngineSettings());
        unionImpl.setOrderings(new OrderingImpl[] {new OrderingImpl(propValImpl3, Order.ASCENDING)});

        unionImpl.init();

        // Execute query with ORDER BY clause - This should use mergeSorted and the final result should be sorted across both the subqueries.
        String[] expected = {
                "/UnionQueryTest/a/b",
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e",
                "/UnionQueryTest2/a"
        };

        Result result = unionImpl.executeQuery();

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(expected[i++], rr.getPath());
        }

    }

    @Test
    public void testOrderLimitOffset() throws Exception {
        String left = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        String right = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        String order = "ORDER BY [jcr:path]";
        String union = String.format("%s UNION %s %s", left, right, order);

        final int limit = 3;
        final int offset = 2;

        String[] expected = {
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e"
        };

        Result result = qe.executeQuery(union, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

        List<ResultRow> rows = CollectionUtils.toList(result.getRows());
        assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);
        }
    }

    @Test
    public void testOrderLimitOption() throws Exception {
        String left = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        String right = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        String order = "ORDER BY [jcr:path]";
        String union = String.format("%s UNION %s %s OPTION(LIMIT 3, OFFSET 2)", left, right, order);

        String[] expected = {
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e"
        };

        Result result = qe.executeQuery(union, QueryEngineImpl.SQL2, Optional.empty(), Optional.empty(),
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

        List<ResultRow> rows = CollectionUtils.toList(result.getRows());
        assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);
        }
    }

    @Test
    public void testExplainStatement() throws Exception {
        final String left = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        final String right = "SELECT [jcr:path] FROM [nt:base] AS a WHERE ISDESCENDANTNODE(a, '/UnionQueryTest')";
        final String order = "ORDER BY [jcr:path]";
        final String union = String.format("%s UNION %s %s", left, right, order);
        final String explainUnion = "explain " + union;

        Result explainResult = executeQuery(explainUnion, QueryEngineImpl.SQL2, QueryEngine.NO_BINDINGS);

        int explainCount = 0;
        ResultRow explainRow = null;
        for (ResultRow row : explainResult.getRows()) {
            if (explainCount == 0) {
                explainRow = row;
            }
            explainCount = explainCount + 1;
        }

        assertEquals("should exist 1 result", 1, explainCount);
        assertNotNull("explain row should not be null", explainRow);

        assertTrue("result should have 'plan' column",
                Arrays.asList(explainResult.getColumnNames()).contains("plan"));
        assertTrue("result should have 'statement' column",
                Arrays.asList(explainResult.getColumnNames()).contains("statement"));

        final String explainedStatement = explainRow.getValue("statement").getValue(Type.STRING);

        assertTrue("'statement' should begin with 'select': " + explainedStatement,
                explainedStatement.startsWith("SELECT"));
        assertTrue("'statement' should contain ' UNION ': " + explainedStatement,
                explainedStatement.contains(" UNION "));

        final int limit = 3;
        final int offset = 2;

        String[] expected = {
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e"
        };

        Result result = qe.executeQuery(explainedStatement, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

        List<ResultRow> rows = CollectionUtils.toList(result.getRows());
        assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);
        }
    }

    @Test
    public void testExplainStatementXPath() throws Exception {
        final String xpath = "/jcr:root/UnionQueryTest//(element(*, nt:base) | element(*, nt:folder)) order by jcr:path";
        final String explainUnion = "explain " + xpath;

        Result explainResult = executeQuery(explainUnion, QueryEngineImpl.XPATH, QueryEngine.NO_BINDINGS);

        int explainCount = 0;
        ResultRow explainRow = null;
        for (ResultRow row : explainResult.getRows()) {
            if (explainCount == 0) {
                explainRow = row;
            }
            explainCount = explainCount + 1;
        }

        assertEquals("should exist 1 result", 1, explainCount);
        assertNotNull("explain row should not be null", explainRow);

        assertTrue("result should have 'plan' column",
                Arrays.asList(explainResult.getColumnNames()).contains("plan"));
        assertTrue("result should have 'statement' column",
                Arrays.asList(explainResult.getColumnNames()).contains("statement"));

        final String explainedStatement = explainRow.getValue("statement").getValue(Type.STRING);
        assertTrue("'statement' should begin with 'select': " + explainedStatement,
                explainedStatement.startsWith("select"));
        assertTrue("'statement' should contain ' union ': " + explainedStatement,
                explainedStatement.contains(" union "));

        final int limit = 3;
        final int offset = 2;

        String[] expected = {
                "/UnionQueryTest/a/b/c",
                "/UnionQueryTest/a/b/c/d",
                "/UnionQueryTest/a/b/c/d/e"
        };

        Result result = qe.executeQuery(explainedStatement, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

        List<ResultRow> rows = CollectionUtils.toList(result.getRows());
        assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);
        }
    }

    private QueryImpl createQuery (String statement, ConstraintImpl c, SourceImpl sImpl) throws Exception {

        NamePathMapper namePathMapper = new NamePathMapperImpl(new GlobalNameMapper(root));
        return new QueryImpl(statement, sImpl, c,
                new ColumnImpl[]{new ColumnImpl("a", "jcr:path", "jcr:path")}, namePathMapper, qeSettings, new QueryStatsData("", "").new QueryExecutionStats());

    }
}
