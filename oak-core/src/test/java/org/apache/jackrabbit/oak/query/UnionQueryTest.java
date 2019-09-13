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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UnionQueryTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new OpenSecurityProvider())
                .with(new InitialContent())
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

        root.commit();
    }

    @After
    public void after() throws Exception {
        // Remove test tree
        root.getTree("/UnionQueryTest").remove();
        root.commit();
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

        List<ResultRow> rows = Lists.newArrayList(result.getRows());
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

        List<ResultRow> rows = Lists.newArrayList(result.getRows());
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

        List<ResultRow> rows = Lists.newArrayList(result.getRows());
        assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr: result.getRows()) {
            assertEquals(rr.getPath(), expected[i++]);
        }
    }
}
