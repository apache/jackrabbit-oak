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
import java.util.Collections;
import java.util.Iterator;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Test;

public class ExplainResultTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak(new MemoryNodeStore(InitialContentHelper.INITIAL_CONTENT))
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexProvider())
                .with(new PropertyIndexEditorProvider()).createContentRepository();
    }

    @Test
    public void test_explain_xpath() throws Exception {
        test("explain_result.txt");
        final String xpath = "/jcr:root/test//*";
        Result result = executeQuery(xpath, "xpath", Collections.emptyMap());
        int count = 0;
        for (Iterator<? extends ResultRow> rows = result.getRows().iterator(); rows.hasNext(); ) {
            rows.next();
            count = count + 1;
        }

        assertEquals("should exist 2 nodes", 2, count);

        Result explainResult = executeQuery("explain " + xpath,
                "xpath", Collections.emptyMap());
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
        assertTrue("'statement' should begin with 'select'", explainedStatement.startsWith("select"));
        assertTrue("statement should contain original xpath with prefix 'xpath: '",
                explainedStatement.contains("xpath: " + xpath));
    }

    @Test
    public void test_explain_sql1() throws Exception {
        test("explain_result.txt");
        final String sql1 = "select [jcr:path] from [nt:base] as a where isdescendantnode(a, '/test')";
        Result result = executeQuery(sql1, "sql", Collections.emptyMap());
        int count = 0;
        for (ResultRow row : result.getRows()) {
            count = count + 1;
        }

        assertEquals("should exist 2 nodes", 2, count);

        Result explainResult = executeQuery("explain " + sql1,
                "sql", Collections.emptyMap());
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
        assertTrue("'statement' should begin with 'select'", explainedStatement.startsWith("select"));
        assertEquals("explained statement should be same as original, without 'explain'",
                sql1, explainedStatement);
    }

    @Test
    public void test_explain_sql2() throws Exception {
        test("explain_result.txt");
        final String sql2 = "select [jcr:path] from [nt:base] as a where isdescendantnode(a, '/test')";
        Result result = executeQuery(sql2, "JCR-SQL2", Collections.emptyMap());
        int count = 0;
        for (ResultRow row : result.getRows()) {
            count = count + 1;
        }

        assertEquals("should exist 2 nodes", 2, count);

        Result explainResult = executeQuery("explain " + sql2,
                "JCR-SQL2", Collections.emptyMap());
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
        assertTrue("'statement' should begin with 'select'", explainedStatement.startsWith("select"));
        assertEquals("explained statement should be same as original, without 'explain'",
                sql2, explainedStatement);
    }
}
