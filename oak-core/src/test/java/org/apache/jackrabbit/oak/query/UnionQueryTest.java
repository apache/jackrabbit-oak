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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

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
}
