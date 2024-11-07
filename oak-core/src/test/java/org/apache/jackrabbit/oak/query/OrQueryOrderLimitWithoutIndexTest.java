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

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class OrQueryOrderLimitWithoutIndexTest extends AbstractQueryTest {

    private final static String[] BASE_NODE_PATHS = {"UnionQueryTest", "UnionQueryTest1"};

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
        createTestContent(5);
    }

    @After
    public void after() throws Exception {
        // Remove test tree
        deleteTestContent();
    }

    @Test
    public void testOrderLimitOffset() throws Exception {
        final int limit = 8;
        final int offset = 0;
        String query = "SELECT idn1.* FROM [nt:base] as idn1 " +
                "WHERE ISDESCENDANTNODE([/UnionQueryTest]) " +
                "OR " +
                "ISDESCENDANTNODE([/UnionQueryTest1])" +
                " ORDER BY idn1.[x] ASC";

        String[] expected = {
                "/UnionQueryTest1/node0",
                "/UnionQueryTest/node0",
                "/UnionQueryTest1/node0/node1",
                "/UnionQueryTest/node0/node1",
                "/UnionQueryTest1/node0/node1/node2",
                "/UnionQueryTest/node0/node1/node2",
                "/UnionQueryTest1/node0/node1/node2/node3",
                "/UnionQueryTest/node0/node1/node2/node3"
        };

        Result result = qe.executeQuery(query, QueryEngineImpl.SQL2, limit, offset,
                QueryEngine.NO_BINDINGS, QueryEngine.NO_MAPPINGS);

        List<ResultRow> rows = CollectionUtils.toList(result.getRows());
        Assert.assertEquals(expected.length, rows.size());

        int i = 0;
        for (ResultRow rr : result.getRows()) {
            Assert.assertEquals(rr.getPath(), expected[i++]);
        }
    }

    private void createTestContent(int numberOfNodes) throws CommitFailedException {
        for (String baseNodePath : BASE_NODE_PATHS) {
            Tree t = root.getTree("/").addChild(baseNodePath);
            for (int i = 0; i < numberOfNodes; i++) {
                t = t.addChild("node" + i);
                t.setProperty("x", "" + i);
            }
            root.commit();
        }
    }

    private void deleteTestContent() throws CommitFailedException {
        for (String baseNodePath : BASE_NODE_PATHS) {
            root.getTree("/" + baseNodePath).remove();
            root.commit();
        }
    }
}
