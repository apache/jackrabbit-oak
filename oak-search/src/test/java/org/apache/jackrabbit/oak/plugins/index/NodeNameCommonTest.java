/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Common test suite for {@code LOCALNAME()} query support backed by
 * {@code indexNodeName=true} on the index definition.
 *
 * <p>Concrete subclasses wire up the specific index backend via
 * {@link #createJcrRepository()} (inherited from {@link AbstractJcrTest})
 * and expose {@link #indexOptions} / {@link #repositoryOptionsUtil}.</p>
 */
public abstract class NodeNameCommonTest extends AbstractJcrTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Before
    public void createIndex() throws RepositoryException {
        IndexDefinitionBuilder builder = indexOptions.createIndex(
                indexOptions.createIndexDefinitionBuilder(), false);
        builder.noAsync();
        builder.indexRule(JcrConstants.NT_BASE).indexNodeName();
        indexOptions.setIndex(adminSession, "nodeName", builder);
    }

    @Test
    public void localNameEquality() throws Exception {
        Node root = adminSession.getRootNode();
        root.addNode("foo");
        root.addNode("camelCase");
        root.addNode("test").addNode("bar");
        adminSession.save();

        assertEventually(() -> {
            try {
                QueryManager qm = adminSession.getWorkspace().getQueryManager();
                assertEquals(List.of("/foo"),
                        paths(qm, "select [jcr:path] from [nt:base] where LOCALNAME() = 'foo'"));
                assertEquals(List.of("/test/bar"),
                        paths(qm, "select [jcr:path] from [nt:base] where LOCALNAME() = 'bar'"));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void localNameLike() throws Exception {
        Node root = adminSession.getRootNode();
        root.addNode("foobar");
        root.addNode("camelCase");
        adminSession.save();

        assertEventually(() -> {
            try {
                QueryManager qm = adminSession.getWorkspace().getQueryManager();
                assertEquals(List.of("/foobar"),
                        paths(qm, "select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'foo%'"));
                assertEquals(List.of("/camelCase"),
                        paths(qm, "select [jcr:path] from [nt:base] where LOCALNAME() LIKE 'camel%'"));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void localNameNoMatch() throws Exception {
        Node root = adminSession.getRootNode();
        root.addNode("alpha");
        adminSession.save();

        assertEventually(() -> {
            try {
                QueryManager qm = adminSession.getWorkspace().getQueryManager();
                assertEquals(List.of(),
                        paths(qm, "select [jcr:path] from [nt:base] where LOCALNAME() = 'nonexistent'"));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    private static List<String> paths(QueryManager qm, String sql) throws RepositoryException {
        QueryResult result = qm.createQuery(sql, Query.JCR_SQL2).execute();
        RowIterator rows = result.getRows();
        List<String> paths = new ArrayList<>();
        while (rows.hasNext()) {
            paths.add(rows.nextRow().getPath());
        }
        paths.sort(String::compareTo);
        return paths;
    }
}
