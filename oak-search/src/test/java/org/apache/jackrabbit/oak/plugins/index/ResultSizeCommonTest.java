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

import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.junit.After;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the fast (insecure) query result size feature across index backends.
 * Ported from the Lucene-only {@code org.apache.jackrabbit.oak.jcr.query.ResultSizeTest}.
 * This base drives the feature through the {@code oak.fastQuerySize} system property;
 * {@code WhiteboardResultSizeCommonTest} overrides the toggle hooks to drive it through
 * a registered {@code SessionQuerySettingsProvider}.
 */
public abstract class ResultSizeCommonTest extends AbstractJcrTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;
    protected String indexName;

    protected static final int NODE_COUNT = 200;
    private static final int TOLERANCE = 50;

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Override
    protected void initialize() {
        try {
            createIndex();
            createData();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    private void createIndex() throws RepositoryException {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.indexRule("nt:base").property("text").analyzed().nodeScopeIndex();
        indexName = "resultsize_" + UUID.randomUUID();
        indexOptions.setIndex(adminSession, indexName, indexOptions.createIndex(builder, false, "text"));
    }

    private void createData() throws RepositoryException {
        Node testRoot = adminSession.getRootNode().addNode("testroot", "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            testRoot.addNode("node" + i).setProperty("text", "Hello World");
        }
        adminSession.save();
    }

    /** Enable (fast) / disable (secure) direct result counts for subsequent queries. */
    protected void setDirectResultCount(boolean fast) {
        if (fast) {
            System.clearProperty("oak.fastQuerySize");
        } else {
            System.setProperty("oak.fastQuerySize", "false");
        }
    }

    /** Session used to run the size queries. Plain variant reuses the admin session. */
    protected Session querySession() throws RepositoryException {
        return adminSession;
    }

    /** Release a session obtained from {@link #querySession()} (no-op for the reused admin session). */
    protected void releaseQuerySession(Session session) {
    }

    @After
    public void clearFastQuerySizeProperty() {
        System.clearProperty("oak.fastQuerySize");
    }

    @Test
    public void testResultSize() {
        assertEventually(() -> {
            try {
                doTestResultSize(false, NODE_COUNT);      // conjunction
                doTestResultSize(true, 2 * NODE_COUNT);   // union (XPath 'or' sums subquery sizes)
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected void doTestResultSize(boolean union, int expected) throws RepositoryException {
        String xpath = union
                ? "/jcr:root/testroot//*[jcr:contains(@text, 'Hello') or jcr:contains(@text, 'World')]"
                : "/jcr:root/testroot//*[jcr:contains(@text, 'Hello World')]";

        // fast (insecure) case
        setDirectResultCount(true);
        Session fastSession = querySession();
        String fastPaths;
        try {
            QueryManager qm = fastSession.getWorkspace().getQueryManager();
            Query q = qm.createQuery(xpath, "xpath");
            NodeIterator it = q.execute().getNodes();
            long size = it.getSize();
            assertTrue("size: " + size + " expected around " + expected,
                    size > expected - TOLERANCE && size < expected + TOLERANCE);
            StringBuilder buff = new StringBuilder();
            while (it.hasNext()) {
                buff.append(it.nextNode().getPath()).append('\n');
            }
            fastPaths = buff.toString();

            q = qm.createQuery(xpath, "xpath");
            q.setLimit(90);
            assertEquals(90, q.execute().getNodes().getSize());
        } finally {
            releaseQuerySession(fastSession);
        }

        // default (secure) case
        setDirectResultCount(false);
        Session secureSession = querySession();
        try {
            QueryManager qm = secureSession.getWorkspace().getQueryManager();
            Query q = qm.createQuery(xpath, "xpath");
            NodeIterator it = q.execute().getNodes();
            assertEquals(-1, it.getSize());
            StringBuilder buff = new StringBuilder();
            while (it.hasNext()) {
                buff.append(it.nextNode().getPath()).append('\n');
            }
            assertEquals(buff.toString(), fastPaths);
        } finally {
            releaseQuerySession(secureSession);
        }
    }
}
