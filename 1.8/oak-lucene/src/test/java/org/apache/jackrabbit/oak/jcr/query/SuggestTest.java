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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.junit.After;
import org.junit.Before;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGESTION_CONFIG;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_UPDATE_FREQUENCY_MINUTES;

/**
 * Tests the suggest support.
 */
public class SuggestTest extends AbstractQueryTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // change suggester update frequency
        superuser.getNode("/oak:index/luceneGlobal/" + SUGGESTION_CONFIG)
                .setProperty(SUGGEST_UPDATE_FREQUENCY_MINUTES, 0);
        superuser.getNode("/oak:index/luceneGlobal").setProperty(REINDEX_PROPERTY_NAME, true);
    }

    @After
    public void tearDown() throws Exception {
        // reset suggester update frequency
        superuser.getNode("/oak:index/luceneGlobal/" + SUGGESTION_CONFIG)
                .setProperty(SUGGEST_UPDATE_FREQUENCY_MINUTES, 10);

        super.tearDown();
    }

    public void testSuggestSql() throws Exception {
        Session session = superuser;

        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "in 2015 my fox is red, like mike's fox and john's fox");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "in 2015 a red fox is still a fox");
        session.save();

        String sql = "SELECT [rep:suggest()] FROM nt:base WHERE [jcr:path] = '/' AND SUGGEST('in 201')";
        Query q = qm.createQuery(sql, Query.SQL);
        List<String> result = getResult(q.execute(), "rep:suggest()");
        assertNotNull(result);
        assertTrue(result.contains("in 2015 a red fox is still a fox"));
        assertTrue(result.contains("in 2015 my fox is red, like mike's fox and john's fox"));
    }

    public void testSuggestXPath() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "in 2015 my fox is red, like mike's fox and john's fox");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "in 2015 a red fox is still a fox");
        session.save();

        String xpath = "/jcr:root[rep:suggest('in 201')]/(rep:suggest())";
        Query q = qm.createQuery(xpath, Query.XPATH);
        List<String> result = getResult(q.execute(), "rep:suggest()");
        assertNotNull(result);
        assertTrue(result.contains("in 2015 a red fox is still a fox"));
        assertTrue(result.contains("in 2015 my fox is red, like mike's fox and john's fox"));
    }

    public void testSuggestInfix() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "in 2015 my fox is red, like mike's fox and john's fox");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "in 2015 a red fox is still a fox");
        session.save();

        String xpath = "/jcr:root[rep:suggest('like mike')]/(rep:suggest())";
        Query q = qm.createQuery(xpath, Query.XPATH);
        List<String> result = getResult(q.execute(), "rep:suggest()");
        assertNotNull(result);
        assertTrue(result.contains("in 2015 my fox is red, like mike's fox and john's fox"));
    }

    public void testNoSuggestions() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "in 2015 my fox is red, like mike's fox and john's fox");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "in 2015 a red fox is still a fox");
        session.save();

        String sql = "SELECT [rep:suggest()] FROM nt:base WHERE [jcr:path] = '/' AND SUGGEST('blablabla')";
        Query q = qm.createQuery(sql, Query.SQL);
        List<String> results = getResult(q.execute(), "rep:suggest()");
        assertNotNull(results);
        assertEquals(0, results.size());
    }

    static List<String> getResult(QueryResult result, String propertyName) throws RepositoryException {
        List<String> results = Lists.newArrayList();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            Row row = it.nextRow();
            results.add(row.getValue(propertyName).getString());
        }
        return results;
    }

}