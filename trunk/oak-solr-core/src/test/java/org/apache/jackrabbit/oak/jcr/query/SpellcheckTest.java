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

import com.google.common.collect.Lists;
import org.apache.jackrabbit.core.query.AbstractQueryTest;

import java.util.List;

/**
 * Tests the spellcheck support.
 */
public class SpellcheckTest extends AbstractQueryTest {

    public void testSpellcheckSql() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "hello hello hello alt");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "hold");
        session.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE [jcr:path] = '/' AND SPELLCHECK('helo')";
        Query q = qm.createQuery(sql, Query.SQL);
        List<String> result = getResult(q.execute(), "rep:spellcheck()");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("[hello, hold]", result.toString());
    }

    public void testSpellcheckXPath() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "hello hello hello alt");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "hold");
        session.save();

        String xpath = "/jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())";
        Query q = qm.createQuery(xpath, Query.XPATH);
        List<String> result = getResult(q.execute(), "rep:spellcheck()");
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("[hello, hold]", result.toString());
    }

    public void testSpellcheckMultipleWords() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("jcr:title", "it is always a good idea to go visiting ontario");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("jcr:title", "ontario is a nice place to live in");
        Node n3 = testRootNode.addNode("node3");
        n2.setProperty("jcr:title", "I flied to ontario for voting for the major polls");
        Node n4 = testRootNode.addNode("node4");
        n2.setProperty("jcr:title", "I will go voting in ontario, I always voted since I've been allowed to");
        session.save();

        String xpath = "/jcr:root[rep:spellcheck('votin in ontari')]/(rep:spellcheck())";
        Query q = qm.createQuery(xpath, Query.XPATH);
        List<String> result = getResult(q.execute(), "rep:spellcheck()");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("voting in ontario", result.get(0));
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