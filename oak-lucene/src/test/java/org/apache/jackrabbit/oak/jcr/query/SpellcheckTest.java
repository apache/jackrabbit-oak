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

import org.apache.jackrabbit.core.query.AbstractQueryTest;

/**
 * Tests the spellcheck support.
 */
public class SpellcheckTest extends AbstractQueryTest {

    public void testSpellcheckSql() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello hello hello alt");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hello");
        session.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE [jcr:path] = '/' AND SPELLCHECK('helo')";
        Query q = qm.createQuery(sql, Query.SQL);
        String result = getResult(q.execute(), "rep:spellcheck()");
        assertNotNull(result);
        assertEquals("[hello, hold]", result);
    }

    public void testSpellcheckXPath() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello hello hello alt");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hello");
        session.save();

        String xpath = "/jcr:root[rep:spellcheck('helo')]/(rep:spellcheck())";
        Query q = qm.createQuery(xpath, Query.XPATH);
        String result = getResult(q.execute(), "rep:spellcheck()");
        assertNotNull(result);
        assertEquals("[hello, hold]", result);
    }

    static String getResult(QueryResult result, String propertyName) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            Row row = it.nextRow();
            buff.append(row.getValue(propertyName).getString());
        }
        return buff.toString();
    }

}