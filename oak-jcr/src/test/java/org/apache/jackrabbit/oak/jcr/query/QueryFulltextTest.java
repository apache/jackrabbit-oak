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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Test;

/**
 * Tests the fulltext index.
 */
public class QueryFulltextTest extends AbstractRepositoryTest {

    public QueryFulltextTest(NodeStoreFixture fixture) {
        super(fixture);
    }
    
    @Test
    public void excerpt() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello world");
        n1.setProperty("desc", "description");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "Hello World");
        n2.setProperty("desc", "Description");
        session.save();

        Query q;
        RowIterator it;
        Row row;
        String s;
        
        String xpath = "//*[jcr:contains(., 'hello')]/rep:excerpt(.) order by @jcr:path";
        
        q = qm.createQuery(xpath, "xpath");
        it = q.execute().getRows();
        row = it.nextRow();
        String path = row.getPath();
        s = row.getValue("rep:excerpt(.)").getString();
        assertTrue(path + ":" + s + " (1)", s.indexOf("<strong>hello</strong> world") >= 0);
        assertTrue(path + ":" + s + " (2)", s.indexOf("description") >= 0);
        row = it.nextRow();
        path = row.getPath();
        s = row.getValue("rep:excerpt(.)").getString();
        // TODO is this expected?
        assertTrue(path + ":" + s + " (3)", s.indexOf("Hello World") >= 0);
        assertTrue(path + ":" + s + " (4)", s.indexOf("Description") >= 0);
        
        xpath = "//*[jcr:contains(., 'hello')]/rep:excerpt(.) order by @jcr:path";

        q = qm.createQuery(xpath, "xpath");
        it = q.execute().getRows();
        row = it.nextRow();
        path = row.getPath();
        s = row.getValue("rep:excerpt(text)").getString();
        assertTrue(path + ":" + s + " (5)", s.indexOf("<strong>hello</strong> world") >= 0);
        assertTrue(path + ":" + s + " (6)", s.indexOf("description") < 0);
        row = it.nextRow();
        path = row.getPath();
        s = row.getValue("rep:excerpt(text)").getString();
        // TODO is this expected?
        assertTrue(path + ":" + s + " (7)", s.indexOf("Hello World") >= 0);
        assertTrue(path + ":" + s + " (8)", s.indexOf("Description") < 0);
    }
    
    @Test
    public void fulltextOrWithinText() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "hello hallo");
        session.save();
       
        String sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([text], 'hello OR hallo') order by [jcr:path]";
        
        Query q;
        
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);

        assertEquals("[nt:base] as [nt:base] /* traverse \"*\" " + 
                "where contains([nt:base].[text], 'hello OR hallo') */",
                getResult(q.execute(), "plan"));
        
        // verify the result
        // uppercase "OR" mean logical "or"
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("/testroot/node1, /testroot/node2, /testroot/node3",
                getResult(q.execute(), "path"));

        // lowercase "or" mean search for the term "or"
        sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([text], 'hello or hallo') order by [jcr:path]";
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("", 
                getResult(q.execute(), "path"));

    }
    
    static String getResult(QueryResult result, String propertyName) throws RepositoryException {
        StringBuilder buff = new StringBuilder();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            if (buff.length() > 0) {
                buff.append(", ");
            }
            buff.append(it.nextRow().getValue(propertyName).getString());
        }
        return buff.toString();
    }
    
}
