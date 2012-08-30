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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.util.NoSuchElementException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Test;

/**
 * Tests the query feature.
 */
public class QueryTest extends AbstractRepositoryTest {

    @SuppressWarnings("deprecation")
    @Test
    public void simple() throws RepositoryException {
        Session session = getAdminSession();
        Node hello = session.getRootNode().addNode("hello");
        hello.setProperty("id",  "1");
        hello.setProperty("text",  "hello world");
        session.save();

        ValueFactory vf = session.getValueFactory();

        QueryManager qm = session.getWorkspace().getQueryManager();

        // SQL-2

        Query q = qm.createQuery("select text from [nt:base] where id = $id", Query.JCR_SQL2);
        q.bindValue("id", vf.createValue("1"));
        QueryResult r = q.execute();
        RowIterator it = r.getRows();
        assertTrue(it.hasNext());
        Row row = it.nextRow();
        assertEquals("hello world", row.getValue("text").getString());
        assertFalse(it.hasNext());

        r = q.execute();
        NodeIterator nodeIt = r.getNodes();
        assertTrue(nodeIt.hasNext());
        Node n = nodeIt.nextNode();
        assertEquals("hello world", n.getProperty("text").getString());
        assertFalse(it.hasNext());

        // SQL

        q = qm.createQuery("select text from [nt:base] where id = 1", Query.SQL);
        q.execute();

        // XPath

        q = qm.createQuery("//*[@id=1]", Query.XPATH);
        q.execute();
    }

    @Test
    public void skip() throws RepositoryException {
        Session session = getAdminSession();
        Node hello1 = session.getRootNode().addNode("hello1");
        hello1.setProperty("id",  "1");
        hello1.setProperty("data",  "x");
        Node hello2 = session.getRootNode().addNode("hello2");
        hello2.setProperty("id",  "2");
        hello2.setProperty("data",  "y");
        session.save();
        ValueFactory vf = session.getValueFactory();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("select id from [nt:base] where data >= $data order by id", Query.JCR_SQL2);
        q.bindValue("data", vf.createValue("x"));
        for (int i = -1; i < 4; i++) {
            QueryResult r = q.execute();
            RowIterator it = r.getRows();
            assertEquals(2, r.getRows().getSize());
            assertEquals(2, r.getNodes().getSize());
            Row row;
            try {
                it.skip(i);
                assertTrue(i >= 0 && i <= 2);
            } catch (IllegalArgumentException e) {
                assertEquals(-1, i);
            } catch (NoSuchElementException e) {
                assertTrue(i >= 2);
            }
            if (i <= 0) {
                assertTrue(it.hasNext());
                row = it.nextRow();
                assertEquals("1", row.getValue("id").getString());
            }
            if (i <= 1) {
                assertTrue(it.hasNext());
                row = it.nextRow();
                assertEquals("2", row.getValue("id").getString());
            }
            assertFalse(it.hasNext());
        }
    }

}
