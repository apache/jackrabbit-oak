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

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

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

import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.iterator.RowIterable;
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
        hello.setProperty("text",  "hello_world");
        session.save();
        Node hello2 = session.getRootNode().addNode("hello2");
        hello2.setProperty("id",  "2");
        hello2.setProperty("text",  "hello world");
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
        assertEquals("hello_world", row.getValue("text").getString());
        String[] columns = r.getColumnNames();
        assertEquals(1, columns.length);
        assertEquals("text", columns[0]);
        assertFalse(it.hasNext());

        r = q.execute();
        NodeIterator nodeIt = r.getNodes();
        assertTrue(nodeIt.hasNext());
        Node n = nodeIt.nextNode();
        assertEquals("hello_world", n.getProperty("text").getString());
        assertFalse(it.hasNext());

        // SQL

        q = qm.createQuery("select text from [nt:base] where text like 'hello\\_world' escape '\\'", Query.SQL);
        r = q.execute();
        columns = r.getColumnNames();
        assertEquals(3, columns.length);
        assertEquals("text", columns[0]);
        assertEquals("jcr:path", columns[1]);
        assertEquals("jcr:score", columns[2]);
        nodeIt = r.getNodes();
        assertTrue(nodeIt.hasNext());
        n = nodeIt.nextNode();
        assertEquals("hello_world", n.getProperty("text").getString());
        assertFalse(nodeIt.hasNext());

        // XPath

        q = qm.createQuery("//*[@id=1]", Query.XPATH);
        r = q.execute();
        columns = r.getColumnNames();
        assertEquals(3, columns.length);
        assertEquals("jcr:path", columns[0]);
        assertEquals("jcr:score", columns[1]);
        assertEquals("*", columns[2]);
    }

    @Test
    public void skip() throws RepositoryException {
        Session session = getAdminSession();
        Node hello1 = session.getRootNode().addNode("hello1");
        hello1.setProperty("id",  "1");
        hello1.setProperty("data",  "x");
        session.save();
        Node hello3 = hello1.addNode("hello3");
        hello3.setProperty("id",  "3");
        hello3.setProperty("data",  "z");
        session.save();
        Node hello2 = hello3.addNode("hello2");
        hello2.setProperty("id",  "2");
        hello2.setProperty("data",  "y");
        session.save();
        ValueFactory vf = session.getValueFactory();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("select id from [nt:base] where data >= $data order by id", Query.JCR_SQL2);
        q.bindValue("data", vf.createValue("x"));
        for (int i = -1; i < 5; i++) {
            QueryResult r = q.execute();
            RowIterator it = r.getRows();
            assertEquals(3, r.getRows().getSize());
            assertEquals(3, r.getNodes().getSize());
            Row row;
            try {
                it.skip(i);
                assertTrue(i >= 0 && i <= 3);
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
            if (i <= 2) {
                assertTrue(it.hasNext());
                row = it.nextRow();
                assertEquals("3", row.getValue("id").getString());
            }
            assertFalse(it.hasNext());
        }
    }
    
    @Test
    public void limit() throws RepositoryException {
        Session session = getAdminSession();
        Node hello1 = session.getRootNode().addNode("hello1");
        hello1.setProperty("id",  "1");
        hello1.setProperty("data",  "x");
        session.save();
        Node hello3 = session.getRootNode().addNode("hello3");
        hello3.setProperty("id",  "3");
        hello3.setProperty("data",  "z");
        session.save();
        Node hello2 = session.getRootNode().addNode("hello2");
        hello2.setProperty("id",  "2");
        hello2.setProperty("data",  "y");
        session.save();
        ValueFactory vf = session.getValueFactory();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("select id from [nt:base] where data >= $data order by id", Query.JCR_SQL2);
        q.bindValue("data", vf.createValue("x"));
        for (int limit = 0; limit < 5; limit++) {
            q.setLimit(limit);
            for (int offset = 0; offset < 3; offset++) {
                q.setOffset(offset);
                QueryResult r = q.execute();
                RowIterator it = r.getRows();
                int l = Math.min(Math.max(0, 3 - offset), limit);
                assertEquals(l, r.getRows().getSize());
                assertEquals(l, r.getNodes().getSize());
                Row row;
                
                for (int x = offset + 1, i = 0; i < limit && x < 4; i++, x++) {
                    assertTrue(it.hasNext());
                    row = it.nextRow();
                    assertEquals("" + x, row.getValue("id").getString());
                }
                assertFalse(it.hasNext());
            }
        }
    }

    @Test
    public void nodeTypeConstraint() throws Exception {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Node folder1 = root.addNode("folder1", "nt:folder");
        Node folder2 = root.addNode("folder2", "nt:folder");
        JcrUtils.putFile(folder1, "file", "text/plain",
                new ByteArrayInputStream("foo bar".getBytes("UTF-8")));
        folder2.addNode("folder3", "nt:folder");
        session.save();

        QueryManager qm = session.getWorkspace().getQueryManager();
        Query q = qm.createQuery("//element(*, nt:folder)", Query.XPATH);
        Set<String> paths = new HashSet<String>();
        for (Row r : new RowIterable(q.execute().getRows())) {
            paths.add(r.getPath());
        }
        assertEquals(new HashSet<String>(Arrays.asList("/folder1", "/folder2", "/folder2/folder3")),
                paths);
    }
}
