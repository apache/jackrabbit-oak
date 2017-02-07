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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests query plans.
 */
public class QueryPlanTest extends AbstractRepositoryTest {

    public QueryPlanTest(NodeStoreFixture fixture) {
        super(fixture);
    }
    
    @Test
    // OAK-1902
    public void propertyIndexVersusNodeTypeIndex() throws Exception {
        Session session = getAdminSession();
        Node nt = session.getRootNode().getNode("oak:index").getNode("nodetype");
        nt.setProperty("entryCount", 200);
        Node uuid = session.getRootNode().getNode("oak:index").getNode("uuid");
        uuid.setProperty("entryCount", 100);
        QueryManager qm = session.getWorkspace().getQueryManager();
        if (session.getRootNode().hasNode("testroot")) {
            session.getRootNode().getNode("testroot").remove();
            session.save();
        }
        Node testRootNode = session.getRootNode().addNode("testroot");
        for (int i = 0; i < 100; i++) {
            Node n = testRootNode.addNode("n" + i, "oak:Unstructured");
            n.addMixin("mix:referenceable");
        }
        session.save();
       
        Query q;
        QueryResult result;
        RowIterator it;

        String xpath = "/jcr:root//element(*, oak:Unstructured)";
        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // System.out.println("plan: " + plan);
        // should use the node type index
        assertEquals("[oak:Unstructured] as [a] " + 
                "/* nodeType Filter(query=explain select [jcr:path], [jcr:score], * " + 
                "from [oak:Unstructured] as a " + 
                "where isdescendantnode(a, '/') " + 
                "/* xpath: /jcr:root//element(*, oak:Unstructured) */" + 
                ", path=//*) where isdescendantnode([a], [/]) */", 
                plan);

        String xpath2 = "/jcr:root//element(*, oak:Unstructured)[@jcr:uuid]";
        q = qm.createQuery("explain " + xpath2 + "", "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        plan = it.nextRow().getValue("plan").getString();
        // should use the index on "jcr:uuid"
        assertEquals("[oak:Unstructured] as [a] " +
                "/* property uuid IS NOT NULL where ([a].[jcr:uuid] is not null) " +
                "and (isdescendantnode([a], [/])) */",
                plan);
    }     
    
    @Test
    // OAK-1903
    public void propertyEqualsVersusPropertyNotNull() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        createPropertyIndex(session, "notNull");
        createPropertyIndex(session, "equals");
        for (int i = 0; i < 100; i++) {
            Node n = testRootNode.addNode("n" + i, "oak:Unstructured");
            if (i % 2 == 0) {
                n.setProperty("notNull", i);
            }
            n.setProperty("equals", 1);
        }
        session.save();
       
        String xpath = "/jcr:root//*[@notNull and @equals=1]";
        
        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // System.out.println("plan: " + plan);
        // should not use the index on "jcr:uuid"
        assertEquals("[nt:base] as [a] /* property notNull IS NOT NULL " +
                "where ([a].[notNull] is not null) " +
                "and ([a].[equals] = 1) " +
                "and (isdescendantnode([a], [/])) */",
                plan);
    }           

    @Test
    // OAK-1898
    public void correctPropertyIndexUsage() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        createPropertyIndex(session, "fiftyPercent");
        createPropertyIndex(session, "tenPercent");
        createPropertyIndex(session, "hundredPercent");
        for (int i = 0; i < 300; i++) {
            Node n = testRootNode.addNode("n" + i, "oak:Unstructured");
            if (i % 10 == 0) {
                n.setProperty("tenPercent", i);
            }
            if (i % 2 == 0) {
                n.setProperty("fiftyPercent", i);
            }
            n.setProperty("hundredPercent", i);
        }
        session.save();
       
        String xpath = "/jcr:root//*[@tenPercent and @fiftyPercent and @hundredPercent]";
        
        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // System.out.println("plan: " + plan);
        // should not use the index on "jcr:uuid"
        assertEquals("[nt:base] as [a] /* property tenPercent IS NOT NULL " +
                "where ([a].[tenPercent] is not null) " +
                "and ([a].[fiftyPercent] is not null) " +
                "and ([a].[hundredPercent] is not null) " +
                "and (isdescendantnode([a], [/])) */",
                plan);
    }           

    @Test
    // OAK-1898
    public void traversalVersusPropertyIndex() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n = testRootNode;
        for (int i = 0; i < 20; i++) {
            n.setProperty("depth", i + 2);
            n = n.addNode("n", "oak:Unstructured");
            n.addMixin("mix:referenceable");
        }
        session.save();
       
        String xpath = "/jcr:root/testroot/n/n/n/n/n/n/n//*[jcr:uuid]";
        
        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // System.out.println("plan: " + plan);
        // should not use the index on "jcr:uuid"
        assertEquals("[nt:base] as [a] /* property uuid IS NOT NULL " +
                "where ([a].[jcr:uuid] is not null) and " + 
                "(isdescendantnode([a], [/testroot/n/n/n/n/n/n/n])) */", 
                plan);
    }        
    
    @Test
    public void nodeType() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node nodetype = session.getRootNode().getNode("oak:index").getNode("nodetype");
        nodetype.setProperty("entryCount", 10000000);
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n1 = testRootNode.addNode("node1");
        Node n2 = n1.addNode("node2");
        n2.addNode("node3");
        session.save();
       
        String sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where [node2/node3/jcr:primaryType] is not null " + 
                "and isdescendantnode('/testroot')";
        
        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // should not use the index on "jcr:primaryType"
        assertEquals("[nt:base] as [nt:base] /* traverse \"/testroot//*\" " + 
                "where ([nt:base].[node2/node3/jcr:primaryType] is not null) " + 
                "and (isdescendantnode([nt:base], [/testroot])) " +
                "*/", 
                plan);
        
        // verify the result
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String path = it.nextRow().getValue("path").getString();
        assertEquals("/testroot/node1", path);
        
    }
    
    @Test
    public void uuidIndex() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n = testRootNode.addNode("node");
        n.addMixin("mix:referenceable");
        session.save();

        // this matches just one node (exact path),
        // so it should use the TraversintIndex
        String xpath = "/jcr:root/testroot/node[@jcr:uuid]";

        Query q;
        QueryResult result;
        RowIterator it;

        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        assertEquals("[nt:base] as [a] /* traverse \"/testroot/node\" where " + 
                "([a].[jcr:uuid] is not null) " + 
                "and (issamenode([a], [/testroot/node])) */", plan);

        // verify the result
        q = qm.createQuery(xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String path = it.nextRow().getPath();
        assertEquals("/testroot/node", path);
        assertFalse(it.hasNext());

        // this potentially matches many nodes,
        // so it should use the index on the UUID property
        xpath = "/jcr:root/testroot/*[@jcr:uuid]";
        
        q = qm.createQuery("explain " + xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        plan = it.nextRow().getValue("plan").getString();
        assertEquals("[nt:base] as [a] /* property uuid IS NOT NULL " +
                "where ([a].[jcr:uuid] is not null) " + 
                "and (ischildnode([a], [/testroot])) */", plan);
        
    }
    
    @Test
    @Ignore("OAK-1372")
    public void pathAndPropertyRestrictions() throws Exception {
        ; // TODO work in progress
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node b = testRootNode.addNode("b");
        Node c = b.addNode("c");
        Node d = c.addNode("d");
        Node e1 = d.addNode("e1");
        e1.setProperty("type", "1");
        Node e2 = d.addNode("e2");
        e2.setProperty("type", "2");
        Node e3 = d.addNode("e3");
        e3.setProperty("type", "3");
        session.save();
       
        String xpath = "/jcr:root/testroot//b/c/d/*[@jcr:uuid='1' or @jcr:uuid='2'] ";
        String 
        sql2 = 
                "select d.[jcr:path] as [jcr:path], d.[jcr:score] as [jcr:score], d.* " + 
                "from [nt:base] as a inner join [nt:base] as b on ischildnode(b, a) " + 
                "inner join [nt:base] as c on ischildnode(c, b) " + 
                "inner join [nt:base] as d on ischildnode(d, c) " + 
                "where name(a) = 'b' " + 
                "and isdescendantnode(a, '/testroot') " + 
                "and name(b) = 'c' " + 
                "and name(c) = 'd' " + 
                "and (d.[jcr:uuid] = '1' or d.[jcr:uuid] = '2')";

        sql2 = 
                "select d.[jcr:path] as [jcr:path], d.[jcr:score] as [jcr:score], d.* " + 
                "from [nt:base] as d " + 
                "where (d.[jcr:uuid] = '1' or d.[jcr:uuid] = '2')";

        sql2 = 
                "select d.[jcr:path] as [jcr:path], d.[jcr:score] as [jcr:score], d.* " + 
                "from [nt:base] as d " + 
                "inner join [nt:base] as c on ischildnode(d, c) " + 
                "inner join [nt:base] as b on ischildnode(c, b) " + 
                "inner join [nt:base] as a on ischildnode(b, a) " + 
                "where name(a) = 'b' " + 
                "and isdescendantnode(a, '/testroot') " + 
                "and name(b) = 'c' " + 
                "and name(c) = 'd' " + 
                "and (d.[jcr:uuid] = '1' or d.[jcr:uuid] = '2')";

        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        assertEquals("", plan);
        
        // [nt:base] as [a] /* traverse "/testroot//*" 
        // where (name([a]) = cast('b' as string)) 
        // and (isdescendantnode([a], [/testroot])) */ 
        // inner join [nt:base] as [b] /* traverse 
        // "/path/from/the/join/selector/*" where name([b]) = cast('c' as string) */ 
        // on ischildnode([b], [a]) inner join [nt:base] as [c] 
        // /* traverse "/path/from/the/join/selector/*"
        // where name([c]) = cast('d' as string) */ on ischildnode([c], [b]) 
        // inner join [nt:base] as [d] /* traverse "/path/from/the/join/selector/*" 
        // where ([d].[type] is not null) and ([d].[type] in(cast('1' as string), cast('2' as string))) */ 
        // on ischildnode([d], [c])
        
//        assertEquals("[nt:base] as [nt:base] /* traverse \"*\" " + 
//                "where [nt:base].[node2/node3/jcr:primaryType] is not null */", 
//                plan);
        
        // verify the result
        q = qm.createQuery(xpath, "xpath");
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String path = it.nextRow().getValue("path").getString();
        assertEquals("/testroot/b/c/d/e1", path);
        path = it.nextRow().getValue("path").getString();
        assertEquals("/testroot/b/c/d/e2", path);
        assertFalse(it.hasNext());
    }
    
    private static void createPropertyIndex(Session s, String propertyName) throws RepositoryException {
        Node n = s.getRootNode().getNode("oak:index").
                addNode(propertyName, "oak:QueryIndexDefinition");
        n.setProperty("type", "property");
        n.setProperty("entryCount", "-1");
        n.setProperty("propertyNames", new String[]{propertyName}, PropertyType.NAME);
        s.save();
    }
    
}
