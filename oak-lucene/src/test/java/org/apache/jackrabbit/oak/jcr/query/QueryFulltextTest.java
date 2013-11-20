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
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.core.query.AbstractQueryTest;

/**
 * Tests the fulltext index.
 */
public class QueryFulltextTest extends AbstractQueryTest {
    
    public void testFulltext() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
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

// TODO the plan should actually be:
//            assertEquals("[nt:base] as [nt:base] /* " + 
//                    "+((text:hallo text:hello)~1) +text:{* TO *} " + 
//                    "ft:(text:\"hallo\" OR text:\"hello\") " +
//                    "where contains([nt:base].[text], cast('hello OR hallo' as string)) */", 
//                    getResult(q.execute(), "plan"));
        assertEquals("[nt:base] as [nt:base] /* " + 
                "aggregate :fulltext:hallo :fulltext:hello " + 
                "ft:(text:\"hallo\" OR text:\"hello\") " +
                "where contains([nt:base].[text], cast('hello OR hallo' as string)) */", 
                getResult(q.execute(), "plan"));
   
        // lowercase "or" mean search for the term "or"
        sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([text], 'hello or hallo') order by [jcr:path]";
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("", 
                getResult(q.execute(), "path"));

    }
    
    public void testFulltextRelativeProperty() throws Exception {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node n1 = testRootNode.addNode("node1");
        n1.setProperty("text", "hello");
        Node n2 = testRootNode.addNode("node2");
        n2.setProperty("text", "hallo");
        Node n3 = testRootNode.addNode("node3");
        n3.setProperty("text", "hello hallo");
        session.save();

        Query q;

        String sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where ISCHILDNODE([/testroot])" + 
                " AND CONTAINS(text, 'hallo')";

        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        // TODO the plan should actually be:
//          assertEquals("[nt:base] as [nt:base] /* " + 
//                  "+text:hallo +:path:/testroot/* +text:{* TO *} " + 
//                  "ft:(text:\"hallo\") " + 
//                  "where (ischildnode([nt:base], [/testroot])) " + 
//                  "and (contains([nt:base].[text], cast('hallo' as string))) */", 
//                  getResult(q.execute(), "plan"));
        assertEquals("[nt:base] as [nt:base] /* " + 
                "aggregate +:fulltext:hallo* +:path:/testroot/* +text:{* TO *}" + 
                "ft:(text:\"hallo\") " + 
                "where (ischildnode([nt:base], [/testroot])) " + 
                "and (contains([nt:base].[text], cast('hallo' as string))) */", 
                getResult(q.execute(), "plan"));
        
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("/testroot/node2, /testroot/node3", getResult(q.execute(), "path"));
       
        sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([node1/text], 'hello') order by [jcr:path]";
        
//      q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
//    assertEquals("[nt:base] as [nt:base] /* " + 
//            "+text:hallo +:path:/testroot/* +text:{* TO *} " + 
//            "ft:(text:\"hallo\") " + 
//            "where (ischildnode([nt:base], [/testroot])) " + 
//            "and (contains([nt:base].[text], cast('hallo' as string))) */", 
//            getResult(q.execute(), "plan"));

        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("/testroot", getResult(q.execute(), "path"));
        
        sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([node2/text], 'hello OR hallo') order by [jcr:path]";
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        // TODO the plan should actually be:
//            assertEquals("[nt:base] as [nt:base] /* " + 
//                    "(text:hallo text:hello)~1 " + 
//                    "ft:(node2/text:\"hallo\" OR node2/text:\"hello\") " + 
//                    "parent:node2 " + 
//                    "where contains([nt:base].[node2/text], cast('hello OR hallo' as string)) */", 
//                    getResult(q.execute(), "plan"));
        assertEquals("[nt:base] as [nt:base] /* " + 
                "aggregate :fulltext:hallo* :fulltext:hello* " + 
                "ft:(node2/text:\"hallo\" OR node2/text:\"hello\") " + 
                "parent:node2 " + 
                "where contains([nt:base].[node2/text], cast('hello OR hallo' as string)) */", 
      getResult(q.execute(), "plan"));
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        assertEquals("/testroot", 
                getResult(q.execute(), "path"));            
        
        sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where contains([node1/text], 'hello') " + 
                "and contains([node2/text], 'hallo') " + 
                "order by [jcr:path]";
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        // TODO OAK-890
        assertEquals("[nt:base] as [nt:base] /* " + 
                "aggregate Not yet implemented " + 
                "where (contains([nt:base].[node1/text], cast('hello' as string))) " + 
                "and (contains([nt:base].[node2/text], cast('hallo' as string))) */", 
                getResult(q.execute(), "plan"));
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        // assertEquals("/testroot", 
        //        getResult(q.execute(), "path"));            
        
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
