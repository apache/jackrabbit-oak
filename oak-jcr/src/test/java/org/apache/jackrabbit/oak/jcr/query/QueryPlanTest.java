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
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.junit.Test;

/**
 * Tests query plans.
 */
public class QueryPlanTest extends AbstractRepositoryTest {

    public QueryPlanTest(NodeStoreFixture fixture) {
        super(fixture);
    }
    
    @Test
    public void nodeType() throws Exception {
        Session session = getAdminSession();
        QueryManager qm = session.getWorkspace().getQueryManager();
        Node testRootNode = session.getRootNode().addNode("testroot");
        Node n1 = testRootNode.addNode("node1");
        Node n2 = n1.addNode("node2");
        n2.addNode("node3");
        session.save();
       
        String sql2 = "select [jcr:path] as [path] from [nt:base] " + 
                "where [node2/node3/jcr:primaryType] is not null";
        
        Query q;
        QueryResult result;
        RowIterator it;
        
        q = qm.createQuery("explain " + sql2, Query.JCR_SQL2);
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String plan = it.nextRow().getValue("plan").getString();
        // should not use the index on "jcr:primaryType"
        assertEquals("[nt:base] as [nt:base] /* traverse \"*\" " + 
                "where [nt:base].[node2/node3/jcr:primaryType] is not null */", 
                plan);
        
        // verify the result
        q = qm.createQuery(sql2, Query.JCR_SQL2);
        result = q.execute();
        it = result.getRows();
        assertTrue(it.hasNext());
        String path = it.nextRow().getValue("path").getString();
        assertEquals("/testroot/node1", path);
        
    }
}
