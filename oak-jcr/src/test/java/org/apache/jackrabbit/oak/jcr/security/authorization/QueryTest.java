/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.security.AccessControlException;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.security.Privilege;

/**
 * Tests access rights for queries.
 */
public class QueryTest extends AbstractEvaluationTest {
    
    public void testJoin() throws Exception {
        // create a visible node /test/node1 
        // with an invisible child /test/node1/node2
        // with an invisible child /test/node1/node2/node3
        Node n = superuser.getNode(path);
        Node visible = n.addNode(nodeName1, testNodeType);
        allow(visible.getPath(), privilegesFromName(Privilege.JCR_READ));
        Node invisible = visible.addNode(nodeName2, testNodeType);
        Node invisible2 = invisible.addNode(nodeName3, testNodeType);
        deny(invisible.getPath(), privilegesFromName(Privilege.JCR_READ));
        deny(invisible2.getPath(), privilegesFromName(Privilege.JCR_READ));
        superuser.save();

        // test visibility
        testSession.refresh(false);
        testSession.checkPermission(visible.getPath(), Session.ACTION_READ);        
        try {
            testSession.checkPermission(invisible.getPath(), Session.ACTION_READ);        
            fail();
        } catch (AccessControlException e) {
            // expected
        }
        Node x = testSession.getNode(visible.getPath());
        
        ValueFactory vf = testSession.getValueFactory();
        Query q;
        QueryResult r;
        NodeIterator ni;

        // verify we can see the visible node
        q = testSession.getWorkspace().getQueryManager().createQuery(
                "select * from [nt:base] where [jcr:path]=$path", Query.JCR_SQL2);
        q.bindValue("path", vf.createValue(visible.getPath()));
        r = q.execute();
        ni = r.getNodes();
        assertTrue(ni.hasNext());
        x = ni.nextNode();
        assertTrue(x.getSession() == testSession);

        // verify we cannot see the invisible node
        q = testSession.getWorkspace().getQueryManager().createQuery(
                "select * from [nt:base] where [jcr:path]=$path", Query.JCR_SQL2);
        q.bindValue("path", vf.createValue(invisible.getPath()));
        r = q.execute();
        assertFalse(r.getNodes().hasNext());
        
        // the superuser should see both nodes
        q = superuser.getWorkspace().getQueryManager().createQuery(
                "select a.* from [nt:base] as a " +
                "inner join [nt:base] as b on isdescendantnode(b, a) " +
                "where a.[jcr:path]=$path", Query.JCR_SQL2);
        q.bindValue("path", vf.createValue(visible.getPath()));
        r = q.execute();
        assertTrue(r.getNodes().hasNext());

        // but the testSession must not:
        // verify we can not deduce existence of the invisible node
        // using a join
        q = testSession.getWorkspace().getQueryManager().createQuery(
                "select a.* from [nt:base] as a " +
                "inner join [nt:base] as b on isdescendantnode(b, a) " +
                "where a.[jcr:path]=$path", Query.JCR_SQL2);
        q.bindValue("path", vf.createValue(visible.getPath()));
        r = q.execute();
        assertFalse(r.getNodes().hasNext());

    }
    
}
