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
package org.apache.jackrabbit.oak.jcr.query;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.junit.Test;

public class ManyIndexesAndUnionPlanTest extends AbstractQueryTest {

    @Test
    public void testResultSize() throws Exception {
        createIndexes();
        createData();
        doTestResultSize(10);
    }
    
    private void createIndexes() throws RepositoryException, InterruptedException {
        Session session = superuser;
        Node index = session.getRootNode().getNode("oak:index");
        for (int i = 0; i < 10; i++) {
            Node lucene = index.addNode("lucene" + i, "oak:QueryIndexDefinition");
            lucene.setProperty("type", "lucene");
            lucene.setProperty("async", "async");
            lucene.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());
        }
        session.save();
    }
    
    private void createData() throws RepositoryException {
        Session session = superuser;
        for (int i = 0; i < 10; i++) {
            Node n = testRootNode.addNode("node" + i);
            n.setProperty("text", "Hello World");
        }
        session.save();
    }
    
    private void doTestResultSize(int expected) throws RepositoryException {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();

        String xpath;
        xpath = "/jcr:root//*[jcr:contains(@text, 'Hello World')]";
        
        Query q;
        long result;
        NodeIterator it;
        StringBuilder buff;
        
        q = qm.createQuery(xpath, "xpath");
        it = q.execute().getNodes();
        result = it.getSize();
        assertTrue("size: " + result + " expected around " + expected, 
                result > expected - 50 && 
                result < expected + 50);
        buff = new StringBuilder();
        while (it.hasNext()) {
            Node n = it.nextNode();
            buff.append(n.getPath()).append('\n');
        }
        for (int j = 0; j < 1; j++) {
            for (int i = 0; i < 1; i++) {
                q = qm.createQuery("explain " + xpath, "xpath");
                RowIterator rit = q.execute().getRows();
                Row r = rit.nextRow();
                assertTrue(r.toString().indexOf("luceneGlobal") >= 0);
            }
        }
    }
    
}