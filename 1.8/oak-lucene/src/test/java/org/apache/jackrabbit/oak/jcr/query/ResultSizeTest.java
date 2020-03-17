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
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;

import org.apache.jackrabbit.core.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;

public class ResultSizeTest extends AbstractQueryTest {

    public void testResultSize() throws Exception {
        doTestResultSize(false);
    }
    
    public void testResultSizeLuceneV1() throws Exception {
        Session session = superuser;
        Node index = session.getRootNode().getNode("oak:index");
        Node luceneGlobal = index.getNode("luceneGlobal");
        luceneGlobal.setProperty("type", "disabled");
        Node luceneV1 = index.addNode("luceneV1", "oak:QueryIndexDefinition");
        luceneV1.setProperty("type", "lucene");
        luceneV1.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());
        session.save();

        try {
            doTestResultSize(true);
        } finally {
            luceneV1.remove();
            luceneGlobal.setProperty("type", "lucene");
            luceneGlobal.setProperty("reindex", true);
            session.save();
        }
    }
    
    private void doTestResultSize(boolean aggregateAtQueryTime) throws RepositoryException {
        createData();
        int expectedForUnion = 400;
        int expectedForTwoConditions = aggregateAtQueryTime ? 400 : 200;
        doTestResultSize(false, expectedForTwoConditions);
        doTestResultSize(true, expectedForUnion);
    }
    
    private void createData() throws RepositoryException {
        Session session = superuser;
        for (int i = 0; i < 200; i++) {
            Node n = testRootNode.addNode("node" + i);
            n.setProperty("text", "Hello World");
        }
        session.save();
    }
    
    private void doTestResultSize(boolean union, int expected) throws RepositoryException {
        Session session = superuser;
        QueryManager qm = session.getWorkspace().getQueryManager();

        String xpath;
        if (union) {
            xpath = "/jcr:root//*[jcr:contains(@text, 'Hello') or jcr:contains(@text, 'World')]";
        } else {
            xpath = "/jcr:root//*[jcr:contains(@text, 'Hello World')]";
        }
        
        Query q;
        long result;
        NodeIterator it;
        StringBuilder buff;
        
        // fast (insecure) case
        // enabled by default now, in LuceneOakRepositoryStub 
        System.clearProperty("oak.fastQuerySize");
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
        String fastSizeResult = buff.toString();
        q = qm.createQuery(xpath, "xpath");
        q.setLimit(90);
        it = q.execute().getNodes();
        assertEquals(90, it.getSize());
        
        
        // default (secure) case
        // manually disabled
        System.setProperty("oak.fastQuerySize", "false");
        q = qm.createQuery(xpath, "xpath");
        it = q.execute().getNodes();
        result = it.getSize();
        assertEquals(-1, result);
        buff = new StringBuilder();
        while (it.hasNext()) {
            Node n = it.nextNode();
            buff.append(n.getPath()).append('\n');
        }
        String regularResult = buff.toString();
        assertEquals(regularResult, fastSizeResult);
        System.clearProperty("oak.fastQuerySize");
        
    }
    
}