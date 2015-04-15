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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.query.InvalidQueryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.query.RowIterator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.core.query.AbstractQueryTest;

public class TextExtractionQueryTest extends AbstractQueryTest {

    public void testScoreWithoutFulltext() throws Exception {
        System.out.println(Query.JCR_SQL2);
        QueryResult r = executeSQL2Query("select [jcr:path] from [nt:base] order by [jcr:score]");
        RowIterator it = r.getRows();
        while (it.hasNext()) {
            it.nextRow();
        }
    }
    
    public void testFileContains() throws Exception {
        assertFileContains("test.txt", "text/plain",
                "AE502DBEA2C411DEBD340AD156D89593");
        assertFileContains("test.rtf", "application/rtf", "quick brown fox");
    }

    public void testNtFile() throws RepositoryException, IOException {
        while (testRootNode.hasNode(nodeName1)) {
            testRootNode.getNode(nodeName1).remove();
        }

        String content = "The quick brown fox jumps over the lazy dog.";
        Node file = JcrUtils.putFile(testRootNode, nodeName1, "text/plain",
                new ByteArrayInputStream(content.getBytes("UTF-8")));

        testRootNode.getSession().save();
        String xpath = testPath + "/*[jcr:contains(jcr:content, 'lazy')]";
        executeXPathQuery(xpath, new Node[] { file });
    }

    private void assertFileContains(String name, String type,
            String... statements) throws Exception {
        if (testRootNode.hasNode(nodeName1)) {
            testRootNode.getNode(nodeName1).remove();
        }
        testRootNode.getSession().save();

        Node resource = testRootNode.addNode(nodeName1, NodeType.NT_RESOURCE);
        resource.setProperty(JcrConstants.JCR_MIMETYPE, type);
        InputStream stream = getClass().getResourceAsStream(name);
        assertNotNull(stream);
        try {
            Binary binary = testRootNode.getSession().getValueFactory()
                    .createBinary(stream);
            resource.setProperty(JcrConstants.JCR_DATA, binary);
        } finally {
            stream.close();
        }
        testRootNode.getSession().save();
        for (String statement : statements) {
            assertContainsQuery(statement, true);
        }
    }

    @SuppressWarnings("deprecation")
    private void assertContainsQuery(String statement, boolean match)
            throws InvalidQueryException, RepositoryException {
        StringBuffer stmt = new StringBuffer();
        stmt.append("/jcr:root").append(testRoot).append("/*");
        stmt.append("[jcr:contains(., '").append(statement);
        stmt.append("')]");

        Query q = qm.createQuery(stmt.toString(), Query.XPATH);
        checkResult(q.execute(), match ? 1 : 0);

        stmt = new StringBuffer();
        stmt.append("SELECT * FROM nt:base ");
        stmt.append("WHERE jcr:path LIKE '").append(testRoot).append("/%' ");
        stmt.append("AND CONTAINS(., '").append(statement).append("')");

        q = qm.createQuery(stmt.toString(), Query.SQL);
        checkResult(q.execute(), match ? 1 : 0);
    }

}
