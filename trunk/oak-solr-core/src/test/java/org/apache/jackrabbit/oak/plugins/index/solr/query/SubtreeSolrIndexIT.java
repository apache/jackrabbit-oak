/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.solr.query;

import javax.jcr.query.Query;
import java.util.Iterator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.DefaultSolrServerProvider;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.*;

/**
 * Integration test for indexing / search over subtrees with Solr index.
 */
public class SubtreeSolrIndexIT extends AbstractQueryTest {

    public static final String SUBTREE = "subtree";

    @Rule
    public TestName name = new TestName();

    @Override
    protected void createTestIndexNode() throws Exception {
        Tree rootTree = root.getTree("/");
        Tree subtree = rootTree.addChild(SUBTREE);
        Tree solrIndexNode = createTestIndexNode(subtree, SolrQueryIndex.TYPE);
        solrIndexNode.setProperty("pathRestrictions", false);
        solrIndexNode.setProperty("propertyRestrictions", true);
        solrIndexNode.setProperty("primaryTypes", false);
        solrIndexNode.setProperty("commitPolicy", "hard");
        Tree server = solrIndexNode.addChild("server");
        server.setProperty("solrServerType", "embedded");
        server.setProperty("solrHomePath", "target/" + name.getMethodName());

        root.commit();
    }

    @Override
    protected ContentRepository createRepository() {
        try {
            DefaultSolrServerProvider solrServerProvider = new DefaultSolrServerProvider();
            DefaultSolrConfigurationProvider oakSolrConfigurationProvider = new DefaultSolrConfigurationProvider();
            return new Oak().with(new InitialContent())
                    .with(new OpenSecurityProvider())
                    .with(new SolrQueryIndexProvider(solrServerProvider, oakSolrConfigurationProvider))
                    .with(new SolrIndexEditorProvider(solrServerProvider, oakSolrConfigurationProvider))
                    .createContentRepository();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() throws Exception {

        Tree content = root.getTree("/").getChild(SUBTREE);
        Tree a = content.addChild("a");
        a.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        a.setProperty("foo", "doc bye");
        a.setProperty("loc", "2");
        Tree b = content.addChild("b");
        b.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
        b.setProperty("foo", "bye doc bye");
        b.setProperty("loc", "1");
        root.commit();

        String query = "select [jcr:path] from [nt:base] where contains(*,'doc') " +
                "AND isdescendantnode('/" + SUBTREE + "')";

        Iterator<String> results = executeQuery(query, Query.JCR_SQL2, true).iterator();
        assertTrue(results.hasNext());
        assertEquals("/" + SUBTREE + "/a", results.next());
        assertTrue(results.hasNext());
        assertEquals("/" + SUBTREE + "/b", results.next());
        assertFalse(results.hasNext());
    }
}
