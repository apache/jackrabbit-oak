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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import javax.jcr.security.Privilege;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_SPELLCHECK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class SpellcheckCommonTest extends AbstractJcrTest {

    protected Node indexNode;
    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Before
    public void createIndex() throws RepositoryException {

        String indexName = UUID.randomUUID().toString();
        IndexDefinitionBuilder builder = indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false);
        builder.noAsync().evaluatePathRestrictions();
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule(JcrConstants.NT_BASE);

        indexRule.property("cons").propertyIndex();
        indexRule.property("foo").propertyIndex();
        indexRule.property("foo").getBuilderTree().setProperty(PROP_USE_IN_SPELLCHECK, true, Type.BOOLEAN);
        indexRule.property("foo").getBuilderTree().setProperty(PROP_ANALYZED, true, Type.BOOLEAN);

        indexOptions.setIndex(adminSession, indexName, builder);
        indexNode = indexOptions.getIndexNode(adminSession, indexName);

    }

    @Test
    public void notExistingWord() throws Exception {
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "lorem");
        adminSession.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('foobarbaz')";
        Query q = qm.createQuery(sql, Query.SQL);
        assertEventually(() -> {
            try {
                assertTrue(getResult(q.execute(), "rep:spellcheck()").isEmpty());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void singleWord() throws Exception {
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "descent");
        Node n2 = n1.addNode("node2");
        n2.setProperty("foo", "decent");
        adminSession.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('desent')";
        Query q = qm.createQuery(sql, Query.SQL);
        assertEventually(() -> {
            try {
                assertEquals("[decent, descent]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void singleWordWithDescendantNode() throws Exception {
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "descent");
        Node n2 = n1.addNode("node2");
        n2.setProperty("foo", "decent");
        adminSession.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('desent') and isDescendantNode('/parent/node1')";
        Query q = qm.createQuery(sql, Query.SQL);
        assertEventually(() -> {
            try {
                assertEquals("[decent]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void multipleWords() throws Exception {
        adminSession.save();
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "it is always a good idea to go visiting ontario");
        Node n2 = par.addNode("node2");
        n2.setProperty("foo", "ontario is a nice place to live in");
        Node n3 = par.addNode("node3");
        n3.setProperty("foo", "I flew to ontario for voting for the major polls");
        Node n4 = par.addNode("node4");
        n4.setProperty("foo", "I will go voting in ontario, I always voted since I've been allowed to");
        adminSession.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('votin in ontari')";
        Query q = qm.createQuery(sql, Query.SQL);

        assertEventually(() -> {
            try {
                assertEquals("[voting in ontario]", getResult(q.execute(), "rep:spellcheck()").toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    static List<String> getResult(QueryResult result, String propertyName) throws RepositoryException {
        List<String> results = new ArrayList<>();
        RowIterator it;

        it = result.getRows();
        while (it.hasNext()) {
            Row row = it.nextRow();
            results.add(row.getValue(propertyName).getString());
        }
        return results;
    }

    private static void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, 3000 * 3);
    }

}
