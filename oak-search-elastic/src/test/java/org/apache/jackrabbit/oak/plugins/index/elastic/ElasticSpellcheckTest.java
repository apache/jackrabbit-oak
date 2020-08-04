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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import javax.jcr.security.Privilege;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_ANALYZED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_USE_IN_SPELLCHECK;
import static org.junit.Assert.assertEquals;

public class ElasticSpellcheckTest {

    private Session adminSession;
    private Session anonymousSession;
    private QueryManager qe;
    private Node indexNode;

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = System.getProperty("elasticConnectionString");

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        anonymousSession.logout();
        adminSession.logout();
        elasticRule.closeElasticConnection();
    }

    @Before
    public void setup() throws Exception {
        createRepository();
        final String indexName = createIndex();
        indexNode = adminSession.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName);
    }

    private void createRepository() throws RepositoryException {
        ElasticConnection connection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(connection);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with(editorProvider)
                .with((Observer) indexProvider)
                .with((QueryIndexProvider) indexProvider);

        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();

        adminSession = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);

        // we'd always query anonymously
        anonymousSession = repository.login(new GuestCredentials(), null);
        anonymousSession.refresh(true);
        anonymousSession.save();

        qe = anonymousSession.getWorkspace().getQueryManager();
    }

    private class IndexSkeleton {
        IndexDefinitionBuilder indexDefinitionBuilder;
        IndexDefinitionBuilder.IndexRule indexRule;

        void initialize() {
            initialize(JcrConstants.NT_BASE);
        }

        void initialize(String nodeType) {
            indexDefinitionBuilder = new ElasticIndexDefinitionBuilder();
            indexRule = indexDefinitionBuilder.indexRule(nodeType);
        }

        String build() throws RepositoryException {
            final String indexName = UUID.randomUUID().toString();
            indexDefinitionBuilder.build(adminSession.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(indexName));
            return indexName;
        }
    }

    private String createIndex() throws RepositoryException {
        IndexSkeleton indexSkeleton = new IndexSkeleton();
        indexSkeleton.initialize();
        indexSkeleton.indexDefinitionBuilder.noAsync();
        indexSkeleton.indexRule.property("cons").propertyIndex();
        indexSkeleton.indexRule.property("foo").propertyIndex();
        indexSkeleton.indexRule.property("foo").getBuilderTree().setProperty(PROP_USE_IN_SPELLCHECK, true, Type.BOOLEAN);
        indexSkeleton.indexRule.property("foo").getBuilderTree().setProperty(PROP_ANALYZED, true, Type.BOOLEAN);

        return indexSkeleton.build();
    }

    @Test
    public void testSpellcheckSingleWord() throws Exception {
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
                assertEquals("[decent, descent]", getResult(q.execute()).toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSpellcheckSingleWordWithDescendantNode() throws Exception {
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
                assertEquals("[decent]", getResult(q.execute()).toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testSpellcheckMultipleWords() throws Exception {
        adminSession.save();
        QueryManager qm = adminSession.getWorkspace().getQueryManager();
        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));
        Node n1 = par.addNode("node1");
        n1.setProperty("foo", "it is always a good idea to go visiting ontario");
        Node n2 = par.addNode("node2");
        n2.setProperty("foo", "ontario is a nice place to live in");
        Node n3 = par.addNode("node3");
        n2.setProperty("foo", "I flied to ontario for voting for the major polls");
        Node n4 = par.addNode("node4");
        n2.setProperty("foo", "I will go voting in ontario, I always voted since I've been allowed to");
        adminSession.save();

        String sql = "SELECT [rep:spellcheck()] FROM nt:base WHERE SPELLCHECK('votin in ontari')";
        Query q = qm.createQuery(sql, Query.SQL);

        assertEventually(() -> {
            try {
                assertEquals("[voting in ontario]", getResult(q.execute()).toString());
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    private static List<String> getResult(QueryResult result) throws RepositoryException {
        List<String> results = new ArrayList<>();
        RowIterator it = result.getRows();
        while (it.hasNext()) {
            Row row = it.nextRow();
            results.add(row.getValue("rep:spellcheck()").getString());
        }
        return results;
    }

    private static void assertEventually(Runnable r) {
        ElasticTestUtils.assertEventually(r, BULK_FLUSH_INTERVAL_MS_DEFAULT * 3);
    }

}
