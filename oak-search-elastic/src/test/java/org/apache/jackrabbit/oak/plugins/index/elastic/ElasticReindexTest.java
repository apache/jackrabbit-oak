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
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;

public class ElasticReindexTest {

    protected int DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS = 5;

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    private Session adminSession;
    private QueryManager qe;

    @Before
    public void setup() throws Exception {
        createRepository();
    }

    private void createRepository() throws RepositoryException {
        ElasticConnection connection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(indexTracker);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);

        Oak oak = new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with(indexTracker)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());

        Jcr jcr = new Jcr(oak);
        Repository repository = jcr.createRepository();

        adminSession = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);

        qe = adminSession.getWorkspace().getQueryManager();
    }

    @Test
    public void reindex() throws Exception {
        IndexDefinitionBuilder indexDefinitionBuilder = new ElasticIndexDefinitionBuilder();
        indexDefinitionBuilder.noAsync();
        IndexDefinitionBuilder.IndexRule indexRule = indexDefinitionBuilder.indexRule("nt:base");
        indexRule.property("a").propertyIndex().analyzed();

        String indexName = UUID.randomUUID().toString();
        indexDefinitionBuilder.build(adminSession.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(indexName, INDEX_DEFINITIONS_NODE_TYPE));
        adminSession.save();

        Node content = adminSession.getRootNode().addNode("content");
        for (int i = 0; i < 100; i++) {
            Node c = content.addNode("c_" + i);
            c.setProperty("a", "foo");
            c.setProperty("b", "bar");
        }
        adminSession.save();

        assertQuery("select [jcr:path] from [nt:base] where contains(a, 'foo')", 100);

        // adding an extra content node (handled by reindex and potentially incremental indexing)
        Node c = content.addNode("c_100");
        c.setProperty("a", "foo");
        c.setProperty("b", "bar");

        Node indexNode = adminSession.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName);
        Node b = indexNode.getNode("indexRules").getNode("nt:base").getNode("properties").addNode("b");
        b.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        b.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        // Now we reindex and see everything works fine
        indexNode.setProperty(REINDEX_PROPERTY_NAME, true);
        adminSession.save();

        assertQuery("select [jcr:path] from [nt:base] where contains(b, 'bar')", 101);
    }

    private void assertQuery(String query, int resultSetSize) {
        TestUtil.assertEventually(() -> {
            try {
                Query q = qe.createQuery(query, Query.JCR_SQL2);
                QueryResult queryResult = q.execute();
                Stream<Row> resultStream = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(queryResult.getRows(), Spliterator.ORDERED), false
                );
                Assert.assertEquals(resultSetSize, resultStream.count());
            } catch (RepositoryException e) {
                e.printStackTrace();
            }
        }, (DEFAULT_ASYNC_INDEXING_TIME_IN_SECONDS * 1000L + BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }
}
