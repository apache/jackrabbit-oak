package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.TestUtils;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
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
import java.io.IOException;
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

    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static final String elasticConnectionString = System.getProperty("elasticConnectionString");

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    private Session adminSession;
    private QueryManager qe;

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        adminSession.logout();
        elasticRule.closeElasticConnection();
    }

    @Before
    public void setup() throws Exception {
        createRepository();
    }

    private void createRepository() throws RepositoryException {
        ElasticConnection connection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(connection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);

        Oak oak = new Oak(nodeStore)
                .with(new OpenSecurityProvider())
                .with(editorProvider)
                .with((Observer) indexProvider)
                .with((QueryIndexProvider) indexProvider)
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

        Node indexNode = adminSession.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(indexName);
        Node b = indexNode.getNode("indexRules").getNode("nt:base").getNode("properties").addNode("b");
        b.setProperty(FulltextIndexConstants.PROP_PROPERTY_INDEX, true);
        b.setProperty(FulltextIndexConstants.PROP_ANALYZED, true);
        // Now we reindex and see everything works fine
        indexNode.setProperty(REINDEX_PROPERTY_NAME, true);
        adminSession.save();

        assertQuery("select [jcr:path] from [nt:base] where contains(b, 'bar')", 100);
    }

    private void assertQuery(String query, int resultSetSize) {
        TestUtils.assertEventually(() -> {
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
