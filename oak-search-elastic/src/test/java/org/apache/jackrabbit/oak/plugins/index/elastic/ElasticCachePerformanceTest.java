package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.index.*;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConfig;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.*;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState.stringProperty;
import static org.apache.jackrabbit.oak.query.QueryEngineSettings.DEFAULT_QUERY_LIMIT_READS;


public class ElasticCachePerformanceTest {

    private static final String ASSET_NODE_TYPE =
            "[dam:Asset]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:TestNode VERSION";

    private static ContentSession session;
    private static QueryEngine queryEngine;

    @BeforeClass
    public static void setup() throws NoSuchWorkspaceException, LoginException, CommitFailedException {
        Properties props = loadProperties();
        String indexPrefix = props.getProperty("elasticIndexPrefix");
        long indexNameSeed = Long.parseUnsignedLong(props.getProperty("elasticIndexNameSeed").toUpperCase(), 16);
        String host = props.getProperty("elasticHost");
        String apiKeyId = props.getProperty("elasticApiKeyId");
        String apiKeySecret = props.getProperty("elasticApiKeySecret");
        String oakIndexName = "damassetlucene-13-custom-1";
        session = createRepository(indexPrefix, host, apiKeyId, apiKeySecret).login(null, null);
        var root = session.getLatestRoot();
        NodeTypeRegistry.register(root, new ByteArrayInputStream(ASSET_NODE_TYPE.getBytes()), "test nodeType");
        var damAssetRuleProps = createIndex(root, "dam:Asset", indexNameSeed, oakIndexName);
        populateIndexProperties(damAssetRuleProps);
        createSomeDamAssetsContent(root);
        root.commit();

        queryEngine = root.getQueryEngine();
    }

    @AfterClass
    public static void tearDown() throws IOException {
        session.close();
    }


    @Test
    public void perfTest() throws InterruptedException {
        var threads = new ArrayList<Thread>();
        for(int i = 0; i < 8; i++) {
            var t = new Thread(() -> {
                try {
                    load();
                } catch (ParseException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    void load() throws ParseException, InterruptedException {
        String query = "(/jcr:root/content/dam/projects/marketing/seasonal//element(*, dam:Asset)[((jcr:content/@jcr:lastModified > xs:dateTime('2025-12-18T17:12:38.000Z') and jcr:content/@jcr:lastModified <= xs:dateTime('2026-12-18T17:12:38.000Z')) and jcr:content/metadata/@dam:scene7ID)] | /jcr:root/content/dam/marketing/global-resources//element(*, dam:Asset)[((jcr:content/@jcr:lastModified > xs:dateTime('2025-12-18T17:12:38.000Z') and jcr:content/@jcr:lastModified <= xs:dateTime('2026-12-18T17:12:38.000Z')) and jcr:content/metadata/@dam:scene7ID)]) order by jcr:content/@jcr:lastModified option (index tag visualSimilaritySearch)";
        String query2 ="(/jcr:root/content/dam/projects/marketing/seasonal//element(*, dam:Asset)[((jcr:content/@jcr:lastModified > xs:dateTime('2024-12-18T17:12:38.651Z') and jcr:content/@jcr:lastModified <= xs:dateTime('2025-12-18T17:12:38.000Z')) and jcr:content/metadata/@dam:scene7ID)] | /jcr:root/content/dam/marketing/global-resources//element(*, dam:Asset)[((jcr:content/@jcr:lastModified > xs:dateTime('2024-12-18T17:12:38.651Z') and jcr:content/@jcr:lastModified <= xs:dateTime('2025-12-18T17:12:38.000Z')) and jcr:content/metadata/@dam:scene7ID)]) order by jcr:content/@jcr:lastModified option (index tag visualSimilaritySearch)";

        for (var i = 0; i < 500; i++) {
            System.out.println("Thread" + Thread.currentThread().getId() + " Iteration " + i);
            var result1 = queryEngine.executeQuery(query, "xpath", Map.of(), NO_MAPPINGS);
            result1.getRows().forEach(ResultRow::getPath);
            var result2 = queryEngine.executeQuery(query2, "xpath", Map.of(), NO_MAPPINGS);
            result2.getRows().forEach(ResultRow::getPath);

            Thread.sleep(10);
        }
    }

    private static ContentRepository createRepository(String indexPrefix, String host, String apiKeyId, String apiKeySecret) {
        var esConnection = ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(
                        "https",
                        host,
                        443
                )
                .withApiKeys(apiKeyId, apiKeySecret)
                .build();

        var nodeStore = new MemoryNodeStore();
        QueryEngineSettings queryEngineSettings = new QueryEngineSettings();
        queryEngineSettings.setInferenceEnabled(true);
        queryEngineSettings.setLimitReads(DEFAULT_QUERY_LIMIT_READS);
        queryEngineSettings.setPrefetchCount(20);
        InferenceConfig.reInitialize(nodeStore, "oak:index/:inferenceConfig", true);
        var indexTracker = new ElasticIndexTracker(esConnection,  new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexProvider indexProvider = new ElasticIndexProvider(
                indexTracker,
                ElasticIndexProvider.DEFAULT_ASYNC_ITERATOR_ENQUEUE_TIMEOUT_MS,
                ElasticIndexProvider.DEFAULT_FACETS_EVALUATION_TIMEOUT_MS,
                true);

        var initialContent = new ElasticInitialContent().getInitialContent();
        Oak oak = new Oak(nodeStore)
                .with(initialContent)
                .with(queryEngineSettings)
                .with(new OpenSecurityProvider())
                .with(indexTracker)
                .with(indexProvider)
                .with(new PropertyIndexEditorProvider())
                .with(new NodeTypeIndexProvider());
        oak.withAsyncIndexing("async", 5);
        return oak.createContentRepository();
    }

    private static class ElasticInitialContent extends InitialContent {
        protected InitialContent getInitialContent() {
            return new InitialContent() {
                @Override
                public void initialize(@NotNull NodeBuilder builder) {
                    super.initialize(builder);
                    // remove all indexes to avoid cost competition (essentially a TODO for fixing cost ES cost estimation)
                    NodeBuilder oiBuilder = builder.child(INDEX_DEFINITIONS_NAME);
                    oiBuilder.getChildNodeNames().forEach(idxName -> oiBuilder.child(idxName).remove());
                }
            };
        }
    }

    static Tree createIndex(Root root, String nodeType, long indexNameSeed, String oakIndexName) {
        ElasticIndexDefinitionBuilder builder = new ElasticIndexDefinitionBuilder();
        builder.noAsync();
        builder.evaluatePathRestrictions();
        builder.indexRule(nodeType)
                .property("title")
                .propertyIndex()
                .nodeScopeIndex().analyzed();

        Tree index = builder.build(root.getTree("/").addChild(INDEX_DEFINITIONS_NAME).addChild(oakIndexName));
        index.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
        index.setProperty("indexNameSeed", indexNameSeed);
        index.setProperty(stringProperty("tags", List.of("visualSimilaritySearch")));
        return TestUtil.newRulePropTree(index, nodeType);
    }


    private static Properties loadProperties() {
        try(InputStream is = Objects.requireNonNull(ElasticCachePerformanceTest.class.getResourceAsStream("/test.properties"))) {
            Properties props = new Properties();
            props.load(is);
            return props;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void populateIndexProperties(Tree damAssetRuleProps) {
        var lastModified = damAssetRuleProps.addChild("lastModified");
        lastModified.setProperty("jcr:primaryType", "nt:unstructured");
        lastModified.setProperty("name", "jcr:lastModified");
        lastModified.setProperty("ordered", true);
        lastModified.setProperty("propertyIndex", true);
        lastModified.setProperty("type", "Date");

        var scene7ID = damAssetRuleProps.addChild("scene7ID");
        scene7ID.setProperty("jcr:primaryType", "nt:unstructured");
        scene7ID.setProperty("name", "jcr:content/metadata/dam:scene7ID");
        scene7ID.setProperty("notNullCheckEnabled", true);
        scene7ID.setProperty("nullCheckEnabled", true);
        scene7ID.setProperty("propertyIndex", true);
    }

    private static void createSomeDamAssetsContent(Root root) {
        var paths = List.of(
                "content/dam/projects/marketing/seasonal",
                "content/dam/marketing/global-resources"
        );

        var rootTree = root.getTree("/");

        for(var rawPath: paths) {
            var path = rawPath.split("/");
            var contentDesc = rootTree;
            for (var p : path) {
                contentDesc = contentDesc.addChild(p);
            }

            for (int i = 0; i < 20; i++) {
                var asset = contentDesc.addChild("asset" + i);
                asset.setProperty("title", "asset " + i);
                asset.setProperty("jcr:primaryType", "dam:Asset");
            }
        }
    }
}
