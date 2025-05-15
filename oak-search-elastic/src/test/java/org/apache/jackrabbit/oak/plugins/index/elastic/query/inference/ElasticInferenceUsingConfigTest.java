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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import ch.qos.logback.classic.Level;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticAbstractQueryTest;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.fulltext.VectorQuery;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants.ENRICHER_CONFIG;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.inference.InferenceConstants.TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ElasticInferenceUsingConfigTest extends ElasticAbstractQueryTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticInferenceUsingConfigTest.class);

    private ScheduledExecutorService executorService;
    private StatisticsProvider statisticsProvider;

    @Rule
    public WireMockRule wireMock = new WireMockRule(WireMockConfiguration.options().dynamicPort());

    private final String defaultEnricherConfig = "{\"enricher\":{\"config\":{\"vectorSpaces\":{\"semantic\":{\"pipeline\":{\"steps\":[{\"inputFields\":{\"description\":\"STRING\",\"title\":\"STRING\"},\"chunkingConfig\":{\"enabled\":true},\"name\":\"sentence-embeddings\",\"model\":\"text-embedding-ada-002\",\"optional\":true,\"type\":\"embeddings\"}]},\"default\":false}},\"version\":\"0.0.1\"}}}";
    private final String defaultEnricherStatusMapping = "{\"properties\":{\"processingTimeMs\":{\"type\":\"date\"},\"latestError\":{\"type\":\"keyword\",\"index\":false},\"errorCount\":{\"type\":\"short\"},\"status\":{\"type\":\"keyword\"}}}";
    private final String defaultEnricherStatusData = "{\"processingTimeMs\":0,\"latestError\":\"\",\"errorCount\":0,\"status\":\"PENDING\"}";

    @Before
    public void setUp() {
        // Set system property for small metric logging interval
        System.setProperty("oak.inference.metrics.log.interval", "100");

        // Initialize StatisticsProvider for metrics testing
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Statistics-Test-Thread-" + UUID.randomUUID());
            t.setDaemon(true);
            return t;
        });
        statisticsProvider = new DefaultStatisticsProvider(executorService);
    }

    @After
    public void tearDownStatistics() {
        // Clear system property
        System.clearProperty("oak.inference.metrics.log.interval");

        if (executorService != null) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while waiting for executor service to terminate", e);
            }
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    public void inferenceConfigStoredInIndexMetadata() throws CommitFailedException, JsonProcessingException {
        String indexName = UUID.randomUUID().toString();

        // Setup inference configuration with multiple models
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder nodeBuilder = rootBuilder;
        for (String path : PathUtils.elements(INFERENCE_CONFIG_PATH)) {
            nodeBuilder = nodeBuilder.child(path);
        }
        nodeBuilder.setProperty(TYPE, InferenceConfig.TYPE);
        nodeBuilder.setProperty(InferenceConstants.ENABLED, true);
        NodeBuilder inferenceConfig = nodeBuilder;

        // Add enricherStatus config
        NodeBuilder enricherStatusConfig = inferenceConfig.child(InferenceConstants.ENRICH_NODE);
        enricherStatusConfig.setProperty(InferenceConstants.ENRICHER_STATUS_MAPPING, defaultEnricherStatusMapping);
        enricherStatusConfig.setProperty(InferenceConstants.ENRICHER_STATUS_DATA, defaultEnricherStatusData);

        // Add inferenceIndexConfig
        NodeBuilder inferenceIndexConfig = inferenceConfig.child(indexName);
        inferenceIndexConfig.setProperty(TYPE, InferenceIndexConfig.TYPE);
        inferenceIndexConfig.setProperty(ENRICHER_CONFIG, defaultEnricherConfig);
        inferenceIndexConfig.setProperty(InferenceConstants.ENABLED, true);

        // Setup two inference models to verify multiple model configurations
        setupInferenceModelConfig(inferenceIndexConfig, "inferenceModel1", "test-model1",
            "http://localhost:8080", 0.8, 3L, true, true,
            Map.of("headerKey1_1", "headerValue1_1", "headerKey2_1", "headerValue2_1"),
            Map.of("textKey", "text1", "dimension", 1536, "model", "model-name-of-inference-model1"));

        setupInferenceModelConfig(inferenceIndexConfig, "inferenceModel2", "test-model2",
            "http://localhost:8080", 0.8, 3L, false, true,
            Map.of("headerKey1_2", "headerValue1_2", "headerKey2_2", "headerValue2_2"),
            Map.of("textKey", "searchString2", "model", "model-name-of-inference-model2"));

        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        Tree index = setIndex(indexName, builder);
        root.commit();

        IndexMappingRecord mapping = getMapping(index);
        Map<String, JsonData> meta = mapping.mappings().meta();
        assertNotNull(meta);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode1 = objectMapper.readTree(defaultEnricherConfig).get("enricher");
        JsonNode jsonNode2 = objectMapper.readTree(meta.get("enricher").toJson().toString());
        assertEquals(jsonNode1, jsonNode2);
    }

    /**
     * Helper method to setup an inference model configuration.
     */
    private void setupInferenceModelConfig(NodeBuilder inferenceIndexConfig,
                                           String configName, String modelName,
                                           String serviceUrl, double threshold,
                                           long minTerms, boolean isDefault, boolean isEnabled,
                                           Map<String, String> headers,
                                           Map<String, Object> payloadConfig) {
        // Add inference model configuration
        NodeBuilder modelConfig = inferenceIndexConfig.child(configName);
        modelConfig.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        modelConfig.setProperty(InferenceModelConfig.MODEL, modelName);
        modelConfig.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, serviceUrl);
        modelConfig.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, threshold);
        modelConfig.setProperty(InferenceModelConfig.MIN_TERMS, minTerms);
        modelConfig.setProperty(InferenceModelConfig.IS_DEFAULT, isDefault);
        modelConfig.setProperty(InferenceModelConfig.ENABLED, isEnabled);

        // Setup header configuration
        NodeBuilder header = modelConfig.child(InferenceModelConfig.HEADER);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            header.setProperty(entry.getKey(), entry.getValue());
        }

        // Setup payload configuration
        NodeBuilder payload = modelConfig.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        for (Map.Entry<String, Object> entry : payloadConfig.entrySet()) {
            if (entry.getValue() instanceof String) {
                payload.setProperty(entry.getKey(), (String) entry.getValue());
            } else if (entry.getValue() instanceof Integer) {
                payload.setProperty(entry.getKey(), (Integer) entry.getValue());
            } else if (entry.getValue() instanceof Long) {
                payload.setProperty(entry.getKey(), (Long) entry.getValue());
            }
        }
    }

    @Test
    public void testHybridSearchWithVectorQueryConfigJson() throws Exception {
        // Test hybrid search with inference configuration
        hybridSearch("?{\"inferenceModelConfig\": \"ada-test-model\"}?");
    }

    @Test
    public void testHybridSearchWithEmptyVectorQueryConfigJson() throws Exception {
        // Test hybrid search with empty inference configuration
        hybridSearch("?{}?");
    }

    @Ignore
    @Test
    public void testHybridSearchWithExperimentalPrefix() throws Exception {
        enableExperimentalInferenceCompatibility();
        // Test hybrid search with experimental inference query prefix
        hybridSearch("?");
        System.clearProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY);
    }

    private void enableExperimentalInferenceCompatibility() {
        System.setProperty(VectorQuery.EXPERIMENTAL_COMPATIBILITY_MODE_KEY, "true");
    }

    private void hybridSearch(String inferenceConfigInQuery) throws Exception {
        String jcrIndexName = UUID.randomUUID().toString();
        String inferenceServiceUrl = "http://localhost:" + wireMock.port() + "/v1/embeddings";
        String inferenceModelConfigName = "ada-test-model";
        String inferenceModelName = "text-embedding-ada-002";

        // Setup log customizer to capture InferenceServiceMetrics logs
        LogCustomizer logCustomizer = LogCustomizer
            .forLogger(InferenceServiceMetrics.class.getName())
            .enable(Level.INFO)
            .enable(Level.DEBUG)
            .contains("Inference service metrics")
            .create();
        logCustomizer.starting();

        try {
            // Create inference config
            createInferenceConfig(jcrIndexName, true, defaultEnricherConfig, inferenceModelConfigName,
                inferenceModelName, inferenceServiceUrl, 0.8, 1L, true, true);
            setupEnricherStatus(defaultEnricherStatusMapping, defaultEnricherStatusData);
            // Create index definition with multiple properties
            IndexDefinitionBuilder builder = createIndexDefinition("title", "description", "updatedBy");
            Tree index = setIndex(jcrIndexName, builder);
            root.commit();

            // Add test content
            addTestContent();

            // Let the index catch up
            assertEventually(() -> assertEquals(7, countDocuments(index)));

            // Enrich documents with embeddings
            setupEmbeddingsForContent(index, inferenceModelConfigName, inferenceModelName);

            // Setup wiremock stubs for inference service
            setupMockInferenceService(inferenceModelConfigName, jcrIndexName);

            // Test query results
            Map<String, String> queryResults = Map.of(
                "a beginner guide to data manipulation in python", "/content/programming",
                "how to improve mental health through exercises", "/content/yoga",
                "nutritional advice for a healthier lifestyle", "/content/health",
                "technological advancements in electric vehicles", "/content/cars",
                "what are the key algorithms used in machine learning", "/content/ml"
            );

            // Verify all queries return expected results
            assertEventually(() -> {
                verifyQueryResults(queryResults, inferenceConfigInQuery, jcrIndexName);

                // Test error handling scenarios
                verifyErrorHandling(jcrIndexName, inferenceConfigInQuery);
            });

            // Test that inference data persists through document updates
            testInferenceDataPersistenceOnUpdate(index);

            // Create and verify metrics directly with our statisticsProvider
            InferenceServiceMetrics directMetrics = new InferenceServiceMetrics(statisticsProvider,
                "test-metrics",
                100);

            // Set reasonable counter values
            CounterStats counter = statisticsProvider.getCounterStats("test-metrics_" + InferenceServiceMetrics.TOTAL_REQUESTS,
                StatsOptions.DEFAULT);
            counter.inc(10);

            // Log metrics with the counts
            directMetrics.logMetricsSummary();

            LOG.info("Successfully logged basic metrics");

            // Verify that we have captured the metrics logs
            Thread.sleep(500); // Give a small delay for logging to complete
            verifyMetricsLogsPresent(logCustomizer);
        } finally {
            logCustomizer.finished();
        }
    }

    /**
     * Verifies that metrics logs were captured by the LogCustomizer.
     *
     * @param logCustomizer The LogCustomizer instance used to capture logs
     */
    private void verifyMetricsLogsPresent(LogCustomizer logCustomizer) {
        List<String> logs = logCustomizer.getLogs();
        assertFalse("Should have captured metrics logs", logs.isEmpty());

        LOG.info("Captured {} metrics log entries", logs.size());

        // At least one log should contain the metrics information
        boolean foundMetricsLog = false;

        for (String log : logs) {
            if (log.contains("Inference service metrics")) {
                foundMetricsLog = true;

                // Verify it contains some of the expected metrics
                assertTrue("Log should contain request count",
                    log.contains("requests="));
                assertTrue("Log should contain cache hit rate",
                    log.contains("hitRate="));
                assertTrue("Log should contain error rate",
                    log.contains("errorRate="));

                LOG.info("Found metrics log: {}", log);
                break;
            }
        }

        assertTrue("Should have found at least one metrics log entry", foundMetricsLog);
    }

    /**
     * Adds test content for the hybrid search test.
     */
    private void addTestContent() throws CommitFailedException {
        Tree content = root.getTree("/").addChild("content");

        // Health content
        Tree health = content.addChild("health");
        health.setProperty("title", "Healthy Eating for a Balanced Life");
        health.setProperty("description", "This article discusses how a well-balanced diet can lead to better health outcomes. It covers the importance of fruits, vegetables, lean proteins, and whole grains.");

        // Cars content
        Tree cars = content.addChild("cars");
        cars.setProperty("title", "The Future of Electric Cars");
        cars.setProperty("description", "Electric vehicles are revolutionizing the automobile industry. This paper explores advancements in battery technology, charging infrastructure, and sustainability.");

        // Programming content
        Tree programming = content.addChild("programming");
        programming.setProperty("title", "Mastering Python for Data Science");
        programming.setProperty("description", "A comprehensive guide to using Python for data science projects. Topics include data manipulation, visualization, and machine learning algorithms like decision trees and neural networks.");

        // Machine learning content
        Tree ml = content.addChild("ml");
        ml.setProperty("title", "Introduction to Machine Learning");
        ml.setProperty("description", "This book introduces machine learning concepts, focusing on supervised and unsupervised learning techniques. It covers algorithms like linear regression, k-means clustering, and support vector machines.");

        // Yoga content
        Tree yoga = content.addChild("yoga");
        yoga.setProperty("title", "Yoga for Mental Wellness");
        yoga.setProperty("description", "The benefits of yoga for mental health are vast. This study shows how practicing yoga can reduce stress, anxiety, and improve overall well-being through breathing techniques and meditation.");

        // Farm content - not enriched with embeddings
        Tree farm = content.addChild("farm");
        farm.setProperty("title", "Sustainable Farming Practices");
        farm.setProperty("description", "Sustainable farming practices are essential for preserving the environment. This article discusses crop rotation, soil health, and water conservation methods to reduce the carbon footprint of agriculture.");

        root.commit();
    }

    /**
     * Sets up embeddings for content based on JSON files.
     */
    private void setupEmbeddingsForContent(Tree index, String inferenceModelConfigName, String inferenceModelName) throws Exception {
        ObjectMapper mapper = new JsonMapper();
        List<String> paths = executeQuery("select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and title is not null", SQL2);

        for (String path : paths) {
            URL json = this.getClass().getResource("/inferenceUsingConfig" + path + ".json");
            if (json != null) {
                Map<String, Collection<Double>> map = mapper.readValue(json, Map.class);
                ObjectNode updateDoc = mapper.createObjectNode();
                List<Float> embeddings = map.get("embedding").stream()
                    .map(d -> ((Double) d).floatValue())
                    .collect(Collectors.toList());

                VectorDocument vectorDocument = new VectorDocument(
                    UUID.randomUUID().toString(),
                    embeddings,
                    Map.of("updatedAt", Instant.now().toEpochMilli(), "model", inferenceModelName)
                );

                ObjectNode vectorSpacesNode = updateDoc.putObject(InferenceConstants.VECTOR_SPACES);
                ArrayNode inferenceModelConfigNode = vectorSpacesNode.putArray(inferenceModelConfigName);
                inferenceModelConfigNode.addPOJO(vectorDocument);

                updateDocument(index, path, updateDoc);
            }
        }
    }

    /**
     * Sets up mock responses for the inference service.
     */
    private void setupMockInferenceService(String inferenceModelConfigName, String jcrIndexName) throws Exception {
        try (Stream<Path> stream = Files.walk(Paths.get(this.getClass().getResource("/inferenceUsingConfig/queries").toURI()))) {
            stream.filter(Files::isRegularFile).forEach(queryFile -> {
                String query = FilenameUtils.removeExtension(queryFile.getFileName().toString()).replaceAll("_", " ");
                String payload = InferenceConfig.getInstance()
                    .getInferenceModelConfig(jcrIndexName, inferenceModelConfigName)
                    .getPayload()
                    .getInferencePayload(query);

                if (queryFile.toAbsolutePath().toString().contains("queries/faulty")) {
                    // Mock server error response
                    wireMock.stubFor(WireMock.post("/v1/embeddings")
                        .withRequestBody(WireMock.equalToJson(payload))
                        .willReturn(WireMock.serverError()));
                } else if (queryFile.toAbsolutePath().toString().contains("delayed")) {
                    // Mock delayed response
                    wireMock.stubFor(WireMock.post("/v1/embeddings")
                        .withRequestBody(WireMock.equalToJson(payload))
                        .willReturn(WireMock.ok()
                            .withHeader("Content-Type", "application/json")
                            .withBody("[]")
                            .withFixedDelay(6000)));
                } else {
                    // Mock normal response
                    String json;
                    try {
                        json = IOUtils.toString(queryFile.toUri(), StandardCharsets.UTF_8);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    wireMock.stubFor(WireMock.post("/v1/embeddings")
                        .withRequestBody(WireMock.equalToJson(payload))
                        .willReturn(WireMock.ok()
                            .withHeader("Content-Type", "application/json")
                            .withBody(json)));
                }
            });
        }
    }

    /**
     * Verifies that queries return expected results.
     */
    private void verifyQueryResults(Map<String, String> queryResults, String inferenceConfigInQuery, String jcrIndexName) {
        for (Map.Entry<String, String> entry : queryResults.entrySet()) {
            String query = entry.getKey();
            String expectedPath = entry.getValue();

            // Test with inference config
            String queryPath = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '"
                + inferenceConfigInQuery + query + "')";
            List<String> results = executeQuery(queryPath, SQL2, true, true);
            assertEquals(expectedPath, results.get(0));

            // Test without inference config
            String queryPath2 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '" + query + "')";
            assertQuery(queryPath2, List.of());
        }
    }

    /**
     * Verifies error handling in queries.
     */
    private void verifyErrorHandling(String jcrIndexName, String inferenceConfigInQuery) {
        // Test server error handling
        String queryPath3 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '"
            + inferenceConfigInQuery  + "machine learning')";
        assertQuery(queryPath3, List.of("/content/ml", "/content/programming"));

        // Test timeout handling
        String queryPath4 = "select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and contains(*, '"
            + inferenceConfigInQuery + "farming practices')";
        assertQuery(queryPath4, List.of("/content/farm"));
    }

    /**
     * Tests that inference data persists after document updates.
     */
    private void testInferenceDataPersistenceOnUpdate(Tree index) throws CommitFailedException {
        ObjectNode carsDoc = getDocument(index, "/content/cars");
        assertNotNull(carsDoc.get(InferenceConstants.VECTOR_SPACES));

        // Update document property
        root.getTree("/content/cars").setProperty("updatedBy", "John Doe");
        root.commit();

        // Verify property was updated and inference data preserved
        assertEventually(() -> {
            assertQuery("select [jcr:path] from [nt:base] where ISDESCENDANTNODE('/content') and updatedBy = 'John Doe'",
                List.of("/content/cars"));

            ObjectNode carsDocUpdated = getDocument(index, "/content/cars");
            assertNotNull(carsDocUpdated.get(InferenceConstants.VECTOR_SPACES));
        });
    }

    /**
     * Creates inference configuration with the specified parameters.
     */
    private void createInferenceConfig(String indexName, boolean isInferenceConfigEnabled,
                                       String enricherConfig, String inferenceModelConfigName,
                                       String inferenceModelName, String embeddingServiceUrl,
                                       Double similarityThreshold, long minTerms, boolean isDefaultInferenceModelConfig,
                                       boolean isInferenceModelConfigEnabled) throws CommitFailedException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder nodeBuilder = rootBuilder;
        for (String path : PathUtils.elements(INFERENCE_CONFIG_PATH)) {
            nodeBuilder = nodeBuilder.child(path);
        }
        nodeBuilder.setProperty(TYPE, InferenceConfig.TYPE);
        nodeBuilder.setProperty(InferenceConstants.ENABLED, isInferenceConfigEnabled);
        NodeBuilder inferenceConfig = nodeBuilder;

        // Add inferenceIndexConfig
        NodeBuilder inferenceIndexConfig = inferenceConfig.child(indexName);
        inferenceIndexConfig.setProperty(TYPE, InferenceIndexConfig.TYPE);
        inferenceIndexConfig.setProperty(ENRICHER_CONFIG, enricherConfig);
        inferenceIndexConfig.setProperty(InferenceConstants.ENABLED, true);
        // Add inference model1 configuration
        NodeBuilder inferenceModelConfig1 = inferenceIndexConfig.child(inferenceModelConfigName);
        inferenceModelConfig1.setProperty(InferenceConstants.TYPE, InferenceModelConfig.TYPE);
        inferenceModelConfig1.setProperty(InferenceModelConfig.MODEL, inferenceModelName);
        inferenceModelConfig1.setProperty(InferenceModelConfig.EMBEDDING_SERVICE_URL, embeddingServiceUrl);
        inferenceModelConfig1.setProperty(InferenceModelConfig.SIMILARITY_THRESHOLD, similarityThreshold);
        inferenceModelConfig1.setProperty(InferenceModelConfig.MIN_TERMS, minTerms);
        inferenceModelConfig1.setProperty(InferenceModelConfig.IS_DEFAULT, isDefaultInferenceModelConfig);
        inferenceModelConfig1.setProperty(InferenceModelConfig.ENABLED, isInferenceModelConfigEnabled);

        // Setup header configuration
        NodeBuilder header1 = inferenceModelConfig1.child(InferenceModelConfig.HEADER);
        header1.setProperty("Content-Type", "application/json");

        // Setup payload configuration
        NodeBuilder payload1 = inferenceModelConfig1.child(InferenceModelConfig.INFERENCE_PAYLOAD);
        payload1.setProperty("input", List.of(""), Type.STRINGS);
        payload1.setProperty("model", "text-embedding-ada-002");
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void testEnricherStatus() throws Exception {
        String jcrIndexName = UUID.randomUUID().toString();
        String inferenceServiceUrl = "http://localhost:" + wireMock.port() + "/v1/embeddings";
        String inferenceModelConfigName = "ada-test-model";
        String inferenceModelName = "text-embedding-ada-002";

        // Create inference config with enricher information
        createInferenceConfig(jcrIndexName, true, defaultEnricherConfig, inferenceModelConfigName,
            inferenceModelName, inferenceServiceUrl, 0.8, 1L, true, true);

        String enricherStatusData = "{\"status\":\"PENDING\"}";
        setupEnricherStatus(defaultEnricherStatusMapping, enricherStatusData);

        // Create index
        Tree index = setIndex(jcrIndexName, createIndexDefinition("title", "description"));
        root.commit();

        // Add content
        Tree content = root.getTree("/").addChild("content");
        Tree document = content.addChild("document");
        document.setProperty("title", "Test Document");
        document.setProperty("description", "This is a test document to verify enricher status is included in document updates.");
        root.commit();

        // Let the index catch up
        assertEventually(() -> assertEquals(2, countDocuments(index)));

        // Add another property to trigger an update
        document.setProperty("updatedAt", Instant.now().toString());
        root.commit();

        // Verify enricher status in the document
        verifyEnricherStatus(index, "/content/document", enricherStatusData);
    }

    @Test
    public void testEnricherStatusPreservedWithVectorEmbeddings() throws Exception {
        String jcrIndexName = UUID.randomUUID().toString();
        String inferenceServiceUrl = "http://localhost:" + wireMock.port() + "/v1/embeddings";
        String inferenceModelConfigName = "ada-test-model";
        String inferenceModelName = "text-embedding-ada-002";

        // Create inference config
        createInferenceConfig(jcrIndexName, true, defaultEnricherConfig, inferenceModelConfigName,
            inferenceModelName, inferenceServiceUrl, 0.8, 1L, true, true);

        String enricherStatusData = "{\"status\":\"PENDING\"}";
        setupEnricherStatus(defaultEnricherStatusMapping, enricherStatusData);

        // Create index
        Tree index = setIndex(jcrIndexName, createIndexDefinition("title", "description", "updatedBy"));
        root.commit();

        // Add content
        Tree content = root.getTree("/").addChild("content");
        Tree document = content.addChild("document");
        document.setProperty("title", "Test Document with Embeddings");
        document.setProperty("description", "This is a test document that will have vector embeddings and enricher status.");
        root.commit();

        // Let the index catch up
        assertEventually(() -> assertEquals(2, countDocuments(index)));

        // Create an update with vector embeddings
        List<Float> embeddings = List.of(0.1f, 0.2f, 0.3f, 0.4f, 0.5f);
        createDocumentWithEmbeddings(index, "/content/document", inferenceModelConfigName, inferenceModelName, embeddings);

        // Verify the document has the embeddings
        assertEventually(() -> {
            ObjectNode docWithEmbeddings = getDocument(index, "/content/document");
            assertNotNull(docWithEmbeddings.get(InferenceConstants.VECTOR_SPACES));
            JsonNode vectorSpaces = docWithEmbeddings.get(InferenceConstants.VECTOR_SPACES);
            assertNotNull(vectorSpaces.get(inferenceModelConfigName));
        });

        // Update a property to trigger another update
        document.setProperty("updatedBy", "Test User");
        root.commit();

        // Verify both embeddings and enricher status
        assertEventually(() -> {
            ObjectNode updatedDoc = getDocument(index, "/content/document");

            // Check vector embeddings
            assertNotNull(updatedDoc.get(InferenceConstants.VECTOR_SPACES));
            JsonNode vectorSpaces = updatedDoc.get(InferenceConstants.VECTOR_SPACES);
            assertNotNull(vectorSpaces.get(inferenceModelConfigName));

            // Check enricher status
            assertNotNull(updatedDoc.get(InferenceConstants.ENRICH_NODE));

            // Check updated property
            assertEquals("Test User", updatedDoc.get("updatedBy").asText());
        });

        // Verify enricher status in detail
        verifyEnricherStatus(index, "/content/document", enricherStatusData);
    }

    @Test
    public void testEnricherStatusOnReinitialization() throws Exception {
        String jcrIndexName = UUID.randomUUID().toString();
        String inferenceServiceUrl = "http://localhost:" + wireMock.port() + "/v1/embeddings";
        String inferenceModelConfigName = "ada-test-model";
        String inferenceModelName = "text-embedding-ada-002";

        // Create inference config
        createInferenceConfig(jcrIndexName, true, defaultEnricherConfig, inferenceModelConfigName,
            inferenceModelName, inferenceServiceUrl, 0.8, 1L, true, true);

        // Set up initial enricher status
        long initialTime = System.currentTimeMillis();
        Map<String, Object> initialStatus = Map.of(
            "lastUpdated", initialTime,
            "status", "initializing",
            "documentsProcessed", 0L
        );
        String enricherStatusData = "{\"status\":\"PENDING\"}";
        setupEnricherStatus(defaultEnricherStatusMapping, enricherStatusData);

        // Force reinitialization of InferenceConfig
        InferenceConfig.reInitialize();

        // Verify initial enricher status
        Map<String, Object> retrievedInitialStatus = InferenceConfig.getInstance().getEnricherStatus();
        assertNotNull(retrievedInitialStatus);
        assertEquals("PENDING", retrievedInitialStatus.get("status"));

        // Update enricher status with new values
        String updatedStatusData = "{\"status\":\"PENDING2\"}";
        setupEnricherStatus(defaultEnricherStatusMapping, updatedStatusData);

        // Force reinitialization of InferenceConfig
        InferenceConfig.reInitialize();

        // Verify updated enricher status
        Map<String, Object> retrievedUpdatedStatus = InferenceConfig.getInstance().getEnricherStatus();
        assertNotNull(retrievedUpdatedStatus);
        assertEquals("PENDING2", retrievedUpdatedStatus.get("status"));

        // Create an index
        Tree index = setIndex(jcrIndexName, createIndexDefinition("title"));
        root.commit();

        // Add content
        Tree content = root.getTree("/").addChild("content");
        Tree document = content.addChild("document");
        Tree document2 = content.addChild("document2");
        document.setProperty("title", "Test Document for Reinitialization");
        document2.setProperty("title", "Test Document for Reinitialization 2");
        root.commit();

        // Let the index catch up
        assertEventually(() -> assertEquals(3, countDocuments(index)));

        // Verify the enricher status in the indexed document
        verifyEnricherStatus(index, "/content/document", updatedStatusData);
        verifyEnricherStatus(index, "/content/document2", updatedStatusData);
    }

    /**
     * Sets up the enricher status with the specified parameters.
     */
    private void setupEnricherStatus(String enricherStatusMapping, String enricherStatusData) throws CommitFailedException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder nodeBuilder = rootBuilder;
        for (String path : PathUtils.elements(INFERENCE_CONFIG_PATH)) {
            nodeBuilder = nodeBuilder.child(path);
        }

        // Add enricher status node
        NodeBuilder enrichNode = nodeBuilder.child(InferenceConstants.ENRICH_NODE);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_DATA, enricherStatusData);
        enrichNode.setProperty(InferenceConstants.ENRICHER_STATUS_MAPPING, enricherStatusMapping);
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * Creates an index definition.
     */
    private IndexDefinitionBuilder createIndexDefinition(String... properties) {
        IndexDefinitionBuilder builder = createIndex();
        builder.includedPaths("/content");

        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule("nt:base");
        for (String property : properties) {
            indexRule.property(property).propertyIndex().analyzed().nodeScopeIndex();
        }

        return builder;
    }

    /**
     * Verifies that a document contains the expected enricher status.
     */
    private void verifyEnricherStatus(Tree index, String path, String expectedEnricherStatusData) {
        assertEventually(() -> {
            ObjectNode docNode = getDocument(index, path);
            assertNotNull(docNode.get(InferenceConstants.ENRICH_NODE));
            JsonNode enrichNodeData = docNode.get(InferenceConstants.ENRICH_NODE);
            assertEquals(expectedEnricherStatusData, enrichNodeData.toString());
        });
    }

    /**
     * Creates a document with vector embeddings.
     */
    private void createDocumentWithEmbeddings(Tree index, String path, String inferenceModelConfigName,
                                              String inferenceModelName, List<Float> embeddings) throws IOException {
        ObjectMapper mapper = new JsonMapper();
        ObjectNode updateDoc = mapper.createObjectNode();
        VectorDocument vectorDocument = new VectorDocument(UUID.randomUUID().toString(), embeddings,
            Map.of("updatedAt", Instant.now().toEpochMilli(), "model", inferenceModelName));
        ObjectNode vectorSpacesNode = updateDoc.putObject(InferenceConstants.VECTOR_SPACES);
        ArrayNode inferenceModelConfigNode = vectorSpacesNode.putArray(inferenceModelConfigName);
        inferenceModelConfigNode.addPOJO(vectorDocument);

        updateDocument(index, path, updateDoc);
    }

    /**
     * Test metrics class that uses unique metric names to avoid conflicts
     */
    private static class TestMetricsWithUniqueNames extends InferenceServiceMetrics {
        private final String uniquePrefix = "test_" + UUID.randomUUID().toString().replace("-", "_");

        public TestMetricsWithUniqueNames(StatisticsProvider statisticsProvider) {
            super(statisticsProvider, "test-unique-metrics", 100);
        }

        @Override
        protected String getMetricName(String baseName) {
            return uniquePrefix + "_" + baseName;
        }
    }
}
