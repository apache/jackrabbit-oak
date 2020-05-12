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
package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import com.github.dockerjava.api.DockerClient;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.index.ElasticsearchIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.query.ElasticsearchIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.elasticsearch.util.ElasticsearchIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.elasticsearch.Version;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.jcr.GuestCredentials;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.security.Privilege;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elasticsearch.ElasticsearchIndexDefinition.BULK_FLUSH_INTERVAL_MS_DEFAULT;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class ElasticsearchFacetTest {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchFacetTest.class);
    private static final String FACET_PROP = "facets";
    private Closer closer;
    private Session session;
    private QueryManager qe;
    Repository repository;
    private Node indexNode;
    private static String TEST_INDEX = "testIndex";
    private static final int NUM_LEAF_NODES = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
    private static final int NUM_LABELS = 4;
    private static final int NUM_LEAF_NODES_FOR_LARGE_DATASET = NUM_LEAF_NODES;
    private static final int NUM_LEAF_NODES_FOR_SMALL_DATASET = NUM_LEAF_NODES / (2 * NUM_LABELS);
    private Map<String, Integer> actualLabelCount = Maps.newHashMap();
    private Map<String, Integer> actualAclLabelCount = Maps.newHashMap();
    private Map<String, Integer> actualAclPar1LabelCount = Maps.newHashMap();


    @Rule
    public final ElasticsearchContainer elastic =
            new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:" + Version.CURRENT);

    @BeforeClass
    public static void beforeMethod() {
        DockerClient client = null;
        try {
            client = DockerClientFactory.instance().client();
        } catch (Exception e) {
            LOG.warn("Docker is not available, ElasticsearchPropertyIndexTest will be skipped");
        }
        assumeNotNull(client);
    }

    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
        createIndex();
        indexNode = session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).getNode(TEST_INDEX);
    }

    private void createRepository() throws RepositoryException {
        ElasticsearchConnection connection = ElasticsearchConnection.newBuilder()
                .withIndexPrefix("" + System.nanoTime())
                .withConnectionParameters(
                        ElasticsearchConnection.DEFAULT_SCHEME,
                        elastic.getContainerIpAddress(),
                        elastic.getMappedPort(ElasticsearchConnection.DEFAULT_PORT)
                ).build();
        ElasticsearchIndexEditorProvider editorProvider = new ElasticsearchIndexEditorProvider(connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        ElasticsearchIndexProvider indexProvider = new ElasticsearchIndexProvider(connection);

        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with(editorProvider)
                .with(indexProvider);

        Jcr jcr = new Jcr(oak);
        repository = jcr.createRepository();

        try {
            session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        } catch (RepositoryException e) {
            throw e;
        }
        closer.register(session::logout);

        // we'd always query anonymously
        Session anonSession = null;
        try {
            anonSession = repository.login(new GuestCredentials(), null);
            anonSession.refresh(true);
            anonSession.save();
        } catch (RepositoryException e) {
            throw e;
        }
        closer.register(anonSession::logout);
        try {
            qe = anonSession.getWorkspace().getQueryManager();
        } catch (RepositoryException e) {
            throw e;
        }
    }


    private class IndexSkeleton {
        IndexDefinitionBuilder indexDefinitionBuilder;
        IndexDefinitionBuilder.IndexRule indexRule;

        private IndexSkeleton() throws RepositoryException {
        }

        void initialize() {
            initialize(JcrConstants.NT_BASE);
        }

        void initialize(String nodeType) {
            indexDefinitionBuilder = new ElasticsearchIndexDefinitionBuilder();
            indexRule = indexDefinitionBuilder.indexRule(nodeType);
        }

        void build() throws RepositoryException {
            indexDefinitionBuilder.build(session.getRootNode().getNode(INDEX_DEFINITIONS_NAME).addNode(TEST_INDEX));
        }
    }

    private void createIndex() throws RepositoryException {
        IndexSkeleton indexSkeleton = new IndexSkeleton();
        indexSkeleton.initialize();
        indexSkeleton.indexDefinitionBuilder.noAsync().evaluatePathRestrictions();
        indexSkeleton.indexRule.property("cons").propertyIndex();
        indexSkeleton.indexRule.property("foo").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.indexRule.property("bar").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexSkeleton.build();
    }

    private void createDataset(int numberOfLeafNodes) throws RepositoryException {
        Random rGen = new Random(42);
        Random rGen1 = new Random(42);
        int[] foolabelCount = new int[NUM_LABELS];
        int[] fooaclLabelCount = new int[NUM_LABELS];
        int[] fooaclPar1LabelCount = new int[NUM_LABELS];

        int[] barlabelCount = new int[NUM_LABELS];
        int[] baraclLabelCount = new int[NUM_LABELS];
        int[] baraclPar1LabelCount = new int[NUM_LABELS];

        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));

        for (int i = 0; i < NUM_LABELS; i++) {
            Node subPar = par.addNode("par" + i);
            for (int j = 0; j < numberOfLeafNodes; j++) {
                Node child = subPar.addNode("c" + j);
                child.setProperty("cons", "val");
                // Add a random label out of "l0", "l1", "l2", "l3"
                int foolabelNum = rGen.nextInt(NUM_LABELS);
                int barlabelNum = rGen1.nextInt(NUM_LABELS);
                child.setProperty("foo", "l" + foolabelNum);
                child.setProperty("bar", "m" + barlabelNum);

                foolabelCount[foolabelNum]++;
                barlabelCount[barlabelNum]++;
                if (i != 0) {
                    fooaclLabelCount[foolabelNum]++;
                    baraclLabelCount[barlabelNum]++;
                }
                if (i == 1) {
                    fooaclPar1LabelCount[foolabelNum]++;
                    baraclPar1LabelCount[barlabelNum]++;
                }
            }

            // deny access for one sub-parent
            if (i == 0) {
                deny(subPar);
            }
        }
        session.save();
        for (int i = 0; i < foolabelCount.length; i++) {
            actualLabelCount.put("l" + i, foolabelCount[i]);
            actualLabelCount.put("m" + i, barlabelCount[i]);
            actualAclLabelCount.put("l" + i, fooaclLabelCount[i]);
            actualAclLabelCount.put("m" + i, baraclLabelCount[i]);
            actualAclPar1LabelCount.put("l" + i, fooaclPar1LabelCount[i]);
            actualAclPar1LabelCount.put("m" + i, baraclPar1LabelCount[i]);
        }
        assertNotEquals("Acl-ed and actual counts mustn't be same", actualLabelCount, actualAclLabelCount);
    }

    @Test
    public void secureFacets() throws Exception {
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        assertEventually(() -> {
            assertEquals(actualAclLabelCount, getFacets());
        });
    }

    @Test
    public void secureFacets_withOneLabelInaccessible() throws Exception {
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        Node inaccessibleChild = deny(session.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        session.save();
        assertEventually(() -> {
            assertEquals(actualAclLabelCount, getFacets());
        });
    }

    @Test
    public void insecureFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        session.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        assertEventually(() -> {
            assertEquals(actualLabelCount, getFacets());
        });
    }

    @Test
    public void statisticalFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        facetConfig.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 3000);
        session.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);

        assertEventually(() -> {
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), getFacets().size());
        });

        for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            assertEventually(() -> {
                int facetCount = facetCount = getFacets().get(facetLabel);
                float ratio = ((float) facetCount) / facet.getValue();
                assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                                "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                        Math.abs(ratio - 1) < 0.1);
            });
        }
    }

    @Test
    public void statisticalFacetsWithHitCountLessThanSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createDataset(NUM_LEAF_NODES_FOR_SMALL_DATASET);

        assertEventually(() -> {
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), getFacets().size());
        });

        // Since the hit count is less than sample size -> flow should have switched to secure facet count instead of statistical
        // and thus the count should be exactly equal
        assertEventually(() -> {
            assertEquals(actualAclLabelCount, getFacets());
        });
    }

    /*
    Currently we are not adding path restrictions in elastic query and filtering paths later from results.
    We will need path restrictions in elastic query itself to get right facet results.
     */
    @Test
    public void statisticalFacets_withHitCountSameAsSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);

        Map<String, Integer> facets = getFacets("/parent/par1");
        assertEquals("Unexpected number of facets", actualAclPar1LabelCount.size(), facets.size());

        for (Map.Entry<String, Integer> facet : actualAclPar1LabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            int facetCount = facets.get(facetLabel);
            float ratio = ((float) facetCount) / facet.getValue();
            assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                            "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                    Math.abs(ratio - 1) < 0.1);
        }
    }

    @Test
    public void statisticalFacets_withOneLabelInaccessible() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        Node inaccessibleChild = deny(session.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        session.save();
        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());
        });

        for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {

            assertEventually(() -> {
                String facetLabel = facet.getKey();
                int facetCount = getFacets().get(facetLabel);
                float ratio = ((float) facetCount) / facet.getValue();
                assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                                "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                        Math.abs(ratio - 1) < 0.1);
            });
        }
    }

    @Test
    public void secureFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        qe = session.getWorkspace().getQueryManager();
        assertEventually(() -> {
            assertEquals(actualLabelCount, getFacets());
        });
    }

    @Test
    public void statisticalFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        qe = session.getWorkspace().getQueryManager();
        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualLabelCount.size(), facets.size());
        });

        for (Map.Entry<String, Integer> facet : actualLabelCount.entrySet()) {
            assertEventually(() -> {
                String facetLabel = facet.getKey();
                int facetCount = getFacets().get(facetLabel);
                float ratio = ((float) facetCount) / facet.getValue();
                assertTrue("Facet count for label: " + facetLabel + " is outside of 5% margin of error. " +
                                "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                        Math.abs(ratio - 1) < 0.05);
            });
        }
    }

    private Map<String, Integer> getFacets() {
        return getFacets(null);
    }

    private Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    private Map<String, Integer> getFacets(String path) {
        Map<String, Integer> map = Maps.newHashMap();

        String pathCons = "";
        if (path != null) {
            pathCons = " AND ISDESCENDANTNODE('" + path + "')";
        }
        String query = "SELECT [rep:facet(foo)], [rep:facet(bar)] FROM [nt:base] WHERE [cons] = 'val'" + pathCons;
        Query q = null;
        QueryResult queryResult;
        try {
            q = qe.createQuery(query, Query.JCR_SQL2);
            queryResult = q.execute();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        FacetResult facetResult = new FacetResult(queryResult);

        Set<String> dims = facetResult.getDimensions();
        for (String dim : dims) {
            List<FacetResult.Facet> facets = facetResult.getFacets(dim);
            for (FacetResult.Facet facet : facets) {
                map.put(facet.getLabel(), facet.getCount());
            }
        }

        return map;
    }

    private static void assertEventually(Runnable r) {
        ElasticsearchTestUtils.assertEventually(r, BULK_FLUSH_INTERVAL_MS_DEFAULT * 3);
    }

}
