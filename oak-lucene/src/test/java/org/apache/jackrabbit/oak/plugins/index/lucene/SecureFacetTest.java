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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.query.facet.FacetResult.Facet;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.*;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.security.Privilege;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.*;
import static org.junit.Assert.*;

public class SecureFacetTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private Closer closer;

    private Session session;
    private Node indexNode;
    private QueryManager qe;

    private static final int NUM_LEAF_NODES = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
    private static final int NUM_LABELS = 4;
    private Map<String, Integer> actualLabelCount = Maps.newHashMap();
    private Map<String, Integer> actualAclLabelCount = Maps.newHashMap();
    private Map<String, Integer> actualAclPar1LabelCount = Maps.newHashMap();

    @Before
    public void setup() throws Exception {
        closer = Closer.create();
        createRepository();
        createIndex();
    }

    private void createRepository() throws RepositoryException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        closer.register(new ExecutorCloser(executorService));
        IndexCopier copier = new IndexCopier(executorService, temporaryFolder.getRoot());
        LuceneIndexEditorProvider editorProvider = new LuceneIndexEditorProvider(copier);
        LuceneIndexProvider queryIndexProvider = new LuceneIndexProvider(copier);
        NodeStore nodeStore = new MemoryNodeStore(INITIAL_CONTENT);
        Oak oak = new Oak(nodeStore)
                .with((QueryIndexProvider) queryIndexProvider)
                .with((Observer) queryIndexProvider)
                .with(editorProvider);

        Jcr jcr = new Jcr(oak);
        @NotNull Repository repository = jcr.createRepository();

        session = repository.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        closer.register(session::logout);

        // we'd always query anonymously
        Session anonSession = repository.login(new GuestCredentials());
        closer.register(anonSession::logout);
        qe = anonSession.getWorkspace().getQueryManager();
    }

    @After
    public void after() throws IOException {
        closer.close();
        IndexDefinition.setDisableStoredIndexDefinition(false);
    }

    private void createIndex() throws RepositoryException {
        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
        idxBuilder.noAsync().evaluatePathRestrictions()
                .indexRule("nt:base")
                .property("cons")
                .propertyIndex()
                .property("foo")
                .getBuilderTree().setProperty(PROP_FACETS, true);

        indexNode = getOrCreateByPath("/oak:index", "nt:unstructured", session)
                .addNode("index", INDEX_DEFINITIONS_NODE_TYPE);
        idxBuilder.build(indexNode);
        session.save();
    }

    private void createLargeDataset() throws RepositoryException {
        Random rGen = new Random(42);
        int[] labelCount = new int[NUM_LABELS];
        int[] aclLabelCount = new int[NUM_LABELS];
        int[] aclPar1LabelCount = new int[NUM_LABELS];

        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));

        for (int i = 0; i < NUM_LABELS; i++) {
            Node subPar = par.addNode("par" + i);
            for (int j = 0; j < NUM_LEAF_NODES; j++) {
                Node child = subPar.addNode("c" + j);
                child.setProperty("cons", "val");

                // Add a random label out of "l0", "l1", "l2", "l3"
                int labelNum = rGen.nextInt(NUM_LABELS);
                child.setProperty("foo", "l" + labelNum);

                labelCount[labelNum]++;
                if (i != 0) {
                    aclLabelCount[labelNum]++;
                }
                if (i == 1) {
                    aclPar1LabelCount[labelNum]++;
                }
            }

            // deny access for one sub-parent
            if (i == 0) {
                deny(subPar);
            }
        }

        session.save();

        for (int i = 0; i < labelCount.length; i++) {
            actualLabelCount.put("l" + i, labelCount[i]);
            actualAclLabelCount.put("l" + i, aclLabelCount[i]);
            actualAclPar1LabelCount.put("l" + i, aclPar1LabelCount[i]);
        }

        assertNotEquals("Acl-ed and actual counts mustn't be same", actualLabelCount, actualAclLabelCount);
    }

    private void createSmallDataset() throws RepositoryException{
        Random rGen = new Random(42);
        int[] labelCount = new int[NUM_LABELS];
        int[] aclLabelCount = new int[NUM_LABELS];
        int[] aclPar1LabelCount = new int[NUM_LABELS];

        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", session));

        for (int i = 0; i < NUM_LABELS; i++) {
            Node subPar = par.addNode("par" + i);
            for (int j = 0; j < NUM_LEAF_NODES/(2 * NUM_LABELS); j++) {
                Node child = subPar.addNode("c" + j);
                child.setProperty("cons", "val");

                // Add a random label out of "l0", "l1", "l2", "l3"
                int labelNum = rGen.nextInt(NUM_LABELS);
                child.setProperty("foo", "l" + labelNum);

                labelCount[labelNum]++;
                if (i != 0) {
                    aclLabelCount[labelNum]++;
                }
                if (i == 1) {
                    aclPar1LabelCount[labelNum]++;
                }
            }

            // deny access for one sub-parent
            if (i == 0) {
                deny(subPar);
            }
        }

        session.save();

        for (int i = 0; i < labelCount.length; i++) {
            actualLabelCount.put("l" + i, labelCount[i]);
            actualAclLabelCount.put("l" + i, aclLabelCount[i]);
            actualAclPar1LabelCount.put("l" + i, aclPar1LabelCount[i]);
        }

        assertNotEquals("Acl-ed and actual counts mustn't be same", actualLabelCount, actualAclLabelCount);

    }

    @Test
    public void secureFacets() throws Exception {
        createLargeDataset();

        assertEquals(actualAclLabelCount, getFacets());
    }

    @Test
    public void secureFacets_withOneLabelInaccessible() throws Exception {
        createLargeDataset();
        Node inaccessibleChild = deny(session.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        session.save();

        assertEquals(actualAclLabelCount, getFacets());
    }

    @Test
    public void insecureFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createLargeDataset();

        assertEquals(actualLabelCount, getFacets());
    }

    @Test
    public void statisticalFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createLargeDataset();

        Map<String, Integer> facets = getFacets();
        assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

        for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            int facetCount = facets.get(facetLabel);
            float ratio = ((float)facetCount) / facet.getValue();
            assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                            "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                    Math.abs(ratio - 1) < 0.1);
        }
    }

    @Test
    public void statisticalFacetsWithHitCountLessThanSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createSmallDataset();

        Map<String, Integer> facets = getFacets();
        assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

        // Since the hit count is less than sample size -> flow should have switched to secure facet count instead of statistical
        // and thus the count should be exactly equal
        assertEquals(actualAclLabelCount, facets);
    }

    @Test
    public void statisticalFacets_withHitCountSameAsSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createLargeDataset();

        Map<String, Integer> facets = getFacets("/parent/par1");
        assertEquals("Unexpected number of facets", actualAclPar1LabelCount.size(), facets.size());

        for (Map.Entry<String, Integer> facet : actualAclPar1LabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            int facetCount = facets.get(facetLabel);
            float ratio = ((float)facetCount) / facet.getValue();
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

        createLargeDataset();
        Node inaccessibleChild = deny(session.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        session.save();

        Map<String, Integer> facets = getFacets();
        assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

        for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            int facetCount = facets.get(facetLabel);
            float ratio = ((float)facetCount) / facet.getValue();
            assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                            "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                    Math.abs(ratio - 1) < 0.1);
        }
    }

    @Test
    public void secureFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createLargeDataset();

        qe = session.getWorkspace().getQueryManager();

        assertEquals(actualLabelCount, getFacets());
    }

    @Test
    public void statisticalFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", session);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        session.save();

        createLargeDataset();

        qe = session.getWorkspace().getQueryManager();

        Map<String, Integer> facets = getFacets();
        assertEquals("Unexpected number of facets", actualLabelCount.size(), facets.size());

        for (Map.Entry<String, Integer> facet : actualLabelCount.entrySet()) {
            String facetLabel = facet.getKey();
            int facetCount = facets.get(facetLabel);
            float ratio = ((float)facetCount) / facet.getValue();
            assertTrue("Facet count for label: " + facetLabel + " is outside of 5% margin of error. " +
                            "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                    Math.abs(ratio - 1) < 0.05);
        }
    }

    private Map<String, Integer> getFacets() throws Exception {
        return getFacets(null);
    }

    private Map<String, Integer> getFacets(String path) throws Exception {
        Map<String, Integer> map = Maps.newHashMap();

        String pathCons = "";
        if (path != null) {
            pathCons = " AND ISDESCENDANTNODE('" + path + "')";
        }
        String query = "SELECT [rep:facet(foo)] FROM [nt:base] WHERE [cons] = 'val'" + pathCons;

        Query q = qe.createQuery(query, Query.JCR_SQL2);
        QueryResult queryResult = q.execute();

        FacetResult facetResult = new FacetResult(queryResult);

        Set<String> dims = facetResult.getDimensions();
        for (String dim : dims) {
            List<Facet> facets = facetResult.getFacets(dim);
            for (Facet facet : facets) {
                map.put(facet.getLabel(), facet.getCount());
            }
        }

        return map;
    }

    @SuppressWarnings("UnusedReturnValue")
    private Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }
}
