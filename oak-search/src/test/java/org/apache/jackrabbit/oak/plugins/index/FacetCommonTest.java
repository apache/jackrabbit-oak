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

import org.junit.Test;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.RowIterator;
import java.util.Map;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_REFRESH_DEFN;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class FacetCommonTest extends FacetBaseTest {

    @Test
    public void secureFacets() throws Exception {
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        assertEventually(() -> assertEquals(actualAclLabelCount, getFacets()));
    }

    @Test
    public void secureFacets_withOneLabelInaccessible() throws Exception {
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        Node inaccessibleChild = deny(adminSession.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        adminSession.save();
        assertEventually(() -> assertEquals(actualAclLabelCount, getFacets()));
    }

    @Test
    public void insecureFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        assertEventually(() -> assertEquals(actualLabelCount, getFacets()));
    }

    @Test
    public void statisticalFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        facetConfig.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 3000);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);

        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

            for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {
                String facetLabel = facet.getKey();
                assertEventually(() -> {
                    int facetCount = facets.get(facetLabel);
                    float ratio = ((float) facetCount) / facet.getValue();
                    assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                                    "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                            Math.abs(ratio - 1) < 0.1);
                });
            }

            try {
                // Verify that the query result is not affected by the facet sampling
                int rowCounter = 0;
                RowIterator rows = getQueryResult(null).getRows();
                while (rows.hasNext()) {
                    rows.nextRow();
                    rowCounter++;
                }
                assertEquals("Unexpected number of rows", 3000, rowCounter);
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void statisticalFacetsWithHitCountLessThanSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_SMALL_DATASET);

        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

            // Since the hit count is less than sample size -> flow should have switched to secure facet count instead of statistical
            // and thus the count should be exactly equal
            assertEquals(actualAclLabelCount, facets);
        });
    }

    @Test
    public void statisticalFacets_withHitCountSameAsSampleSize() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);

        assertEventually(() -> {
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
        });
    }

    @Test
    public void statisticalFacets_withOneLabelInaccessible() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        Node inaccessibleChild = deny(adminSession.getNode("/parent").addNode("par4")).addNode("c0");
        inaccessibleChild.setProperty("cons", "val");
        inaccessibleChild.setProperty("foo", "l4");
        adminSession.save();
        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualAclLabelCount.size(), facets.size());

            for (Map.Entry<String, Integer> facet : actualAclLabelCount.entrySet()) {
                String facetLabel = facet.getKey();
                int facetCount = facets.get(facetLabel);
                float ratio = ((float) facetCount) / facet.getValue();
                assertTrue("Facet count for label: " + facetLabel + " is outside of 10% margin of error. " +
                                "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                        Math.abs(ratio - 1) < 0.1);
            }
        });
    }

    @Test
    public void secureFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        adminSession.save();
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        qm = adminSession.getWorkspace().getQueryManager();
        assertEventually(() -> assertEquals(actualLabelCount, getFacets()));
    }

    @Test
    public void statisticalFacets_withAdminSession() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        indexNode.setProperty(PROP_REFRESH_DEFN, true);
        adminSession.save();
        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        qm = adminSession.getWorkspace().getQueryManager();
        assertEventually(() -> {
            Map<String, Integer> facets = getFacets();
            assertEquals("Unexpected number of facets", actualLabelCount.size(), facets.size());

            for (Map.Entry<String, Integer> facet : actualLabelCount.entrySet()) {
                String facetLabel = facet.getKey();
                int facetCount = facets.get(facetLabel);
                float ratio = ((float) facetCount) / facet.getValue();
                assertTrue("Facet count for label: " + facetLabel + " is outside of 5% margin of error. " +
                                "Expected: " + facet.getValue() + "; Got: " + facetCount + "; Ratio: " + ratio,
                        Math.abs(ratio - 1) < 0.05);
            }
        });
    }
}
