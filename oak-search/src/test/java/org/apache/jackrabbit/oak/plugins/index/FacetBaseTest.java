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
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractJcrTest;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.query.Query;
import javax.jcr.query.QueryResult;
import javax.jcr.security.Privilege;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_RANDOM_SEED;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
import static org.junit.Assert.assertNotEquals;

public abstract class FacetBaseTest extends AbstractJcrTest {
    private static final Logger LOG = LoggerFactory.getLogger(FacetBaseTest.class);
    private static final PerfLogger LOG_PERF = new PerfLogger(LOG);
    protected TestRepository repositoryOptionsUtil;
    protected Node indexNode;
    protected IndexOptions indexOptions;
    private static final String FACET_PROP = "facets";

    protected static final int NUM_LEAF_NODES = STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;
    protected static final int NUM_LABELS = 4;
    protected static final int NUM_LEAF_NODES_FOR_LARGE_DATASET = NUM_LEAF_NODES;
    protected static final int NUM_LEAF_NODES_FOR_SMALL_DATASET = NUM_LEAF_NODES / (2 * NUM_LABELS);
    protected final Map<String, Integer> actualLabelCount = new HashMap<>();
    protected final Map<String, Integer> actualAclLabelCount = new HashMap<>();
    protected  final Map<String, Integer> actualAclPar1LabelCount = new HashMap<>();
    private static final Random INDEX_SUFFIX_RANDOMIZER = new Random(7);


    @Before
    public void createIndex() throws RepositoryException {
        IndexDefinitionBuilder builder = indexOptions.createIndex(indexOptions.createIndexDefinitionBuilder(), false);
        builder.noAsync().evaluatePathRestrictions();
        builder.getBuilderTree().setProperty("jcr:primaryType", "oak:QueryIndexDefinition", Type.NAME);
        // Statistical facets in Elasticsearch use a random function with a fixed seed but the results are not
        // consistent when the index name changes. So we set the index name to a fixed values.
        String indexName = "FacetCommonTestIndex" + INDEX_SUFFIX_RANDOMIZER.nextInt(1000);
        builder.getBuilderTree().setProperty(PROP_RANDOM_SEED, 3000L, Type.LONG);
        builder.getBuilderTree().setProperty("indexNameSeed", 300L, Type.LONG);
        IndexDefinitionBuilder.IndexRule indexRule = builder.indexRule(JcrConstants.NT_BASE);
        indexRule.property("cons").propertyIndex();
        indexRule.property("foo").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexRule.property("bar").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);
        indexRule.property("baz").propertyIndex().getBuilderTree().setProperty(FACET_PROP, true, Type.BOOLEAN);

        indexOptions.setIndex(adminSession, indexName, builder);
        indexNode = indexOptions.getIndexNode(adminSession, indexName);
    }

    protected void createDataset(int numberOfLeafNodes) throws RepositoryException {
        Random rGen = new Random(42);
        Random rGen1 = new Random(42);
        int[] foolabelCount = new int[NUM_LABELS];
        int[] fooaclLabelCount = new int[NUM_LABELS];
        int[] fooaclPar1LabelCount = new int[NUM_LABELS];

        int[] barlabelCount = new int[NUM_LABELS];
        int[] baraclLabelCount = new int[NUM_LABELS];
        int[] baraclPar1LabelCount = new int[NUM_LABELS];

        Node par = allow(getOrCreateByPath("/parent", "oak:Unstructured", adminSession));

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
        adminSession.save();
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

    protected Map<String, Integer> getFacets() {
        return getFacets(null);
    }

    protected Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    protected Map<String, Integer> getFacets(String path) {
        QueryResult queryResult = getQueryResult(path);
        long start = LOG_PERF.start("Getting the Facet Results...");
        FacetResult facetResult = new FacetResult(queryResult);
        LOG_PERF.end(start, -1, "Facet Results fetched");

        return facetResult.getDimensions()
                .stream()
                .flatMap(dim -> Objects.requireNonNull(facetResult.getFacets(dim)).stream())
                .collect(Collectors.toMap(FacetResult.Facet::getLabel, FacetResult.Facet::getCount));
    }

    protected QueryResult getQueryResult(String path) {
        String pathCons = "";
        if (path != null) {
            pathCons = " AND ISDESCENDANTNODE('" + path + "')";
        }
        String query = "SELECT [jcr:path], [rep:facet(foo)], [rep:facet(bar)], [rep:facet(baz)] FROM [nt:base] WHERE [cons] = 'val'" + pathCons;
        Query q;
        QueryResult queryResult;
        try {
            q = qm.createQuery(query, Query.JCR_SQL2);
            queryResult = q.execute();
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
        return queryResult;
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }
}
