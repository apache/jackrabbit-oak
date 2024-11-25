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
package org.apache.jackrabbit.oak.benchmark;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.tree.factories.TreeFactory;
import org.apache.jackrabbit.oak.query.facet.FacetResult;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.security.Privilege;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.api.Type.BOOLEAN;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.COMPAT_MODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_RULES;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_PROPERTY_INDEX;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_INSECURE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT;

public class FacetSearchTest extends AbstractTest<FacetSearchTest.TestContext> {

    private static final Logger LOG = LoggerFactory.getLogger(FacetSearchTest.class);
    protected TestContext defaultContext;

    // Number of sub nodes to be created
    // Total number of nodes created will be NUM_LEAF_NODES*NUM_LABELS
    private static final int NUM_LEAF_NODES = Integer.getInteger("numFacetLeafNodes", STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT);
    private static final int NUM_LABELS = Integer.getInteger("numLabelsForFacets", 4);
    protected static final String SECURE_FACET = "SECURE";
    protected static final String INSECURE_FACET = "INSECURE";
    protected static final String STATISTICAL_FACET = "STATISTICAL";

    protected static final String SEARCH_PROP = "cons";
    protected static final String FACET_PROP_1 = "foo";
    protected static final String FACET_PROP_2 = "bar";
    private final Map<String, Integer> actualLabelCount = new HashMap<>();
    private final Map<String, Integer> actualAclLabelCount = new HashMap<>();
    private final Map<String, Integer> actualAclPar1LabelCount = new HashMap<>();
    protected Boolean storageEnabled;
    protected Set<String> propVals = new HashSet<>();
    protected Random rgen = new Random(42);

    public FacetSearchTest(Boolean storageEnabled) {
        this.storageEnabled = storageEnabled;
    }


    @Override
    protected void beforeSuite() throws Exception {
        final Session session = loginWriter();
        createTestData(session, NUM_LEAF_NODES);
        // Allow indexing to catch up (This test makes use of sync updates)
        Thread.sleep(10000);
        defaultContext = new TestContext();
    }

    @Override
    protected void afterSuite() throws Exception {

    }

    @Override
    protected void runTest() throws Exception {
        runTest(defaultContext);
    }

    @Override
    protected void runTest(TestContext ec) throws Exception {
        LOG.trace("Starting test execution");
        Map<String, Integer> map = new HashMap<>();
        QueryManager qm = ec.session.getWorkspace().getQueryManager();
        String query = getQuery();
        LOG.trace(query);
        Query q;
        QueryResult queryResult;
        try {
            q = qm.createQuery(query, Query.JCR_SQL2);
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
        LOG.trace("Facet Results - " + map);
    }

    protected String getQuery() {
        List<String> samples = new ArrayList<>(propVals);
        return "SELECT [rep:facet(foo)], [rep:facet(bar)] FROM [nt:base] WHERE [cons] = '" + samples.get(rgen.nextInt(samples.size())) + "'";
    }


    protected void createTestData(Session session, int numberOfLeafNodes) throws Exception {

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
                // Add a random label
                int foolabelNum = rGen.nextInt(NUM_LABELS);
                int barlabelNum = rGen1.nextInt(NUM_LABELS);

                String val = "val_" + j/100;
                propVals.add(val);
                Node child = subPar.addNode("c" + j);
                child.setProperty("cons", val);

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

    }


    private Node deny(Node node) throws RepositoryException {
        AccessControlUtils.deny(node, "anonymous", Privilege.JCR_ALL);
        return node;
    }

    private Node allow(Node node) throws RepositoryException {
        AccessControlUtils.allow(node, "anonymous", Privilege.JCR_READ);
        return node;
    }

    class TestContext {
        final Session session = loginWriter();
    }

    protected String getFacetMode() {
        return SECURE_FACET;
    }

    static class FacetIndexInitializer implements RepositoryInitializer {

        private String name;
        private Map<String, Boolean> props;
        private String type;
        private String facetMode;

        public FacetIndexInitializer(@NotNull final String name, @NotNull final Map<String, Boolean> props, @NotNull String type, @NotNull String facetMode) {
            this.name = name;
            this.props = props;
            this.type = type;
            this.facetMode = facetMode;
        }

        @Override
        public void initialize(@NotNull NodeBuilder builder) {
            if (!isAlreadyThere(builder)) {
                Tree t = TreeFactory.createTree(builder.child(INDEX_DEFINITIONS_NAME));
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                NodeBuilder uuid = IndexUtils.createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "uuid", true, true,
                        ImmutableList.<String>of("jcr:uuid"), null);
                uuid.setProperty("info",
                        "Oak index for UUID lookup (direct lookup of nodes with the mixin 'mix:referenceable').");

                t = t.addChild(name);
                t.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, NAME);
                t.setProperty(COMPAT_MODE, 2L, LONG);
                t.setProperty(TYPE_PROPERTY_NAME, type, STRING);
                t.setProperty(REINDEX_PROPERTY_NAME, true);

                if (!SECURE_FACET.equals(facetMode)) {
                    Tree facetConfig = t.addChild(FACETS);
                    facetConfig.setProperty("jcr:primaryType","nt:unstructured", NAME);
                    switch(facetMode) {
                        case INSECURE_FACET :

                            facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_INSECURE);
                            break;
                        case STATISTICAL_FACET:
                            facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
                            facetConfig.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 3000);
                            break;
                        default:
                            break;
                    }

                }
                t = t.addChild(INDEX_RULES);
                t.setOrderableChildren(true);
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                t = t.addChild("nt:base");
                t.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                Tree propnode = t.addChild(PROP_NODE);
                propnode.setOrderableChildren(true);
                propnode.setProperty("jcr:primaryType", "nt:unstructured", NAME);

                for (String p : props.keySet()) {
                    Tree t1 = propnode.addChild(PathUtils.getName(p));
                    t1.setProperty(PROP_PROPERTY_INDEX, true, BOOLEAN);
                    t1.setProperty(PROP_NAME, p);
                    t1.setProperty("jcr:primaryType", "nt:unstructured", NAME);
                    if (props.get(p)) {
                        t1.setProperty("facets", true, Type.BOOLEAN);
                    }
                }
            }
        }

        private boolean isAlreadyThere(final @NotNull NodeBuilder root) {
            return root.hasChildNode(INDEX_DEFINITIONS_NAME) &&
                    root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(name);
        }
    }
}
