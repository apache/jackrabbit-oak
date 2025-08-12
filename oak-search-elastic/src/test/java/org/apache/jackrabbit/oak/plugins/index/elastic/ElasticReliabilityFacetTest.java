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

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Latency;
import eu.rekawek.toxiproxy.model.toxic.LimitData;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.index.FacetBaseTest;
import org.apache.jackrabbit.oak.plugins.index.TestUtil;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticBulkProcessorHandler;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import javax.jcr.Node;
import javax.jcr.Repository;
import java.io.IOException;
import java.util.Map;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticReliabilityTest.TOXIPROXY_IMAGE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider.FACETS_EVALUATION_TIMEOUT_MS_PROPERTY;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_SECURE_FACETS_VALUE_STATISTICAL;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROP_STATISTICAL_FACET_SAMPLE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticReliabilityFacetTest extends FacetBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticReliabilityFacetTest.class);
    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse(TOXIPROXY_IMAGE_NAME);
    protected ToxiproxyContainer toxiproxy;
    protected Proxy proxy;

    // Use a very low timeout for callers requesting facets
//    @Rule
//    public final ProvideSystemProperty facetsSystemProperties
//            = new ProvideSystemProperty(FACETS_EVALUATION_TIMEOUT_MS_PROPERTY, "10");

    @Override
    public void before() throws Exception {
        toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withStartupAttempts(3)
                .withNetwork(elasticRule.elastic.getNetwork());
        toxiproxy.start();
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxy = toxiproxyClient.createProxy("elastic", "0.0.0.0:8666", "elasticsearch:9200");
        super.before();

    }

    @After
    @Override
    public void after() throws IOException {
        super.after();
        if (toxiproxy.isRunning()) {
            toxiproxy.stop();
        }
    }

    protected Repository createJcrRepository() {
        indexOptions = new ElasticIndexOptions();
        repositoryOptionsUtil =
                new ElasticTestRepositoryBuilder(elasticRule, toxiproxy.getHost(), toxiproxy.getMappedPort(8666))
                        .build();
        Oak oak = repositoryOptionsUtil.getOak();
        Jcr jcr = new Jcr(oak);
        return jcr.createRepository();
    }

    protected void assertEventually(Runnable r) {
        TestUtil.assertEventually(r,
                ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + ElasticBulkProcessorHandler.BULK_FLUSH_INTERVAL_MS_DEFAULT) * 5);
    }

    @Test
    public void callerTimeoutsWaitingForFacets() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        facetConfig.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 3000);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);
        LimitData latency = proxy.toxics().limitData("latency", ToxicDirection.DOWNSTREAM, 1000);
        try {
            Map<String, Integer> facets = getFacets();
            fail("Should have failed. Instead got: " + facets);
        } catch (RuntimeException e) {
            assertTrue("Exception message should contain 'Timeout waiting for next result from'",
                    e.getMessage().contains("Timeout waiting for next result from"));
        } finally {
            latency.remove();
        }
    }


    @Test
    public void noDelays() throws Exception {
        Node facetConfig = getOrCreateByPath(indexNode.getPath() + "/" + FACETS, "nt:unstructured", adminSession);
        facetConfig.setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        facetConfig.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 3000);
        adminSession.save();

        createDataset(NUM_LEAF_NODES_FOR_LARGE_DATASET);

        assertEventually(() -> {
            LOG.info("Requesting facets");
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

//        LimitData latency = proxy.toxics().limitData("latency", ToxicDirection.DOWNSTREAM, 1000);
        Latency latency = proxy.toxics().latency("latency", ToxicDirection.DOWNSTREAM, 1000);
        try {
            Map<String, Integer> facets = getFacets();
            fail("Should have failed. Instead got: " + facets);
        } catch (RuntimeException e) {
            assertTrue("Exception message should contain 'Timeout waiting for next result from'",
                    e.getMessage().contains("Timeout waiting for next result from"));
        } finally {
            latency.remove();
        }

    }
}
