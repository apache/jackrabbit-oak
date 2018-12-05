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

package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;

import java.util.Random;

import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.*;
import static org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition.SecureFacetConfiguration.MODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class IndexDefinitionFacetConfigTest {

    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    private NodeState root = EMPTY_NODE;

    private final NodeBuilder builder = root.builder();

    private static final long RANDOM_SEED = 1L;

    @Test
    public void defaultConfig() {
        SecureFacetConfiguration config = SecureFacetConfiguration.getInstance(RANDOM_SEED, root);

        assertEquals(config.getMode(), MODE.SECURE);

        int sampleSize = config.getStatisticalFacetSampleSize();
        assertEquals(-1, sampleSize);
    }

    @Test
    public void nonSecureConfigMode() {
        SecureFacetConfiguration config;

        builder.setProperty(PROP_SECURE_FACETS, "insecure");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, builder.getNodeState());
        assertEquals(-1, config.getStatisticalFacetSampleSize());

        builder.setProperty(PROP_SECURE_FACETS, "statistical");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, builder.getNodeState());
        assertEquals(STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT, config.getStatisticalFacetSampleSize());
    }

    @Test
    public void systemPropSecureFacet() {
        SecureFacetConfiguration config;

        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "random");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, root);
        assertEquals(MODE.SECURE, config.getMode());

        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "secure");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, root);
        assertEquals(MODE.SECURE, config.getMode());

        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "insecure");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, root);
        assertEquals(MODE.INSECURE, config.getMode());

        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "insecure");
        builder.setProperty(PROP_SECURE_FACETS, "secure");
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, builder.getNodeState());
        assertEquals(MODE.SECURE, config.getMode());
    }

    @Test
    public void systemPropSecureFacetStatisticalSampleSize() {
        int sampleSize;

        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, PROP_SECURE_FACETS_VALUE_STATISTICAL);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "10");
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, root).getStatisticalFacetSampleSize();
        assertEquals(10, sampleSize);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "-10");
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, root).getStatisticalFacetSampleSize();
        assertEquals(STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT, sampleSize);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "100000000000");
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, root).getStatisticalFacetSampleSize();
        assertEquals(STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT, sampleSize);
    }

    @Test
    public void invalidSecureFacetSampleSize() {
        int sampleSize;
        NodeBuilder configBuilder = builder.child("config").setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL);
        NodeState nodeState = configBuilder.getNodeState();

        configBuilder.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, -10);
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState).getStatisticalFacetSampleSize();
        assertEquals(STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT, sampleSize);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "10");
        configBuilder.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, -20);
        nodeState = configBuilder.getNodeState();
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState).getStatisticalFacetSampleSize();
        assertEquals(10, sampleSize);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "-10");
        configBuilder.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, -20);
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState).getStatisticalFacetSampleSize();
        assertEquals(STATISTICAL_FACET_SAMPLE_SIZE_DEFAULT, sampleSize);

        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "-10");
        configBuilder.setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 10);
        nodeState = configBuilder.getNodeState();
        sampleSize = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState).getStatisticalFacetSampleSize();
        assertEquals(10, sampleSize);
    }

    @Test
    public void orderingOfOverrides() {
        System.setProperty(PROP_SECURE_FACETS_VALUE_JVM_PARAM, "insecure");
        System.setProperty(STATISTICAL_FACET_SAMPLE_SIZE_JVM_PARAM, "10");

        NodeState nodeState;
        SecureFacetConfiguration config;
        int sampleSize;

        nodeState = builder.child("config1").setProperty(PROP_SECURE_FACETS, "secure").getNodeState();
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState);
        assertEquals(MODE.SECURE, config.getMode());
        assertEquals(-1, config.getStatisticalFacetSampleSize());

        nodeState = builder.child("config2")
                .setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL)
                .setProperty(PROP_STATISTICAL_FACET_SAMPLE_SIZE, 20)
                .getNodeState();
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState);
        sampleSize = config.getStatisticalFacetSampleSize();
        assertEquals(20, sampleSize);

        nodeState = builder.child("config3")
                .setProperty(PROP_SECURE_FACETS, PROP_SECURE_FACETS_VALUE_STATISTICAL)
                .getNodeState();
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, nodeState);
        sampleSize = config.getStatisticalFacetSampleSize();
        assertEquals(10, sampleSize);
    }

    @Test
    public void legacyConfig() {
        NodeState ns = builder.setProperty(PROP_SECURE_FACETS, true).getNodeState();
        SecureFacetConfiguration config = SecureFacetConfiguration.getInstance(RANDOM_SEED, ns);
        assertEquals(MODE.SECURE, config.getMode());
        assertEquals(-1, config.getStatisticalFacetSampleSize());

        ns = builder.setProperty(PROP_SECURE_FACETS, false).getNodeState();
        config = SecureFacetConfiguration.getInstance(RANDOM_SEED, ns);
        assertEquals(MODE.INSECURE, config.getMode());
        assertEquals(-1, config.getStatisticalFacetSampleSize());
    }

    @Test
    public void absentFacetConfigNode() {
        IndexDefinition idxDefn = new IndexDefinition(root, root, "/foo");

        SecureFacetConfiguration config = idxDefn.getSecureFacetConfiguration();

        assertEquals(MODE.SECURE, config.getMode());
    }

    @Test
    public void randomSeed() {
        long seed = new Random().nextLong();
        builder.setProperty(PROP_RANDOM_SEED, seed);
        root = builder.getNodeState();
        IndexDefinition idxDefn = new IndexDefinition(root, root, "/foo");

        SecureFacetConfiguration config = idxDefn.getSecureFacetConfiguration();

        assertEquals(seed, config.getRandomSeed());
    }

    @Test
    public void randomSeedWithoutOneInDef() {
        long seed1 = new IndexDefinition(root, root, "/foo").getSecureFacetConfiguration().getRandomSeed();
        long seed2 = new IndexDefinition(root, root, "/foo").getSecureFacetConfiguration().getRandomSeed();

        assertNotEquals(seed1, seed2);
    }
}