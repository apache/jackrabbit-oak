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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.UpToDateNodeStateConfiguration}
 */
public class UpToDateNodeStateConfigurationTest {

    private NodeStore store;

    @Before
    public void setUp() throws Exception {
        MicroKernel microKernel = new MicroKernelImpl();
        String jsop = "^\"a\":1 ^\"b\":2 ^\"c\":3 +\"x\":{} +\"y\":{} +\"z\":{} " +
                "+\"oak:index\":{\"solrIdx\":{\"coreName\":\"cn\", \"solrHomePath\":\"sh\", \"solrConfigPath\":\"sc\"}} ";
        microKernel.commit("/", jsop, microKernel.getHeadRevision(), "test data");
        store = new KernelNodeStore(microKernel);
    }

    @Test
    public void testExistingPath() throws Exception {
        String path = "oak:index/solrIdx";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        EmbeddedSolrServerConfiguration solrServerConfiguration = (EmbeddedSolrServerConfiguration) upToDateNodeStateConfiguration.getSolrServerConfiguration();
        assertNotNull(solrServerConfiguration);
        assertEquals("sh", solrServerConfiguration.getSolrHomePath()); // property defined in the node state
        assertEquals("cn", solrServerConfiguration.getCoreName()); // property defined in the node state
        assertEquals("sc", solrServerConfiguration.getSolrConfigPath()); // property defined in the node state
        assertEquals("path_exact", upToDateNodeStateConfiguration.getPathField()); // using default as this property not defined in the node state
    }

    @Test
    public void testNonExistingPath() throws Exception {
        String path = "some/path/to/oak:index/solrIdx";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        assertNotNull(upToDateNodeStateConfiguration.getSolrServerConfiguration());
    }

    @Test
    public void testWrongNodeState() throws Exception {
        String path = "a";
        UpToDateNodeStateConfiguration upToDateNodeStateConfiguration = new UpToDateNodeStateConfiguration(store, path);
        assertFalse(upToDateNodeStateConfiguration.getConfigurationNodeState().exists());
        assertNotNull(upToDateNodeStateConfiguration.getSolrServerConfiguration()); // defaults are used
    }
}
