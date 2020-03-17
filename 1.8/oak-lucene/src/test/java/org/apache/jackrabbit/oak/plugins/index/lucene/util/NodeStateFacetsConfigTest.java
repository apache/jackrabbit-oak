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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.facet.FacetsConfig;
import org.junit.Test;

import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link NodeStateFacetsConfig}
 */
public class NodeStateFacetsConfigTest {

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void testMultivaluedDimConfig() throws Exception {
        NodeStateFacetsConfig nodeStateFacetsConfig = new NodeStateFacetsConfig(builder);
        String dimension = "foo";
        nodeStateFacetsConfig.setMultiValued(dimension, true);
        FacetsConfig.DimConfig dimConfig = nodeStateFacetsConfig.getDimConfig(dimension);
        assertNotNull(dimConfig);
        assertTrue(dimConfig.multiValued);

        NodeStateFacetsConfig nodeStateFacetsConfig2 = new NodeStateFacetsConfig(builder);
        FacetsConfig.DimConfig dimConfig2 = nodeStateFacetsConfig2.getDimConfig(dimension);
        assertNotNull(dimConfig2);
        assertTrue(dimConfig2.multiValued);
    }

    @Test
    public void testMultivaluedRelativeDimConfig() throws Exception {
        NodeStateFacetsConfig nodeStateFacetsConfig = new NodeStateFacetsConfig(builder);
        String dimension = "jcr:content/text";
        nodeStateFacetsConfig.setMultiValued(dimension, true);
        FacetsConfig.DimConfig dimConfig = nodeStateFacetsConfig.getDimConfig(dimension);
        assertNotNull(dimConfig);
        assertTrue(dimConfig.multiValued);

        NodeStateFacetsConfig nodeStateFacetsConfig2 = new NodeStateFacetsConfig(builder);
        assertTrue(nodeStateFacetsConfig.getDimConfig(dimension).multiValued);
    }

}