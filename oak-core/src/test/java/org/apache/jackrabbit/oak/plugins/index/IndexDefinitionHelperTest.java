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

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.junit.Assert.*;

public class IndexDefinitionHelperTest {

    @Test
    public void testNormalize_TypeOnly() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("type", "lucene", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        assertEquals("lucene", props.getActiveTarget());
        assertEquals(Arrays.asList("lucene"), props.getStoreTargets());
        assertFalse(props.isMultiTarget());
    }

    @Test
    public void testNormalize_BothStoreTargetsAndActiveTarget() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);
        builder.setProperty("activeTarget", "lucene47", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        assertEquals("lucene47", props.getActiveTarget());
        assertEquals(Arrays.asList("lucene47", "lucene9"), props.getStoreTargets());
        assertTrue(props.isMultiTarget());
    }

    @Test
    public void testNormalize_ActiveTargetOnly() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("activeTarget", "lucene9", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        assertEquals("lucene9", props.getActiveTarget());
        assertEquals(Arrays.asList("lucene9"), props.getStoreTargets());
        assertFalse(props.isMultiTarget());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalize_StoreTargetsWithoutActiveTarget() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);

        // Should throw: storeTargets requires activeTarget
        IndexDefinitionHelper.normalize(builder.getNodeState());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalize_NoProperties() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();

        // Should throw: Either type or activeTarget must be defined
        IndexDefinitionHelper.normalize(builder.getNodeState());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNormalize_ActiveTargetNotInStoreTargets() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);
        builder.setProperty("activeTarget", "elasticsearch", STRING);

        // Should throw: activeTarget must be in storeTargets
        IndexDefinitionHelper.normalize(builder.getNodeState());
    }

    @Test
    public void testNormalize_TypeIgnoredWhenStoreTargetsDefined() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("type", "lucene", STRING);
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);
        builder.setProperty("activeTarget", "lucene47", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        // type should be ignored, storeTargets/activeTarget used
        assertEquals("lucene47", props.getActiveTarget());
        assertEquals(Arrays.asList("lucene47", "lucene9"), props.getStoreTargets());
    }

    @Test
    public void testNormalize_TypeIgnoredWhenActiveTargetDefined() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("type", "lucene", STRING);
        builder.setProperty("activeTarget", "lucene9", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        // type should be ignored, activeTarget used
        assertEquals("lucene9", props.getActiveTarget());
        assertEquals(Arrays.asList("lucene9"), props.getStoreTargets());
    }

    @Test
    public void testGetActiveTarget_ConvenienceMethod() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("type", "lucene", STRING);

        String activeTarget = IndexDefinitionHelper.getActiveTarget(builder.getNodeState());

        assertEquals("lucene", activeTarget);
    }

    @Test
    public void testGetStoreTargets_ConvenienceMethod() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("type", "lucene", STRING);

        List<String> storeTargets = IndexDefinitionHelper.getStoreTargets(builder.getNodeState());

        assertEquals(Arrays.asList("lucene"), storeTargets);
    }

    @Test
    public void testNormalizedIndexProperties_ImmutableStoreTargets() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);
        builder.setProperty("activeTarget", "lucene47", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        try {
            props.getStoreTargets().add("elasticsearch");
            fail("Should not be able to modify storeTargets list");
        } catch (UnsupportedOperationException e) {
            // Expected
        }
    }

    @Test
    public void testNormalizedIndexProperties_ToString() {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setProperty("storeTargets", Arrays.asList("lucene47", "lucene9"), STRINGS);
        builder.setProperty("activeTarget", "lucene47", STRING);

        NormalizedIndexProperties props = IndexDefinitionHelper.normalize(builder.getNodeState());

        String str = props.toString();
        assertTrue(str.contains("storeTargets"));
        assertTrue(str.contains("activeTarget"));
        assertTrue(str.contains("lucene47"));
        assertTrue(str.contains("lucene9"));
    }
}
