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

package org.apache.jackrabbit.oak.plugins.index.upgrade;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_DISABLED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class IndexDisablerTest {
    private NodeBuilder builder = EMPTY_NODE.builder();
    private NodeBuilder rootBuilder = EMPTY_NODE.builder();
    private IndexDisabler disabler = new IndexDisabler(rootBuilder);

    @Test
    public void simpleIndex() throws Exception{
        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes.isEmpty());
    }

    @Test
    public void disableIndexes() throws Exception{
        rootBuilder.child("oak:index").child("fooIndex");

        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex", "/oak:index/barIndex"), Type.STRINGS);

        refreshBuilder();

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertThat(disabledIndexes, containsInAnyOrder("/oak:index/fooIndex"));
        assertFalse(builder.getBoolean(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE));
        assertEquals(TYPE_DISABLED,
                rootBuilder.getChildNode("oak:index").getChildNode("fooIndex").getString(TYPE_PROPERTY_NAME));

        //Check no node created for non existing node
        assertFalse(rootBuilder.getChildNode("oak:index").getChildNode("barIndex").exists());

        refreshBuilder();
        List<String> disabledIndexes2 = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes2.isEmpty());
    }

    private void refreshBuilder() {
        builder = builder.getNodeState().builder();
    }

    /**
     * Test that indexes are not disabled in same cycle
     * as when reindexing is done
     */
    @Test
    public void reindexCase() throws Exception{
        rootBuilder.child("oak:index").child("fooIndex");

        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex", "/oak:index/barIndex"), Type.STRINGS);

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes.isEmpty());
    }

    @Test
    public void nodeTypeIndexDisabling_noop() throws Exception{
        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@bar"), Type.STRINGS);

        refreshBuilder();

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes.isEmpty());
    }

    @Test
    public void nodeTypeIndexDisabling_noDeclaringTypes() throws Exception{
        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        rootBuilder.child("oak:index").child("fooIndex");
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@bar"), Type.STRINGS);

        refreshBuilder();

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes.isEmpty());
    }

    @Test
    public void nodeTypeIndexDisabling_typeNotExist() throws Exception{
        createIndexDefinition(rootBuilder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), asList("oak:TestNode"));
        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@oak:BarType"), Type.STRINGS);

        refreshBuilder();

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertTrue(disabledIndexes.isEmpty());
    }

    @Test
    public void nodeTypeIndexDisabling_typeExist() throws Exception{
        createIndexDefinition(rootBuilder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), asList("oak:TestNode", "oak:BarType"));

        builder.setProperty(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE, true);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@oak:BarType"), Type.STRINGS);

        refreshBuilder();

        List<String> disabledIndexes = disabler.disableOldIndexes("/oak:index/foo", builder);
        assertThat(disabledIndexes, containsInAnyOrder("/oak:index/fooIndex/@oak:BarType"));
        assertFalse(builder.getBoolean(IndexConstants.DISABLE_INDEXES_ON_NEXT_CYCLE));


        PropertyState declaringNodeType = rootBuilder.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode("fooIndex").getProperty(DECLARING_NODE_TYPES);
        assertEquals(Type.NAMES, declaringNodeType.getType());

        Set<String> names = Sets.newHashSet(declaringNodeType.getValue(Type.NAMES));
        assertThat(names, containsInAnyOrder("oak:TestNode"));
    }

    //~-------------------------------< anyIndexToBeDisabled >

    @Test
    public void indexToBeDisabled_Noop() throws Exception{
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void indexToBeDisabled_PathNotExists() throws Exception{
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex", "/oak:index/barIndex"), Type.STRINGS);
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void indexToBeDisabled_PathExistsButDisabled() throws Exception{
        rootBuilder.child("oak:index").child("fooIndex").setProperty(TYPE_PROPERTY_NAME, TYPE_DISABLED);
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex", "/oak:index/barIndex"), Type.STRINGS);
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void indexToBeDisabled_PathExists() throws Exception{
        rootBuilder.child("oak:index").child("fooIndex").setProperty(TYPE_PROPERTY_NAME, "property");
        recreateDisabler();

        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex", "/oak:index/barIndex"), Type.STRINGS);
        assertTrue(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertTrue(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void nodeTypeIndexToBeDisabled_PathNotExists() throws Exception{
        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@bar", "/oak:index/barIndex"), Type.STRINGS);
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void nodeTypeIndexToBeDisabled_DeclaringTypeNotExists() throws Exception{
        rootBuilder.child("oak:index").child("fooIndex");
        recreateDisabler();

        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@bar", "/oak:index/barIndex"), Type.STRINGS);
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    @Test
    public void nodeTypeIndexToBeDisabled_TypeNotExists() throws Exception{
        createIndexDefinition(rootBuilder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), asList("oak:TestNode"));
        recreateDisabler();

        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@bar", "/oak:index/barIndex"), Type.STRINGS);
        assertFalse(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertFalse(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }


    @Test
    public void nodeTypeIndexToBeDisabled_TypeExists() throws Exception{
        createIndexDefinition(rootBuilder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo"), asList("oak:TestNode"));
        recreateDisabler();

        builder.setProperty(IndexConstants.SUPERSEDED_INDEX_PATHS,
                asList("/oak:index/fooIndex/@oak:TestNode", "/oak:index/barIndex"), Type.STRINGS);
        assertTrue(disabler.markDisableFlagIfRequired("/oak:index/foo", builder));
        assertTrue(builder.getBoolean(DISABLE_INDEXES_ON_NEXT_CYCLE));
    }

    private void recreateDisabler() {
        disabler = new IndexDisabler(rootBuilder.getNodeState().builder());
    }

}