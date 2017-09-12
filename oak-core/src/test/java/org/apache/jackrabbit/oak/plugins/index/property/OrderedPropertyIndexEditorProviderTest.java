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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.List;

import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class OrderedPropertyIndexEditorProviderTest {
    private final CommitHook hook = new EditorHook(new IndexUpdateProvider(
        new OrderedPropertyIndexEditorProvider()));
    private final LogCustomizer custom = LogCustomizer
        .forLogger(OrderedPropertyIndexEditorProvider.class.getName()).enable(Level.WARN).create();
    
    private final String indexName = "mickey";
    private final String indexedProperty = "mouse";

    private final String DEPRECATION_MESSAGE = OrderedIndex.DEPRECATION_MESSAGE.replace("{}",
            "/" + INDEX_DEFINITIONS_NAME + "/" + indexName);
    
    private Tree createIndexDef(NodeBuilder root) throws RepositoryException {
        return IndexUtils
        .createIndexDefinition(
            TreeFactory.createTree(root
                .child(IndexConstants.INDEX_DEFINITIONS_NAME)), indexName, false,
            ImmutableList.of(indexedProperty), null, OrderedIndex.TYPE, null);
    }
    
    @Test
    public void withIndexDefSingleNode() throws RepositoryException, CommitFailedException {
        NodeBuilder root = EMPTY_NODE.builder();
        
        createIndexDef(root);
        
        NodeState before = root.getNodeState();
        root.child("n1").setProperty(indexedProperty, "dead");
        NodeState after = root.getNodeState();

        custom.starting();
        root = hook.processCommit(before, after, CommitInfo.EMPTY).builder();
        assertEquals(1, custom.getLogs().size());
        assertThat(custom.getLogs(), hasItem(DEPRECATION_MESSAGE));
        custom.finished();
        
        NodeBuilder b = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NODE_TYPE)
            .getChildNode(indexName).getChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME);
        assertFalse("nothing should have been touched under the actual index", b.exists());
    }
    
    @Test
    public void withIndexMultipleNodes() throws RepositoryException, CommitFailedException {
        final int threshold = 5;
        final int nodes = 16;
        final int traces = 1 + (nodes - 1) / threshold;
        OrderedPropertyIndexEditorProvider.setThreshold(threshold);
        final List<String> expected = Collections.nCopies(traces, DEPRECATION_MESSAGE);
        NodeBuilder root = EMPTY_NODE.builder();
        createIndexDef(root);

        custom.starting();
        for (int i = 0; i < nodes; i++) {
            NodeState before = root.getNodeState();
            root.child("n" + i).setProperty(indexedProperty, "dead" + i);
            NodeState after = root.getNodeState();
            root = hook.processCommit(before, after, CommitInfo.EMPTY).builder();
        }
        
        assertThat(custom.getLogs(), is(expected));
        custom.finished();
        assertFalse(root.getChildNode(INDEX_DEFINITIONS_NAME).getChildNode(indexName)
            .getChildNode(INDEX_CONTENT_NODE_NAME).exists());
    }

    @Test
    public void providerShouldBeAvailable() throws Exception {
        CommitHook hook = new EditorHook(new IndexUpdateProvider(
                new OrderedPropertyIndexEditorProvider(), null, true));

        NodeBuilder root = EMPTY_NODE.builder();

        createIndexDef(root).setProperty("reindex", false);

        NodeState before = root.getNodeState();
        root.child("foo");
        NodeState after = root.getNodeState();

        hook.processCommit(before, after, CommitInfo.EMPTY);
    }
}
