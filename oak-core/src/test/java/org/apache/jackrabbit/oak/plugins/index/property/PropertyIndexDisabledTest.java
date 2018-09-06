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
package org.apache.jackrabbit.oak.plugins.index.property;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor.COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.spi.commit.CommitInfo.EMPTY;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.InitialContentHelper;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Test the Property2 index mechanism.
 */
public class PropertyIndexDisabledTest {

    private static final int MANY = 100;

    private NodeState root;
    private NodeBuilder rootBuilder;
    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));
 
    @Before
    public void setup() throws Exception {
        root = EmptyNodeState.EMPTY_NODE;
        rootBuilder = InitialContentHelper.INITIAL_CONTENT.builder();
        commit();
    }

    @Test
    public void disabled() throws Exception {
        NodeBuilder index = createIndexDefinition(rootBuilder.child(INDEX_DEFINITIONS_NAME), 
                "foo", true, false, ImmutableSet.of("foo"), null);
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/");
        commit();
        for (int i = 0; i < MANY; i++) {
            rootBuilder.child("test").child("n" + i).setProperty("foo", "x" + i % 20);
        }
        commit();
        FilterImpl f = createFilter(root, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("x10"));
        PropertyIndex propertyIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertTrue(propertyIndex.getCost(f, root) != Double.POSITIVE_INFINITY);
        assertEquals("property foo = x10", propertyIndex.getPlan(f, root));

        // now test with a node that doesn't exist
        index = rootBuilder.child(INDEX_DEFINITIONS_NAME).child("foo");
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/doesNotExist");
        commit();
        // need to create a new one - otherwise the cached plan is used
        propertyIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertTrue(propertyIndex.getCost(f, root) == Double.POSITIVE_INFINITY);
        assertEquals("property index not applicable", propertyIndex.getPlan(f, root));
        
        // test with a property that does exist
        index = rootBuilder.child(INDEX_DEFINITIONS_NAME).child("foo");
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/oak:index/@jcr:primaryType");
        commit();
        // need to create a new one - otherwise the cached plan is used
        propertyIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertTrue(propertyIndex.getCost(f, root) != Double.POSITIVE_INFINITY);
        assertEquals("property foo = x10", propertyIndex.getPlan(f, root));
        
        // test with a property that does not exist
        index = rootBuilder.child(INDEX_DEFINITIONS_NAME).child("foo");
        index.setProperty(IndexConstants.USE_IF_EXISTS, "/oak:index/@unknownProperty");
        commit();
        // need to create a new one - otherwise the cached plan is used
        propertyIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertTrue(propertyIndex.getCost(f, root) == Double.POSITIVE_INFINITY);
        assertEquals("property index not applicable", propertyIndex.getPlan(f, root));
    }
    
    private void commit() throws Exception {
        root = HOOK.processCommit(rootBuilder.getBaseState(), rootBuilder.getNodeState(), EMPTY);
        rootBuilder = root.builder();
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);        
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
    }

}
