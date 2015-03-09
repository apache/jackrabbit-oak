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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.NodeTypeRegistry;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.state.ApplyDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestUtil {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static final String NT_TEST = "oak:TestNode";

    public static final String TEST_NODE_TYPE = "[oak:TestNode]\n" +
            " - * (UNDEFINED) multiple\n" +
            " - * (UNDEFINED)\n" +
            " + * (nt:base) = oak:TestNode VERSION";

    static void useV2(NodeBuilder idxNb) {
        idxNb.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
    }

    static void useV2(Tree idxTree) {
        idxTree.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V2.getVersion());
    }

    public static NodeBuilder newLuceneIndexDefinitionV2(
            @Nonnull NodeBuilder index, @Nonnull String name,
            @Nullable Set<String> propertyTypes) {
        NodeBuilder nb = LuceneIndexHelper.newLuceneIndexDefinition(index, name, propertyTypes, null, null, null);
        useV2(nb);
        return nb;
    }

    public static Tree enableForFullText(Tree props, String propName) {
        return enableForFullText(props, propName, false);
    }

    public static Tree enableForFullText(Tree props, String propName,  boolean regex) {
        Tree prop = props.addChild(unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, regex);
        prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
        prop.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true);
        prop.setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true);
        return prop;
    }

    public static Tree enablePropertyIndex(Tree props, String propName,  boolean regex) {
        Tree prop = props.addChild(unique("prop"));
        prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);
        prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
        prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, regex);
        prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, false);
        prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, false);
        return prop;
    }

    public static AggregatorBuilder newNodeAggregator(Tree indexDefn){
        return new AggregatorBuilder(indexDefn);
    }

    public static Tree newRulePropTree(Tree indexDefn, String typeName){
        Tree rules = indexDefn.addChild(LuceneIndexConstants.INDEX_RULES);
        rules.setOrderableChildren(true);
        Tree rule = rules.addChild(typeName);
        Tree props = rule.addChild(LuceneIndexConstants.PROP_NODE);
        props.setOrderableChildren(true);
        return props;
    }

    public static NodeBuilder child(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    static class AggregatorBuilder {
        private final Tree aggs;

        private AggregatorBuilder(Tree indexDefn) {
            this.aggs = indexDefn.addChild(LuceneIndexConstants.AGGREGATES);
        }

        AggregatorBuilder newRuleWithName(String primaryType,
                                          List<String> includes){
            Tree agg = aggs.addChild(primaryType);
            for (String include : includes){
                agg.addChild(unique("include")).setProperty(LuceneIndexConstants.AGG_PATH, include);
            }
            return this;
        }
    }

    static String unique(String name){
        return name + COUNTER.getAndIncrement();
    }

    public static NodeBuilder registerTestNodeType(NodeBuilder builder){
        registerNodeType(builder, TEST_NODE_TYPE);
        return builder;
    }

    public static void registerNodeType(NodeBuilder builder, String nodeTypeDefn){
        //Taken from org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent
        NodeState base = ModifiedNodeState.squeeze(builder.getNodeState());
        NodeStore store = new MemoryNodeStore(base);
        Root root = new SystemRoot(
                store, new EditorHook(new CompositeEditorProvider(
                new NamespaceEditorProvider(),
                new TypeEditorProvider())));
        NodeTypeRegistry.register(root, IOUtils.toInputStream(nodeTypeDefn), "test node types");
        NodeState target = store.getRoot();
        target.compareAgainstBaseState(base, new ApplyDiff(builder));
    }

    public static Tree createNodeWithType(Tree t, String nodeName, String typeName){
        t = t.addChild(nodeName);
        t.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return t;
    }

    public static NodeBuilder createNodeWithType(NodeBuilder builder, String nodeName, String typeName){
        builder = builder.child(nodeName);
        builder.setProperty(JcrConstants.JCR_PRIMARYTYPE, typeName, Type.NAME);
        return builder;
    }
}
