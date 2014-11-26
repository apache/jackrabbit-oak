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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

public class TestUtil {
    private static final AtomicInteger COUNTER = new AtomicInteger();

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

    private static String unique(String name){
        return name + COUNTER.getAndIncrement();
    }
}
