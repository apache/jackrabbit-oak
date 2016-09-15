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

import java.util.Map;
import java.util.Set;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public final class IndexDefinitionBuilder {
    private final NodeBuilder builder = EMPTY_NODE.builder();
    private final Tree tree = TreeFactory.createTree(builder);
    private final Map<String, IndexRule> rules = Maps.newHashMap();
    private final Map<String, AggregateRule> aggRules = Maps.newHashMap();
    private final Tree indexRule;
    private Tree aggregatesTree;

    public IndexDefinitionBuilder(){
        tree.setProperty(LuceneIndexConstants.COMPAT_MODE, 2);
        tree.setProperty("async", "async");
        tree.setProperty("reindex", true);
        tree.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "lucene");
        tree.setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition", NAME);
        indexRule = createChild(tree, LuceneIndexConstants.INDEX_RULES);
    }

    public IndexDefinitionBuilder evaluatePathRestrictions(){
        tree.setProperty(LuceneIndexConstants.EVALUATE_PATH_RESTRICTION, true);
        return this;
    }

    public IndexDefinitionBuilder includedPaths(String ... paths){
        tree.setProperty(PathFilter.PROP_INCLUDED_PATHS, asList(paths), STRINGS);
        return this;
    }

    public IndexDefinitionBuilder excludedPaths(String ... paths){
        tree.setProperty(PathFilter.PROP_EXCLUDED_PATHS, asList(paths), STRINGS);
        return this;
    }

    public IndexDefinitionBuilder codec(String codecName){
        tree.setProperty(LuceneIndexConstants.CODEC_NAME, checkNotNull(codecName));
        return this;
    }

    public IndexDefinitionBuilder async(String ... asyncVals){
        tree.removeProperty("async");
        tree.setProperty("async", asList(asyncVals), STRINGS);
        return this;
    }

    public NodeState build(){
        return builder.getNodeState();
    }

    public Tree build(Tree tree){
        NodeStateCopyUtils.copyToTree(builder.getNodeState(), tree);
        return tree;
    }

    public Node build(Node node) throws RepositoryException {
        NodeStateCopyUtils.copyToNode(builder.getNodeState(), node);
        return node;
    }


    //~--------------------------------------< IndexRule >

    public IndexRule indexRule(String type){
        IndexRule rule = rules.get(type);
        if (rule == null){
            rule = new IndexRule(createChild(indexRule, type), type);
            rules.put(type, rule);
        }
        return rule;
    }

    public static class IndexRule {
        private final Tree indexRule;
        private final Tree propsTree;
        private final String ruleName;
        private final Map<String, PropertyRule> props = Maps.newHashMap();
        private final Set<String> propNodeNames = Sets.newHashSet();

        private IndexRule(Tree indexRule, String type) {
            this.indexRule = indexRule;
            this.propsTree = createChild(indexRule, LuceneIndexConstants.PROP_NODE);
            this.ruleName = type;
        }

        public IndexRule indexNodeName(){
            indexRule.setProperty(LuceneIndexConstants.INDEX_NODE_NAME, true);
            return this;
        }

        public PropertyRule property(String name){
            return property(name, false);
        }

        public PropertyRule property(String name, boolean regex){
            PropertyRule propRule = props.get(name);
            if (propRule == null){
                propRule = new PropertyRule(this, createChild(propsTree, createPropNodeName(name, regex)), name, regex);
                props.put(name, propRule);
            }
            return propRule;
        }

        private String createPropNodeName(String name, boolean regex) {
            name = regex ? "prop" : getSafePropName(name);
            if (name.isEmpty()){
                name = "prop";
            }
            if (propNodeNames.contains(name)){
                name = name + "_" + propNodeNames.size();
            }
            propNodeNames.add(name);
            return name;
        }

        public String getRuleName() {
            return ruleName;
        }
    }

    public static class PropertyRule {
        private final IndexRule indexRule;
        private final Tree propTree;

        private PropertyRule(IndexRule indexRule, Tree propTree, String name, boolean regex) {
            this.indexRule = indexRule;
            this.propTree = propTree;
            propTree.setProperty(LuceneIndexConstants.PROP_NAME, name);
            if (regex) {
                propTree.setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
            }
        }

        public PropertyRule useInExcerpt(){
            propTree.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true);
            return this;
        }

        public PropertyRule analyzed(){
            propTree.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
            return this;
        }

        public PropertyRule nodeScopeIndex(){
            propTree.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
            return this;
        }

        public PropertyRule ordered(){
            propTree.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
            return this;
        }

        public PropertyRule ordered(String type){
            //This would throw an IAE if type is invalid
            PropertyType.valueFromName(type);
            propTree.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
            propTree.setProperty(LuceneIndexConstants.PROP_TYPE, type);
            return this;
        }

        public PropertyRule propertyIndex(){
            propTree.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
            return this;
        }

        public PropertyRule nullCheckEnabled(){
            propTree.setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
            return this;
        }

        public PropertyRule notNullCheckEnabled(){
            propTree.setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);
            return this;
        }

        public IndexRule enclosingRule(){
            return indexRule;
        }
    }

    //~--------------------------------------< Aggregates >

    public AggregateRule aggregateRule(String type){
        if (aggregatesTree == null){
            aggregatesTree = createChild(tree, LuceneIndexConstants.AGGREGATES);
        }
        AggregateRule rule = aggRules.get(type);
        if (rule == null){
            rule = new AggregateRule(createChild(aggregatesTree, type));
            aggRules.put(type, rule);
        }
        return rule;
    }

    public AggregateRule aggregateRule(String primaryType, String ... includes){
        AggregateRule rule = aggregateRule(primaryType);
        for (String include : includes){
            rule.include(include);
        }
        return rule;
    }

    public static class AggregateRule {
        private final Tree aggregate;
        private final Map<String, Include> includes = Maps.newHashMap();

        private AggregateRule(Tree aggregate) {
            this.aggregate = aggregate;
        }

        public Include include(String includePath) {
            Include include = includes.get(includePath);
            if (include == null){
                include = new Include(createChild(aggregate, "include" + includes.size()));
                includes.put(includePath, include);
            }
            include.path(includePath);
            return include;
        }

        public static class Include {
            private final Tree include;

            private Include(Tree include) {
                this.include = include;
            }

            public Include path(String includePath) {
                include.setProperty(LuceneIndexConstants.AGG_PATH, includePath);
                return this;
            }

            public Include relativeNode(){
                include.setProperty(LuceneIndexConstants.AGG_RELATIVE_NODE, true);
                return this;
            }
        }
    }

    private static Tree createChild(Tree tree, String name){
        Tree child = tree.addChild(name);
        child.setOrderableChildren(true);
        child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        return child;
    }

    static String getSafePropName(String relativePropName) {
        String propName = PathUtils.getName(relativePropName);
        int indexOfColon = propName.indexOf(':');
        if (indexOfColon > 0){
            propName = propName.substring(indexOfColon + 1);
        }

        //Just keep ascii chars
        propName = propName.replaceAll("\\W", "");
        return propName;
    }
}
