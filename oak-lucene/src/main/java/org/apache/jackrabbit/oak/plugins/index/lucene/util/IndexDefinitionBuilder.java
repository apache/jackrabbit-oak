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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.state.EqualsDiff;
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
    private final NodeBuilder builder;
    private final Tree tree;
    private final Map<String, IndexRule> rules = Maps.newHashMap();
    private final Map<String, AggregateRule> aggRules = Maps.newHashMap();
    private final Tree indexRule;
    private final boolean autoManageReindexFlag;
    private Tree aggregatesTree;
    private final NodeState initial;
    private boolean reindexRequired;


    public IndexDefinitionBuilder(){
        this(EMPTY_NODE.builder());
    }

    public IndexDefinitionBuilder(NodeBuilder nodeBuilder){
        this(nodeBuilder, true);
    }

    public IndexDefinitionBuilder(NodeBuilder nodeBuilder, boolean autoManageReindexFlag){
        this.autoManageReindexFlag = autoManageReindexFlag;
        this.builder = nodeBuilder;
        this.initial = nodeBuilder.getNodeState();
        this.tree = TreeFactory.createTree(builder);
        tree.setProperty(LuceneIndexConstants.COMPAT_MODE, 2);
        tree.setProperty("async", "async");
        setType();
        tree.setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition", NAME);
        indexRule = getOrCreateChild(tree, LuceneIndexConstants.INDEX_RULES);
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

    public IndexDefinitionBuilder mergePolicy(String mergePolicy) {
        tree.setProperty(LuceneIndexConstants.MERGE_POLICY_NAME, checkNotNull(mergePolicy));
        return this;
    }

    public IndexDefinitionBuilder noAsync(){
        tree.removeProperty("async");
        return this;
    }

    public IndexDefinitionBuilder async(String ... asyncVals){
        tree.removeProperty("async");
        tree.setProperty("async", asList(asyncVals), STRINGS);
        return this;
    }

    public Tree getBuilderTree(){
        return tree;
    }

    public NodeState build(){
        setReindexFlagIfRequired();
        return builder.getNodeState();
    }

    public Tree build(Tree tree){
        NodeStateCopyUtils.copyToTree(build(), tree);
        return tree;
    }

    public Node build(Node node) throws RepositoryException {
        NodeStateCopyUtils.copyToNode(build(), node);
        return node;
    }

    public boolean isReindexRequired() {
        if (reindexRequired){
            return true;
        }
        return !SelectiveEqualsDiff.equals(initial, builder.getNodeState());
    }

    private void setReindexFlagIfRequired(){
        if (!reindexRequired && !SelectiveEqualsDiff.equals(initial, builder.getNodeState()) && autoManageReindexFlag){
            tree.setProperty("reindex", true);
            reindexRequired = true;
        }
    }

    private void setType() {
        PropertyState type = tree.getProperty(IndexConstants.TYPE_PROPERTY_NAME);
        if (type == null || !"disabled".equals(type.getValue(Type.STRING))) {
            tree.setProperty(IndexConstants.TYPE_PROPERTY_NAME, "lucene");
        }
    }

    //~--------------------------------------< IndexRule >

    public IndexRule indexRule(String type){
        IndexRule rule = rules.get(type);
        if (rule == null){
            rule = new IndexRule(getOrCreateChild(indexRule, type), type);
            rules.put(type, rule);
        }
        return rule;
    }

    public boolean hasIndexRule(String type){
        return indexRule.hasChild(type);
    }

    public static class IndexRule {
        private final Tree indexRule;
        private final Tree propsTree;
        private final String ruleName;
        private final Map<String, PropertyRule> props = Maps.newHashMap();
        private final Set<String> propNodeNames = Sets.newHashSet();

        private IndexRule(Tree indexRule, String type) {
            this.indexRule = indexRule;
            this.propsTree = getOrCreateChild(indexRule, LuceneIndexConstants.PROP_NODE);
            this.ruleName = type;
            loadExisting();
        }

        public IndexRule indexNodeName(){
            indexRule.setProperty(LuceneIndexConstants.INDEX_NODE_NAME, true);
            return this;
        }

        public IndexRule includePropertyTypes(String ... types){
            indexRule.setProperty(LuceneIndexConstants.INCLUDE_PROPERTY_TYPES, asList(types), STRINGS);
            return this;
        }

        public PropertyRule property(String name){
            return property(name, false);
        }

        public PropertyRule property(String name, boolean regex) {
            return property(null, name, regex);
        }

        public PropertyRule property(String propDefnNodeName, String name) {
            return property(propDefnNodeName, name, false);
        }

        public PropertyRule property(String propDefnNodeName, String name, boolean regex){
            PropertyRule propRule = props.get(name);
            if (propRule == null){
                Tree propTree = findExisting(name);
                if (propTree == null){
                    if (propDefnNodeName == null){
                        propDefnNodeName = createPropNodeName(name, regex);
                    }
                    propTree = getOrCreateChild(propsTree, propDefnNodeName);
                }
                propRule = new PropertyRule(this, propTree, name, regex);
                props.put(name, propRule);
            }
            return propRule;
        }

        private void loadExisting() {
            for (Tree tree : propsTree.getChildren()){
                if (!tree.hasProperty(LuceneIndexConstants.PROP_NAME)){
                    continue;
                }
                String name = tree.getProperty(LuceneIndexConstants.PROP_NAME).getValue(Type.STRING);
                boolean regex = false;
                if (tree.hasProperty(LuceneIndexConstants.PROP_IS_REGEX)) {
                    regex = tree.getProperty(LuceneIndexConstants.PROP_IS_REGEX).getValue(Type.BOOLEAN);
                }
                PropertyRule pr = new PropertyRule(this, tree, name, regex);
                props.put(name, pr);
            }
        }

        private Tree findExisting(String name) {
            for (Tree tree : propsTree.getChildren()){
                if (name.equals(tree.getProperty(LuceneIndexConstants.PROP_NAME).getValue(Type.STRING))){
                    return tree;
                }
            }
            return null;
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

        public boolean hasPropertyRule(String propName){
            return findExisting(propName) != null;
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

        public PropertyRule useInSpellcheck(){
            propTree.setProperty(LuceneIndexConstants.PROP_USE_IN_SPELLCHECK, true);
            return this;
        }

        public PropertyRule type(String type){
            //This would throw an IAE if type is invalid
            PropertyType.valueFromName(type);
            propTree.setProperty(LuceneIndexConstants.PROP_TYPE, type);
            return this;
        }

        public PropertyRule useInSuggest(){
            propTree.setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
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
            type(type);
            propTree.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
            return this;
        }

        public PropertyRule disable() {
            propTree.setProperty(LuceneIndexConstants.PROP_INDEX, false);
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

        public PropertyRule weight(int weight){
            propTree.setProperty(LuceneIndexConstants.PROP_WEIGHT, weight);
            return this;
        }

        public IndexRule enclosingRule(){
            return indexRule;
        }

        public Tree getBuilderTree(){
            return propTree;
        }

        public PropertyRule property(String name){
            return indexRule.property(name, false);
        }

        public PropertyRule property(String name, boolean regex) {
            return indexRule.property(null, name, regex);
        }

        public PropertyRule property(String propDefnNodeName, String name) {
            return indexRule.property(propDefnNodeName, name, false);
        }
    }

    //~--------------------------------------< Aggregates >

    public AggregateRule aggregateRule(String type){
        if (aggregatesTree == null){
            aggregatesTree = getOrCreateChild(tree, LuceneIndexConstants.AGGREGATES);
        }
        AggregateRule rule = aggRules.get(type);
        if (rule == null){
            rule = new AggregateRule(getOrCreateChild(aggregatesTree, type));
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
            loadExisting(aggregate);
        }

        public Include include(String includePath) {
            Include include = includes.get(includePath);
            if (include == null){
                Tree includeTree = findExisting(includePath);
                if (includeTree == null){
                    includeTree = getOrCreateChild(aggregate, "include" + includes.size());
                }
                include = new Include(this, includeTree);
                includes.put(includePath, include);
            }
            include.path(includePath);
            return include;
        }

        private Tree findExisting(String includePath) {
            for (Tree tree : aggregate.getChildren()){
                if (includePath.equals(tree.getProperty(LuceneIndexConstants.AGG_PATH).getValue(Type.STRING))){
                    return tree;
                }
            }
            return null;
        }

        private void loadExisting(Tree aggregate) {
            for (Tree tree : aggregate.getChildren()){
                if (tree.hasProperty(LuceneIndexConstants.AGG_PATH)) {
                    Include include = new Include(this, tree);
                    includes.put(include.getPath(), include);
                }
            }
        }

        public static class Include {
            private final AggregateRule aggregateRule;
            private final Tree include;

            private Include(AggregateRule aggregateRule, Tree include) {
                this.aggregateRule = aggregateRule;
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

            public Include include(String path){
                return aggregateRule.include(path);
            }

            public String getPath(){
                return include.getProperty(LuceneIndexConstants.AGG_PATH).getValue(Type.STRING);
            }
        }
    }

    private static Tree getOrCreateChild(Tree tree, String name){
        if (tree.hasChild(name)){
            return tree.getChild(name);
        }
        Tree child = tree.addChild(name);
        child.setOrderableChildren(true);
        child.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, NAME);
        return child;
    }

    static class SelectiveEqualsDiff extends EqualsDiff {
        public static boolean equals(NodeState before, NodeState after) {
            return before.exists() == after.exists()
                    && after.compareAgainstBaseState(before, new SelectiveEqualsDiff());
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            if (IndexConstants.ASYNC_PROPERTY_NAME.equals(before.getName())){
                Set<String> asyncBefore = getAsyncValuesWithoutNRT(before);
                Set<String> asyncAfter = getAsyncValuesWithoutNRT(after);
                return asyncBefore.equals(asyncAfter);
            }
            return false;
        }

        private Set<String> getAsyncValuesWithoutNRT(PropertyState state){
            Set<String> async = Sets.newHashSet(state.getValue(Type.STRINGS));
            async.remove(IndexConstants.INDEXING_MODE_NRT);
            async.remove(IndexConstants.INDEXING_MODE_SYNC);
            return async;
        }
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
