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
package org.apache.jackrabbit.oak.plugins.index.optimizer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.stream.Collectors;
import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

public class IndexDefinitionBuilder {
    private final NodeBuilder builder = EMPTY_NODE.builder();
    private final Map<String, IndexRule> rules = new HashMap<>();
    private final Map<String, AggregateRule> aggRules = new HashMap<>();
    private final NodeBuilder indexRule;
    private NodeBuilder aggregateBuilder;

    public IndexDefinitionBuilder() {
        builder.setProperty("compatVersion", 2);
        builder.setProperty("async", "async");
        builder.setProperty("type", "lucene");
        builder.setProperty("evaluatePathRestrictions", true);
        builder.setProperty(JCR_PRIMARYTYPE, "oak:QueryIndexDefinition", Type.NAME);
        indexRule = createChild(builder, "indexRules");
    }

    public IndexDefinitionBuilder evaluatePathRestrictions(){
        builder.setProperty("evaluatePathRestrictions", true);
        return this;
    }

    public IndexDefinitionBuilder includedPaths(String ... paths){
        builder.setProperty(createProperty(PathFilter.PROP_INCLUDED_PATHS, Arrays.asList(paths), Type.STRINGS));
        return this;
    }

    public IndexDefinitionBuilder queryPaths(String ... paths){
        builder.setProperty(createProperty(IndexConstants.QUERY_PATHS, Arrays.asList(paths), Type.STRINGS));
        return this;
    }

    public IndexDefinitionBuilder excludedPaths(String ... paths){
        builder.setProperty(createProperty(PathFilter.PROP_EXCLUDED_PATHS, Arrays.asList(paths), Type.STRINGS));
        return this;
    }

    public IndexDefinitionBuilder tags(String... tags) {
        builder.setProperty(IndexConstants.INDEX_TAGS, Arrays.asList(tags), Type.STRINGS);
        return this;
    }

    public NodeState build(){
        return builder.getNodeState();
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
        private final NodeBuilder builder;
        private final NodeBuilder propertiesBuilder;
        private final String ruleName;
        private final Map<String, PropertyRule> props = new HashMap<>();
        private final Set<String> propNodeNames = new HashSet<>();

        private IndexRule(NodeBuilder builder, String type) {
            this.builder = builder;
            this.propertiesBuilder = createChild(builder, "properties");
            this.ruleName = type;
        }

        public IndexRule indexNodeName(){
            builder.setProperty("indexNodeName", true);
            return this;
        }

        public PropertyRule property(String name){
            PropertyRule propRule = props.get(name);
            if (propRule == null) {
                if (name.equals("jcr:path")) {
                    propRule = new PropertyRule(this, createChild(propertiesBuilder, createPropNodeName(name)), name);
                    propRule.function("path()");
                } else {
                    propRule = new PropertyRule(this, createChild(propertiesBuilder, createPropNodeName(name)), name);
                }
                props.put(name, propRule);
            }
            return propRule;
        }

        private String createPropNodeName(String name) {
            name = getSafePropName(name);
            if (name.isEmpty()) {
                name = "prop";
            }
            if (propNodeNames.contains(name)) {
                name = name + "_" + propNodeNames.size();
            }
            propNodeNames.add(name);
            return name;
        }

        public String getRuleName() {
            return ruleName;
        }

        @Override
        public String toString() {
            String propsString = props.isEmpty()
                ? "None"
                : props.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining(", ", "[", "]"));

            return String.format("IndexRule{ruleName='%s', properties=%s, indexNodeName=%s}",
                ruleName,
                propsString,
                builder.getProperty("indexNodeName"));
        }


    }

    public static class PropertyRule {
        private final IndexRule indexRule;
        private final NodeBuilder builder;

        private PropertyRule(IndexRule indexRule, NodeBuilder builder, String name) {
            this.indexRule = indexRule;
            this.builder = builder;
            builder.setProperty(LuceneIndexConstants.PROP_NAME, name);
        }

        public PropertyRule useInExcerpt(){
            builder.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, true);
            return this;
        }

        public PropertyRule analyzed(){
            builder.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
            return this;
        }

        public PropertyRule nodeScopeIndex(){
            builder.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
            return this;
        }

        public PropertyRule ordered(){
            builder.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
            return this;
        }

        public PropertyRule ordered(String type){
            //This would throw an IAE if type is invalid
            PropertyType.valueFromName(type);
            builder.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
            builder.setProperty(LuceneIndexConstants.PROP_TYPE, type);
            return this;
        }

        public PropertyRule propertyIndex(){
            builder.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);
            return this;
        }

        public PropertyRule nullCheckEnabled(){
            builder.setProperty(LuceneIndexConstants.PROP_NULL_CHECK_ENABLED, true);
            return this;
        }

        public PropertyRule notNullCheckEnabled(){
            builder.setProperty(LuceneIndexConstants.PROP_NOT_NULL_CHECK_ENABLED, true);
            return this;
        }

        public PropertyRule function(String fnName) {
            builder.setProperty(LuceneIndexConstants.FUNC_NAME, fnName);
            builder.removeProperty(LuceneIndexConstants.PROP_NAME);
            return this;
        }

        public IndexRule enclosingRule(){
            return indexRule;
        }
    }

    //~--------------------------------------< Aggregates >

    public AggregateRule aggregateRule(String type){
        if (aggregateBuilder == null){
            aggregateBuilder = createChild(builder, LuceneIndexConstants.AGGREGATES);
        }
        AggregateRule rule = aggRules.get(type);
        if (rule == null){
            rule = new AggregateRule(createChild(aggregateBuilder, type));
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
        private final NodeBuilder builder;
        private final Map<String, Include> includes = new HashMap<>();

        private AggregateRule(NodeBuilder builder) {
            this.builder = builder;
        }

        public Include include(String includePath) {
            Include include = includes.get(includePath);
            if (include == null){
                include = new Include(createChild(builder, "include" + includes.size()));
                includes.put(includePath, include);
            }
            include.path(includePath);
            return include;
        }

        public static class Include {
            private final NodeBuilder builder;

            private Include(NodeBuilder builder) {
                this.builder = builder;
            }

            public Include path(String includePath) {
                builder.setProperty(LuceneIndexConstants.AGG_PATH, includePath);
                return this;
            }

            public Include relativeNode(){
                builder.setProperty(LuceneIndexConstants.AGG_RELATIVE_NODE, true);
                return this;
            }
        }
    }

    private static NodeBuilder createChild(NodeBuilder builder, String name){
        NodeBuilder result = builder.child(name);
        result.setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME);
        return result;
    }

    // TODO: document examples of this
    // when the property is like jcr:primaryType or something like this.
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
