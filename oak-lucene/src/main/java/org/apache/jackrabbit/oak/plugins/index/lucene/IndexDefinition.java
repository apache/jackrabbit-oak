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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeTypeIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.lucene.codecs.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.BLOB_SIZE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.EXPERIMENTAL_STORAGE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FIELD_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.FULL_TEXT_ENABLED;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INCLUDE_PROPERTY_TYPES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.ORDERED_PROP_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.PROP_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition.DEFAULT_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getOptionalValue;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;

class IndexDefinition {
    private static final Logger log = LoggerFactory.getLogger(IndexDefinition.class);

    /**
     * Blob size to use by default. To avoid issues in OAK-2105 the size should not
     * be power of 2.
     */
    static final int DEFAULT_BLOB_SIZE = OakDirectory.DEFAULT_BLOB_SIZE - 300;

    /**
     * Default entry count to keep estimated entry count low.
     */
    static final long DEFAULT_ENTRY_COUNT = 1000;

    private static String TYPES_ALLOW_ALL_NAME = "all";

    private static final int TYPES_ALLOW_NONE = PropertyType.UNDEFINED;

    private static final int TYPES_ALLOW_ALL = -1;

    private final int propertyTypes;

    private final Set<String> excludes;

    private final Set<String> includes;

    private final boolean fullTextEnabled;

    private final boolean propertyIndexEnabled;

    private final NodeState definition;

    private final NodeState root;

    private final String funcName;

    private final int blobSize;

    private final Codec codec;

    private final Set<String> relativePropNames;

    private final List<RelativeProperty> relativeProperties;

    private final int relativePropsMaxLevels;

    /**
     * Defines the maximum estimated entry count configured.
     * Defaults to {#DEFAULT_ENTRY_COUNT}
     */
    private final long entryCount;

    /**
     * The {@link IndexingRule}s inside this configuration. Keys being the NodeType names
     */
    private final Map<String, List<IndexingRule>> indexRules;

    private final List<IndexingRule> definedRules;

    private final String indexName;

    public IndexDefinition(NodeState root, NodeState defn) {
        this(root, defn, null);
    }

    public IndexDefinition(NodeState root, NodeState defn, @Nullable String indexPath) {
        this.root = root;
        this.definition = defn;
        this.indexName = determineIndexName(defn, indexPath);
        this.propertyTypes = getSupportedTypes(defn, TYPES_ALLOW_ALL);

        this.excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        this.includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);

        this.blobSize = getOptionalValue(defn, BLOB_SIZE, DEFAULT_BLOB_SIZE);

        NodeState rulesState = defn.getChildNode(LuceneIndexConstants.INDEX_RULES);
        if (!rulesState.exists()){
            rulesState = createIndexRules(defn).getNodeState();
        }

        List<IndexingRule> definedIndexRules = newArrayList();
        this.indexRules = collectIndexRules(rulesState, definedIndexRules);
        this.definedRules = ImmutableList.copyOf(definedIndexRules);

        this.relativeProperties = collectRelativeProperties(definedIndexRules);
        this.fullTextEnabled = hasFulltextEnabledIndexRule(definedIndexRules);
        this.propertyIndexEnabled = hasPropertyIndexEnabledIndexRule(definedIndexRules);

        this.relativePropNames = collectRelPropertyNames(relativeProperties);
        this.relativePropsMaxLevels = getRelPropertyMaxLevels(relativeProperties);

        String functionName = getOptionalValue(defn, LuceneIndexConstants.FUNC_NAME, null);
        this.funcName = functionName != null ? "native*" + functionName : null;

        this.codec = createCodec();

        if (defn.hasProperty(ENTRY_COUNT_PROPERTY_NAME)) {
            this.entryCount = defn.getProperty(ENTRY_COUNT_PROPERTY_NAME).getValue(Type.LONG);
        } else {
            this.entryCount = DEFAULT_ENTRY_COUNT;
        }
    }

    boolean includeProperty(String name) {
        if(!includes.isEmpty()){
            return includes.contains(name);
        }
        return !excludes.contains(name.toLowerCase());
    }

    boolean includePropertyType(int type){
        if(propertyTypes < 0){
            return false;
        }
        return (propertyTypes & (1 << type)) != 0;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public boolean isPropertyIndexEnabled() {
        return propertyIndexEnabled;
    }

    public boolean skipTokenization(String propertyName) {
        //If fulltext is not enabled then we never tokenize
        //irrespective of property name
        if (!isFullTextEnabled()) {
            return true;
        }
        return LuceneIndexHelper.skipTokenization(propertyName);
    }

    public String getFunctionName(){
        return funcName;
    }

    public boolean hasFunctionDefined(){
        return funcName != null;
    }

    /**
     * Size in bytes for the blobs created while storing the index content
     * @return size in bytes
     */
    public int getBlobSize() {
        return blobSize;
    }

    public Codec getCodec() {
        return codec;
    }

    public long getReindexCount(){
        if(definition.hasProperty(REINDEX_COUNT)){
            return definition.getProperty(REINDEX_COUNT).getValue(Type.LONG);
        }
        return 0;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public Collection<RelativeProperty> getRelativeProps() {
        return relativeProperties;
    }

    /**
     * Collects the relative properties where the property name matches given name. Note
     * that multiple relative properties can end with same name e.g. foo/bar, baz/bar
     *
     * @param name property name without path
     * @param relProps matching relative properties where the relative property path ends
     *                 with given name
     */
    public void collectRelPropsForName(String name, Collection<RelativeProperty> relProps){
        if(hasRelativeProperty(name)){
            for(RelativeProperty rp : relativeProperties){
                if(rp.name.equals(name)){
                    relProps.add(rp);
                }
            }
        }
    }

    boolean hasRelativeProperties(){
        return !relativeProperties.isEmpty();
    }

    boolean hasRelativeProperty(String name) {
        return relativePropNames.contains(name);
    }

    @Override
    public String toString() {
        return "IndexDefinition : " + indexName;
    }

    //~------------------------------------------< Internal >

    private Map<String, RelativeProperty> collectRelativeProps(Iterable<String> propNames) {
        Map<String, RelativeProperty> relProps = newHashMap();
        for (String propName : propNames) {
            if (RelativeProperty.isRelativeProperty(propName)) {
                relProps.put(propName, new RelativeProperty(propName));
            }
        }
        return ImmutableMap.copyOf(relProps);
    }


    private int getRelPropertyMaxLevels(Collection<RelativeProperty> props) {
        int max = -1;
        for (RelativeProperty prop : props) {
            max = Math.max(max, prop.ancestors.length);
        }
        return max;
    }

    public int getRelPropertyMaxLevels() {
        return relativePropsMaxLevels;
    }

    private Codec createCodec() {
        String codecName = getOptionalValue(definition, LuceneIndexConstants.CODEC_NAME, null);
        Codec codec = null;
        if (codecName != null) {
            codec = Codec.forName(codecName);
        } else if (fullTextEnabled) {
            codec = new OakCodec();
        }
        return codec;
    }

    //~---------------------------------------------------< IndexRule >


    public List<IndexingRule> getDefinedRules() {
        return definedRules;
    }

    @CheckForNull
    public IndexingRule getApplicableIndexingRule(String primaryNodeType) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(primaryNodeType);
        if (r != null) {
            rules = new ArrayList<IndexingRule>();
            rules.addAll(r);
        }

        if (rules != null) {
            for (IndexingRule rule : rules) {
                if (rule.appliesTo(primaryNodeType)) {
                    return rule;
                }
            }
        }

        // no applicable rule
        return null;
    }

        /**
         * Returns the first indexing rule that applies to the given node
         * <code>state</code>.
         *
         * @param state a node state.
         * @return the indexing rule or <code>null</code> if none applies.
         */
    @CheckForNull
    public IndexingRule getApplicableIndexingRule(Tree state) {
        //This method would be invoked for every node. So be as
        //conservative as possible in object creation
        List<IndexingRule> rules = null;
        List<IndexingRule> r = indexRules.get(getPrimaryTypeName(state));
        if (r != null) {
            rules = new ArrayList<IndexingRule>();
            rules.addAll(r);
        }

        for (String name : getMixinTypeNames(state)) {
            r = indexRules.get(name);
            if (r != null) {
                if (rules == null) {
                    rules = new ArrayList<IndexingRule>();
                }
                rules.addAll(r);
            }
        }

        if (rules != null) {
            for (IndexingRule rule : rules) {
                if (rule.appliesTo(state)) {
                    return rule;
                }
            }
        }

        // no applicable rule
        return null;
    }

    private Map<String, List<IndexingRule>> collectIndexRules(NodeState indexRules,
                                                              List<IndexingRule> definedIndexRules){
        //TODO if a rule is defined for nt:base then this map would have entry for each
        //registered nodeType in the system

        if (!indexRules.exists()) {
            return Collections.emptyMap();
        }

        if (!hasOrderableChildren(indexRules)){
            log.warn("IndexRule node does not have orderable children in [{}]", IndexDefinition.this);
        }

        Map<String, List<IndexingRule>> nt2rules = newHashMap();
        ReadOnlyNodeTypeManager ntReg = createNodeTypeManager(new ImmutableTree(root));

        //Use Tree API to read ordered child nodes
        ImmutableTree ruleTree = new ImmutableTree(indexRules);
        final List<String> allNames = getAllNodeTypes(ntReg);
        for (Tree ruleEntry : ruleTree.getChildren()) {
            IndexingRule rule = new IndexingRule(ruleEntry.getName(), indexRules.getChildNode(ruleEntry.getName()));
            definedIndexRules.add(rule);

            // register under node type and all its sub types
            log.debug("Found rule '{}' for NodeType '{}'", rule, rule.getNodeTypeName());

            List<String> ntNames = allNames;
            if (!rule.inherited){
                //Trim the list to rule's nodeType so that inheritance check
                //is not performed for other nodeTypes
                ntNames = Collections.singletonList(rule.getNodeTypeName());
            }

            for (String ntName : ntNames) {
                if (ntReg.isNodeType(ntName, rule.getNodeTypeName())) {
                    List<IndexingRule> perNtConfig = nt2rules.get(ntName);
                    if (perNtConfig == null) {
                        perNtConfig = new ArrayList<IndexingRule>();
                        nt2rules.put(ntName, perNtConfig);
                    }
                    log.debug("Registering it for name '{}'", ntName);
                    perNtConfig.add(new IndexingRule(rule, ntName));
                }
            }
        }

        for (Map.Entry<String, List<IndexingRule>> e : nt2rules.entrySet()){
            e.setValue(ImmutableList.copyOf(e.getValue()));
        }

        return ImmutableMap.copyOf(nt2rules);
    }

    public class IndexingRule {
        private final String baseNodeType;
        private final String nodeTypeName;
        private final Map<String, PropertyDefinition> propConfigs;
        private final List<NamePattern> namePatterns;
        final float boost;
        final boolean inherited;
        final boolean defaultFulltextEnabled;
        final boolean defaultStorageEnabled;
        final int propertyTypes;
        final boolean fulltextEnabled;
        final boolean propertyIndexEnabled;

        //TODO To be relooked with support for aggregation
        final Map<String,RelativeProperty> relativeProps;
        final Set<String> relativePropNames;


        IndexingRule(String nodeTypeName, NodeState config) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = nodeTypeName;
            this.boost = getOptionalValue(config, FIELD_BOOST, DEFAULT_BOOST);
            this.inherited = getOptionalValue(config, LuceneIndexConstants.RULE_INHERITED, true);
            this.defaultFulltextEnabled = getOptionalValue(config, LuceneIndexConstants.FULL_TEXT_ENABLED, false);
            //TODO Provide a new proper propertyName for enabling storage
            this.defaultStorageEnabled = getOptionalValue(config, LuceneIndexConstants.EXPERIMENTAL_STORAGE, false);
            this.propertyTypes = getSupportedTypes(config, TYPES_ALLOW_ALL);

            List<NamePattern> namePatterns = newArrayList();
            Map<String,RelativeProperty> relativeProps = newHashMap();
            this.propConfigs = collectPropConfigs(config, namePatterns, relativeProps);

            this.namePatterns = ImmutableList.copyOf(namePatterns);
            this.fulltextEnabled = hasAnyFullTextEnabledProperty();
            this.propertyIndexEnabled = hasAnyPropertyIndexConfigured();
            this.relativeProps = ImmutableMap.copyOf(relativeProps);
            this.relativePropNames = collectRelPropertyNames(this.relativeProps.values());
        }

        /**
         * Creates a new indexing rule base on an existing one, but for a
         * different node type name.
         *
         * @param original the existing rule.
         * @param nodeTypeName the node type name for the rule.
         */
        IndexingRule(IndexingRule original, String nodeTypeName) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = original.getNodeTypeName();
            this.propConfigs = original.propConfigs;
            this.namePatterns = original.namePatterns;
            this.boost = original.boost;
            this.defaultFulltextEnabled = original.defaultFulltextEnabled;
            this.defaultStorageEnabled = original.defaultStorageEnabled;
            this.inherited = original.inherited;
            this.propertyTypes = original.propertyTypes;
            this.fulltextEnabled = original.fulltextEnabled;
            this.relativeProps = original.relativeProps;
            this.relativePropNames = original.relativePropNames;
            this.propertyIndexEnabled = original.propertyIndexEnabled;
        }

        /**
         * Returns <code>true</code> if the property with the given name is
         * indexed according to this rule.
         *
         * @param propertyName the name of a property.
         * @return <code>true</code> if the property is indexed;
         *         <code>false</code> otherwise.
         */
        public boolean isIndexed(String propertyName) {
            return getConfig(propertyName) != null;
        }


        /**
         * Returns the name of the node type where this rule applies to.
         *
         * @return name of the node type.
         */
        public String getNodeTypeName() {
            return nodeTypeName;
        }

        @Override
        public String toString() {
            String str = "IndexRule: "+ nodeTypeName;
            if (!baseNodeType.equals(nodeTypeName)){
                str += "(" + baseNodeType + ")";
            }
            return str;
        }

        /**
         * Returns <code>true</code> if this rule applies to the given node
         * <code>state</code>.
         *
         * @param state the state to check.
         * @return <code>true</code> the rule applies to the given node;
         *         <code>false</code> otherwise.
         */
        public boolean appliesTo(Tree state) {
            if (!nodeTypeName.equals(getPrimaryTypeName(state))) {
                return false;
            }
            //TODO Add support for condition
            //return condition == null || condition.evaluate(state);
            return true;
        }

        public boolean appliesTo(String nodeTypeName) {
            if (!this.nodeTypeName.equals(nodeTypeName)) {
                return false;
            }

            //TODO Once condition support is done return false
            //return condition == null || condition.evaluate(state);
            return true;

        }

        public boolean isFulltextEnabled() {
            return fullTextEnabled;
        }
        /**
         * @param propertyName name of a property.
         * @return the property configuration or <code>null</code> if this
         *         indexing rule does not contain a configuration for the given
         *         property.
         */
        @CheckForNull
        public PropertyDefinition getConfig(String propertyName) {
            PropertyDefinition config = propConfigs.get(propertyName);
            if (config != null) {
                return config;
            } else if (namePatterns.size() > 0) {
                // check patterns
                for (NamePattern np : namePatterns) {
                    if (np.matches(propertyName)) {
                        return np.getConfig();
                    }
                }
            }
            return null;
        }

        public boolean includePropertyType(int type){
            //TODO Rules related to type inclusion need to be synced
            //Login IndexEditor evaluates differently compared to IndexDefinition
            if(propertyTypes == TYPES_ALLOW_ALL){
                return true;
            }

            if (propertyTypes == TYPES_ALLOW_NONE){
                return false;
            }

            return (propertyTypes & (1 << type)) != 0;
        }

        public Collection<RelativeProperty> getRelativeProps() {
            return relativeProps.values();
        }

        private Map<String, PropertyDefinition> collectPropConfigs(NodeState config, List<NamePattern> patterns,
                                                                   Map<String,RelativeProperty> relativeProps) {
            Map<String, PropertyDefinition> propDefns = newHashMap();
            NodeState propNode = config.getChildNode(LuceneIndexConstants.PROP_NODE);

            if (!propNode.exists()){
                return Collections.emptyMap();
            }

            if (!hasOrderableChildren(propNode)){
                log.warn("Properties node for [{}] does not have orderable " +
                "children in [{}]", this, IndexDefinition.this);
            }

            //Include all immediate child nodes to 'properties' node by default
            Tree propTree = new ImmutableTree(propNode);
            for (Tree prop : propTree.getChildren()) {
                String propName = prop.getName();
                NodeState propDefnNode = propNode.getChildNode(propName);
                if (propDefnNode.exists() && !propDefns.containsKey(propName)) {
                    PropertyDefinition pd = new PropertyDefinition(this, propName, propDefnNode);
                    if(pd.isRegexp){
                        patterns.add(new NamePattern(pd.name, pd));
                    } else {
                        propDefns.put(pd.name, pd);
                    }

                    if (RelativeProperty.isRelativeProperty(pd.name)){
                        relativeProps.put(pd.name, new RelativeProperty(pd.name, pd));
                    }
                }
            }
            return ImmutableMap.copyOf(propDefns);
        }

        private boolean hasAnyFullTextEnabledProperty() {
            for (PropertyDefinition pd : propConfigs.values()){
                if (pd.fulltextEnabled()){
                    return true;
                }
            }

            for (NamePattern np : namePatterns){
                if (np.getConfig().fulltextEnabled()){
                    return true;
                }
            }
            return false;
        }

        private boolean hasAnyPropertyIndexConfigured() {
            for (PropertyDefinition pd : propConfigs.values()){
                if (pd.propertyIndex){
                    return true;
                }
            }

            for (NamePattern np : namePatterns){
                if (np.getConfig().propertyIndex){
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * A property name pattern.
     */
    private static final class NamePattern {
        /**
         * The pattern to match.
         */
        private final Pattern pattern;

        /**
         * The associated configuration.
         */
        private final PropertyDefinition config;

        /**
         * Creates a new name pattern.
         *
         * @param pattern the pattern as defined by the property definition
         * @param config the associated configuration.
         */
        private NamePattern(String pattern,
                            PropertyDefinition config){

            this.pattern = Pattern.compile(pattern);
            this.config = config;
        }

        /**
         * @param path property name to match
         * @return <code>true</code> if <code>property name</code> matches this name
         *         pattern; <code>false</code> otherwise.
         */
        boolean matches(String propertyPath) {
            return pattern.matcher(propertyPath).matches();
        }

        PropertyDefinition getConfig() {
            return config;
        }
    }

    //~---------------------------------------------< compatibility >

    public void updateDefinition(NodeBuilder indexDefn){
        //TODO Remove existing config once transformed config is persisted
        NodeState rulesState = definition.getChildNode(LuceneIndexConstants.INDEX_RULES);
        if (!rulesState.exists()){
            rulesState = createIndexRules(definition).getNodeState();
            indexDefn.setChildNode(LuceneIndexConstants.INDEX_RULES, rulesState);
            log.info("Updated index definition for {}", this);
            //indexDefn.removeProperty(DECLARING_NODE_TYPES);
            //indexDefn.removeProperty(INCLUDE_PROPERTY_NAMES);
            //indexDefn.removeProperty(EXCLUDE_PROPERTY_NAMES);
            //indexDefn.removeProperty(ORDERED_PROP_NAMES);
        }
    }

    /**
     * Constructs IndexingRule based on earlier format of index configuration
     */
    private NodeBuilder createIndexRules(NodeState defn){
        NodeBuilder builder = EMPTY_NODE.builder();
        Set<String> declaringNodeTypes = getMultiProperty(defn, DECLARING_NODE_TYPES);
        Set<String> includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        Set<String> excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        Set<String> orderedProps = getMultiProperty(defn, ORDERED_PROP_NAMES);
        boolean fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        boolean storageEnabled = getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);
        NodeState propNodeState = defn.getChildNode(LuceneIndexConstants.PROP_NODE);

        //If no explicit nodeType defined then all config applies for nt:base
        if (declaringNodeTypes.isEmpty()){
            declaringNodeTypes = Collections.singleton(NT_BASE);
        }

        Set<String> propNamesSet = Sets.newHashSet();
        propNamesSet.addAll(includes);
        propNamesSet.addAll(excludes);
        propNamesSet.addAll(orderedProps);

        //Also include all immediate leaf propNode names
        for (ChildNodeEntry cne : propNodeState.getChildNodeEntries()){
            if (!propNamesSet.contains(cne.getName())
                    && Iterables.isEmpty(cne.getNodeState().getChildNodeNames())){
                propNamesSet.add(cne.getName());
            }
        }

        List<String> propNames = new ArrayList<String>(propNamesSet);

        final String includeAllProp = ".*";
        if (fullTextEnabled
                && includes.isEmpty()){
            //Add the regEx for including all properties at the end
            //for fulltext index and when no explicit includes are defined
            propNames.add(includeAllProp);
        }

        for (String typeName : declaringNodeTypes){
            NodeBuilder rule = builder.child(typeName);
            markAsNtUnstructured(rule);
            List<String> propNodeNames = newArrayListWithCapacity(propNamesSet.size());
            NodeBuilder propNodes = rule.child(PROP_NODE);
            int i = 0;
            for (String propName : propNames){
                String propNodeName = propName;

                //For proper propName use the propName as childNode name
                if(RelativeProperty.isRelativeProperty(propName)
                        || propName.equals(includeAllProp)){
                    propNodeName = "prop" + i++;
                }
                propNodeNames.add(propNodeName);

                NodeBuilder prop = propNodes.child(propNodeName);
                markAsNtUnstructured(prop);
                prop.setProperty(LuceneIndexConstants.PROP_NAME, propName);

                if (excludes.contains(propName)){
                    prop.setProperty(LuceneIndexConstants.PROP_INDEX, false);
                } else if (fullTextEnabled){
                    prop.setProperty(LuceneIndexConstants.PROP_ANALYZED, true);
                    prop.setProperty(LuceneIndexConstants.PROP_NODE_SCOPE_INDEX, true);
                    prop.setProperty(LuceneIndexConstants.PROP_USE_IN_EXCERPT, storageEnabled);
                    prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, false);
                } else {
                    prop.setProperty(LuceneIndexConstants.PROP_PROPERTY_INDEX, true);

                    if (orderedProps.contains(propName)){
                        prop.setProperty(LuceneIndexConstants.PROP_ORDERED, true);
                    }
                }

                if (propName.equals(includeAllProp)){
                    prop.setProperty(LuceneIndexConstants.PROP_IS_REGEX, true);
                }

                //Copy over the property configuration
                NodeState propDefNode  = getPropDefnNode(defn, propName);
                if (propDefNode != null){
                    for (PropertyState ps : propDefNode.getProperties()){
                        prop.setProperty(ps);
                    }
                }
            }

            //If no propertyType defined then default to UNKNOWN such that none
            //of the properties get indexed
            PropertyState supportedTypes = defn.getProperty(INCLUDE_PROPERTY_TYPES);
            if (supportedTypes == null){
                supportedTypes = PropertyStates.createProperty(INCLUDE_PROPERTY_TYPES, TYPES_ALLOW_ALL_NAME);
            }
            rule.setProperty(supportedTypes);

            if (!NT_BASE.equals(typeName)) {
                rule.setProperty(LuceneIndexConstants.RULE_INHERITED, false);
            }

            propNodes.setProperty(OAK_CHILD_ORDER, propNodeNames ,NAMES);
            markAsNtUnstructured(propNodes);
        }

        markAsNtUnstructured(builder);
        builder.setProperty(OAK_CHILD_ORDER, declaringNodeTypes ,NAMES);
        return builder;
    }

    private NodeState getPropDefnNode(NodeState defn, String propName){
        NodeState propNode = defn.getChildNode(LuceneIndexConstants.PROP_NODE);
        NodeState propDefNode;
        if (RelativeProperty.isRelativeProperty(propName)) {
            propDefNode = new RelativeProperty(propName).getPropDefnNode(propNode);
        } else {
            propDefNode = propNode.getChildNode(propName);
        }
        return propDefNode.exists() ? propDefNode : null;
    }


    //~---------------------------------------------< utility >

    private static String determineIndexName(NodeState defn, String indexPath) {
        String indexName = defn.getString(PROP_NAME);
        if (indexName ==  null){
            if (indexPath != null) {
                return indexPath;
            }
            return "<No 'name' property defined>";
        }

        if (indexPath != null){
            return indexName + "(" + indexPath + ")";
        }
        return indexName;
    }

    private static Set<String> getMultiProperty(NodeState definition, String propName){
        PropertyState pse = definition.getProperty(propName);
        return pse != null ? ImmutableSet.copyOf(pse.getValue(Type.STRINGS)) : Collections.<String>emptySet();
    }

    private static Set<String> toLowerCase(Set<String> values){
        Set<String> result = newHashSet();
        for(String val : values){
            result.add(val.toLowerCase());
        }
        return ImmutableSet.copyOf(result);
    }

    private static List<String> getAllNodeTypes(ReadOnlyNodeTypeManager ntReg) {
        try {
            List<String> typeNames = newArrayList();
            NodeTypeIterator ntItr = ntReg.getAllNodeTypes();
            while (ntItr.hasNext()){
                typeNames.add(ntItr.nextNodeType().getName());
            }
            return typeNames;
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    private static ReadOnlyNodeTypeManager createNodeTypeManager(final Tree root) {
        return new ReadOnlyNodeTypeManager() {
            @Override
            protected Tree getTypes() {
                return TreeUtil.getTree(root,NODE_TYPES_PATH);
            }

            @Nonnull
            @Override
            protected NamePathMapper getNamePathMapper() {
                return NamePathMapper.DEFAULT;
            }
        };
    }

    private static String getPrimaryTypeName(Tree state) {
        String primaryType = TreeUtil.getPrimaryTypeName(state);
        //In case not a proper JCR assume nt:base TODO return null and ignore indexing such nodes
        //at all
        return primaryType != null ? primaryType : "nt:base";
    }

    private static Iterable<String> getMixinTypeNames(Tree tree) {
        PropertyState property = tree.getProperty(JcrConstants.JCR_MIMETYPE);
        return property != null ? property.getValue(Type.NAMES) : Collections.<String>emptyList();
    }

    private static boolean hasOrderableChildren(NodeState state){
        return state.hasProperty(OAK_CHILD_ORDER);
    }

    private static int getSupportedTypes(NodeState defn, int defaultVal) {
        PropertyState pst = defn.getProperty(INCLUDE_PROPERTY_TYPES);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                if (TYPES_ALLOW_ALL_NAME.equals(inc)){
                    return TYPES_ALLOW_ALL;
                }

                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            return types;
        }
        return defaultVal;
    }

    private static boolean hasFulltextEnabledIndexRule(List<IndexingRule> rules) {
        for (IndexingRule rule : rules){
            if (rule.fulltextEnabled){
                return true;
            }
        }
        return false;
    }

    private static boolean hasPropertyIndexEnabledIndexRule(List<IndexingRule> rules) {
        for (IndexingRule rule : rules){
            if (rule.propertyIndexEnabled){
                return true;
            }
        }
        return false;
    }

    private static Set<String> collectRelPropertyNames(Collection<RelativeProperty> props) {
        Set<String> propNames = newHashSet();
        for (RelativeProperty prop : props) {
            propNames.add(prop.name);
        }
        return ImmutableSet.copyOf(propNames);
    }

    private static List<RelativeProperty> collectRelativeProperties(List<IndexingRule> indexRules) {
        List<RelativeProperty> relProps = newArrayList();
        for (IndexingRule rule : indexRules){
            relProps.addAll(rule.getRelativeProps());
        }
        return ImmutableList.copyOf(relProps);
    }

    private static void markAsNtUnstructured(NodeBuilder nb){
        nb.setProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME);
    }
}
