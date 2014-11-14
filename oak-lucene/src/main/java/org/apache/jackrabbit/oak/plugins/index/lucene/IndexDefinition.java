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
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.lucene.codecs.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
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
import static org.apache.jackrabbit.oak.plugins.index.lucene.PropertyDefinition.DEFAULT_BOOST;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getOptionalValue;
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

    private final int propertyTypes;

    private final Set<String> excludes;

    private final Set<String> includes;

    private final Set<String> orderedProps;

    private final Set<String> declaringNodeTypes;

    private final boolean fullTextEnabled;

    private final boolean storageEnabled;

    private final NodeState definition;

    private final NodeState root;

    private final Map<String, PropertyDefinition> propDefns;

    private final String funcName;

    private final int blobSize;

    private final Codec codec;

    private final Map<String,RelativeProperty> relativeProps;

    private final Set<String> relativePropNames;

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

    private final String indexName;

    public IndexDefinition(NodeState root, NodeState defn) {
        this(root, defn, null);
    }

    public IndexDefinition(NodeState root, NodeState defn, @Nullable String indexPath) {
        this.root = root;
        this.definition = defn;
        this.indexName = determineIndexName(defn, indexPath);
        PropertyState pst = defn.getProperty(INCLUDE_PROPERTY_TYPES);
        if (pst != null) {
            int types = 0;
            for (String inc : pst.getValue(Type.STRINGS)) {
                try {
                    types |= 1 << PropertyType.valueFromName(inc);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown property type: " + inc);
                }
            }
            this.propertyTypes = types;
        } else {
            this.propertyTypes = -1;
        }

        this.excludes = toLowerCase(getMultiProperty(defn, EXCLUDE_PROPERTY_NAMES));
        this.includes = getMultiProperty(defn, INCLUDE_PROPERTY_NAMES);
        this.orderedProps = getMultiProperty(defn, ORDERED_PROP_NAMES);
        this.declaringNodeTypes = getMultiProperty(defn, DECLARING_NODE_TYPES);

        this.blobSize = getOptionalValue(defn, BLOB_SIZE, DEFAULT_BLOB_SIZE);

        this.fullTextEnabled = getOptionalValue(defn, FULL_TEXT_ENABLED, true);
        //Storage is disabled for non full text indexes
        this.storageEnabled = this.fullTextEnabled && getOptionalValue(defn, EXPERIMENTAL_STORAGE, true);
        //TODO Flag out invalid propertyNames like one which are absolute
        this.relativeProps = collectRelativeProps(Iterables.concat(includes, orderedProps));
        this.propDefns = collectPropertyDefns(defn);

        this.indexRules = collectIndexRules(defn.getChildNode(LuceneIndexConstants.INDEX_RULES));

        this.relativePropNames = collectRelPropertyNames(this.relativeProps.values());
        this.relativePropsMaxLevels = getRelPropertyMaxLevels(this.relativeProps.values());

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

    boolean isOrdered(String name) {
        return orderedProps.contains(name);
    }

    public NodeState getDefinition() {
        return definition;
    }

    public boolean isFullTextEnabled() {
        return fullTextEnabled;
    }

    public int getPropertyTypes() {
        return propertyTypes;
    }

    public Set<String> getDeclaringNodeTypes() {
        return declaringNodeTypes;
    }

    public boolean hasDeclaredNodeTypes(){
        return !declaringNodeTypes.isEmpty();
    }
    /**
     * Checks if a given property should be stored in the lucene index or not
     */
    public boolean isStored(String name) {
        return storageEnabled;
    }

    public boolean skipTokenization(String propertyName) {
        //If fulltext is not enabled then we never tokenize
        //irrespective of property name
        if (!isFullTextEnabled()) {
            return true;
        }
        return LuceneIndexHelper.skipTokenization(propertyName);
    }

    @CheckForNull
    public PropertyDefinition getPropDefn(String propName){
        return propDefns.get(propName);
    }

    public boolean hasPropertyDefinition(String propName){
        return propDefns.containsKey(propName);
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
        return relativeProps.values();
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
            for(RelativeProperty rp : relativeProps.values()){
                if(rp.name.equals(name)){
                    relProps.add(rp);
                }
            }
        }
    }

    boolean hasRelativeProperties(){
        return !relativePropNames.isEmpty();
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

    private Map<String, PropertyDefinition> collectPropertyDefns(NodeState defn) {
        Map<String, PropertyDefinition> propDefns = newHashMap();
        NodeState propNode = defn.getChildNode(LuceneIndexConstants.PROP_NODE);
        //Include all immediate child nodes to 'properties' node by default
        for (String propName : Iterables.concat(includes, orderedProps, propNode.getChildNodeNames())) {
            NodeState propDefnNode;
            if (relativeProps.containsKey(propName)) {
                propDefnNode = relativeProps.get(propName).getPropDefnNode(propNode);
            } else {
                propDefnNode = propNode.getChildNode(propName);
            }

            if (propDefnNode.exists() && !propDefns.containsKey(propName)) {
                propDefns.put(propName, new PropertyDefinition(this, propName, propDefnNode));
            }
        }
        return ImmutableMap.copyOf(propDefns);
    }

    private Set<String> collectRelPropertyNames(Collection<RelativeProperty> props) {
        Set<String> propNames = newHashSet();
        for (RelativeProperty prop : props) {
            propNames.add(prop.name);
        }
        return ImmutableSet.copyOf(propNames);
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

    private Map<String, List<IndexingRule>> collectIndexRules(NodeState indexRules){
        //TODO if a rule is defined for nt:base then this map would have entry for each
        //registered nodeType in the system

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

        IndexingRule(String nodeTypeName, NodeState config) {
            this.nodeTypeName = nodeTypeName;
            this.baseNodeType = nodeTypeName;
            this.boost = getOptionalValue(config, FIELD_BOOST, DEFAULT_BOOST);
            this.inherited = getOptionalValue(config, LuceneIndexConstants.RULE_INHERITED, true);

            List<NamePattern> namePatterns = newArrayList();
            this.propConfigs = collectPropConfigs(config, namePatterns);

            this.namePatterns = ImmutableList.copyOf(namePatterns);
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
            this.inherited = original.inherited;
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
        private boolean appliesTo(Tree state) {
            if (!nodeTypeName.equals(getPrimaryTypeName(state))) {
                return false;
            }
            //TODO Add support for condition
            //return condition == null || condition.evaluate(state);
            return true;
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

        private Map<String, PropertyDefinition> collectPropConfigs(NodeState config, List<NamePattern> patterns) {
            Map<String, PropertyDefinition> propDefns = newHashMap();
            NodeState propNode = config.getChildNode(LuceneIndexConstants.PROP_NODE);

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
                    PropertyDefinition pd = new PropertyDefinition(IndexDefinition.this, propName, propDefnNode);
                    if(pd.isRegexp){
                        patterns.add(new NamePattern(pd.name, pd));
                    } else {
                        propDefns.put(pd.name, pd);
                    }
                }
            }
            return ImmutableMap.copyOf(propDefns);
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
        //In case not a proper JCR assume nt:base
        return primaryType != null ? primaryType : "nt:base";
    }

    private static Iterable<String> getMixinTypeNames(Tree tree) {
        PropertyState property = tree.getProperty(JcrConstants.JCR_MIMETYPE);
        return property != null ? property.getValue(Type.NAMES) : Collections.<String>emptyList();
    }

    private static boolean hasOrderableChildren(NodeState state){
        return state.hasProperty(OAK_CHILD_ORDER);
    }
}
