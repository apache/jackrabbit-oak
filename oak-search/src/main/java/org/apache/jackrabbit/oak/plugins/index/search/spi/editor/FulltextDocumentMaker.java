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

package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.jcr.PropertyType;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.Aggregate;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.PropertyDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil.getPrimaryTypeName;

/**
 * Abstract implementation of a {@link DocumentMaker}.
 *
 * D is the type of entities / documents to be indexed specific to subclasses implementations.
 */
public abstract class FulltextDocumentMaker<D> implements DocumentMaker<D> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    public static final String WARN_LOG_STRING_SIZE_THRESHOLD_KEY = "oak.repository.property.index.logWarnStringSizeThreshold";
    private static final int DEFAULT_WARN_LOG_STRING_SIZE_THRESHOLD_VALUE = 102400;

    private final FulltextBinaryTextExtractor textExtractor;
    protected final IndexDefinition definition;
    protected final IndexDefinition.IndexingRule indexingRule;
    protected final String path;
    private final int logWarnStringSizeThreshold;

    public FulltextDocumentMaker(@Nullable FulltextBinaryTextExtractor textExtractor,
                               @NotNull IndexDefinition definition,
                               IndexDefinition.IndexingRule indexingRule,
                               @NotNull String path) {
        this.textExtractor = textExtractor;
        this.definition = checkNotNull(definition);
        this.indexingRule = checkNotNull(indexingRule);
        this.path = checkNotNull(path);
        this.logWarnStringSizeThreshold = Integer.getInteger(WARN_LOG_STRING_SIZE_THRESHOLD_KEY,
                DEFAULT_WARN_LOG_STRING_SIZE_THRESHOLD_VALUE);
    }

    protected abstract D initDoc();

    protected abstract D finalizeDoc(D doc, boolean dirty, boolean facet) throws IOException;

    protected abstract boolean isFacetingEnabled();

    protected abstract boolean indexTypeOrderedFields(D doc, String pname, int tag, PropertyState property, PropertyDefinition pd);

    protected abstract boolean addBinary(D doc, String path, List<String> binaryValues);

    protected abstract boolean indexFacetProperty(D doc, int tag, PropertyState property, String pname);

    protected abstract void indexAnalyzedProperty(D doc, String pname, String value, PropertyDefinition pd);

    protected abstract void indexSuggestValue(D doc, String value);

    protected abstract void indexSpellcheckValue(D doc, String value);

    protected abstract void indexFulltextValue(D doc, String value);

    protected abstract void indexTypedProperty(D doc, PropertyState property, String pname, PropertyDefinition pd, int index);

    protected abstract void indexAncestors(D doc, String path);

    protected abstract void indexNotNullProperty(D doc, PropertyDefinition pd);

    protected abstract void indexNullProperty(D doc, PropertyDefinition pd);

    protected abstract void indexAggregateValue(D doc, Aggregate.NodeIncludeResult result, String value, PropertyDefinition pd);

    protected abstract void indexNodeName(D doc, String value);

    protected void logLargeStringProperties(String propertyName, String value) {
        if (value.length() > logWarnStringSizeThreshold) {
            log.warn("String length: {} for property: {} at Node: {} is greater than configured value {}",
                    value.length(), propertyName, path, logWarnStringSizeThreshold);
        }
    }

    @Nullable
    public D makeDocument(NodeState state) throws IOException {
        return makeDocument(state, false, Collections.<PropertyState>emptyList());
    }

    @Nullable
    public D makeDocument(NodeState state, boolean isUpdate, List<PropertyState> propertiesModified) throws IOException {
        boolean facet = false;

        D document = initDoc();
        boolean dirty = false;
        Pattern propertyRegex = definition.getPropertyRegex();

        //We 'intentionally' are indexing node names only on root state as we don't support indexing relative or
        //regex for node name indexing
        PropertyState nodenamePS =
                new StringPropertyState(FieldNames.NODE_NAME, getName(path));
        for (PropertyState property : Iterables.concat(state.getProperties(), Collections.singleton(nodenamePS))) {
            String pname = property.getName();

            if (!isVisible(pname) && !FieldNames.NODE_NAME.equals(pname)) {
                continue;
            }

            PropertyDefinition pd = indexingRule.getConfig(pname);

            if (pd == null || !pd.index){
                continue;
            }

            if (pd.ordered) {
                dirty |= addTypedOrderedFields(document, property, pname, pd);
            }

            dirty |= indexProperty(path, document, state, property, pname, pd);

            facet |= pd.facet;
        }

        boolean[] dirties = indexAggregates(path, document, state);
        dirty |= dirties[0]; // any (aggregate) indexing happened
        facet |= dirties[1]; // facet indexing during (index-time) aggregation
        dirty |= indexNullCheckEnabledProps(path, document, state);
        dirty |= indexFunctionRestrictions(path, document, state);
        dirty |= indexNotNullCheckEnabledProps(path, document, state);

        dirty |= augmentCustomFields(path, document, state);

        // Check if a node having a single property was modified/deleted
        if (!dirty) {
            dirty = indexIfSinglePropertyRemoved(propertiesModified);
        }

        if (isUpdate && !dirty) {
            // updated the state but had no relevant changes
            return null;
        }

        String name = getName(path);
        if (indexingRule.isNodeNameIndexed()){
            addNodeNameField(document, name);
            dirty = true;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!indexingRule.indexesAllNodesOfMatchingType() && !dirty){
            return null;
        }

        if (indexingRule.isFulltextEnabled()) {
            boolean shouldAdd = propertyRegex == null || propertyRegex.matcher(name).find();
            if (shouldAdd) {
                indexFulltextValue(document, name);
            }
        }

        if (definition.evaluatePathRestrictions()){
            indexAncestors(document, path);
        }

        return finalizeDoc(document, dirty, facet);
    }


    private boolean indexFacets(D doc, PropertyState property, String pname, PropertyDefinition pd) {
        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            log.debug("[{}] Facet property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), path);
            tag = idxDefinedTag;
        }
        return indexFacetProperty(doc, tag, property, pname);
    }

    private boolean indexProperty(String path,
                                  D doc,
                                  NodeState state,
                                  PropertyState property,
                                  String pname,
                                  PropertyDefinition pd) {
        boolean includeTypeForFullText = indexingRule.includePropertyType(property.getType().tag());

        boolean dirty = false;
        if (Type.BINARY.tag() == property.getType().tag() && pd.useInSimilarity) {
            try {
                log.trace("indexing similarity binaries for {}", pd.name);
                indexSimilarityBinaries(doc, pd, property.getValue(Type.BINARY));
                dirty = true;
            } catch (Exception e) {
                log.error("could not index similarity field for property {} and definition {}", property, pd);
            }
        } else if (Type.BINARY.tag() == property.getType().tag()
                && includeTypeForFullText) {
            List<String> binaryValues = newBinary(property, state, path + "@" + pname);
            addBinary(doc, null, binaryValues);
            dirty = true;
        } else {
            if (pd.propertyIndex && pd.includePropertyType(property.getType().tag())) {
                dirty |= addTypedFields(doc, property, pname, pd);
            }

            if (pd.fulltextEnabled() && includeTypeForFullText) {
                for (String value : property.getValue(Type.STRINGS)) {
                    logLargeStringProperties(property.getName(), value);
                    if (definition.getPropertyRegex() != null && !definition.getPropertyRegex().matcher(value).find()) {
                        continue;
                    }
                    if (!includePropertyValue(value, pd)){
                        continue;
                    }

                    if (pd.analyzed && pd.includePropertyType(property.getType().tag())) {
                        indexAnalyzedProperty(doc, pname, value, pd);
                    }

                    if (pd.useInSuggest) {
                        indexSuggestValue(doc, value);
                    }

                    if (pd.useInSpellcheck) {
                        indexSpellcheckValue(doc, value);
                    }

                    if (pd.nodeScopeIndex) {
                        indexFulltextValue(doc, value);
                        if (pd.useInSimilarity) {
                            log.trace("indexing similarity strings for {}", pd.name);
                            try {
                                // fallback for when feature vectors are written in string typed properties
                                indexSimilarityStrings(doc, pd, value);
                            } catch (Exception e) {
                                log.error("could not index similarity field for property {} and definition {}", property, pd);
                            }
                        }
                    }
                    dirty = true;
                }
            }
            if (pd.facet && isFacetingEnabled()) {
                dirty |= indexFacets(doc, property, pname, pd);
            }
            if (pd.similarityTags) {
                dirty |= indexSimilarityTag(doc, property);
            }

        }

        return dirty;
    }

    protected abstract boolean indexSimilarityTag(D doc, PropertyState property);

    protected abstract void indexSimilarityBinaries(D doc, PropertyDefinition pd, Blob blob) throws IOException;

    protected abstract void indexSimilarityStrings(D doc, PropertyDefinition pd, String value) throws IOException;

    private boolean addTypedFields(D doc, PropertyState property, String pname, PropertyDefinition pd) {
        int tag = property.getType().tag();
        boolean fieldAdded = false;

        for (int i = 0; i < property.count(); i++) {
            if (!includePropertyValue(property, i, pd)) {
                continue;
            }

            indexTypedProperty(doc, property, pname, pd, i);
            fieldAdded = true;

            if (tag == Type.STRING.tag()) {
                logLargeStringProperties(property.getName(), property.getValue(Type.STRING, i));
            }
        }
        return fieldAdded;
    }

    private boolean addTypedOrderedFields(D doc,
                                          PropertyState property,
                                          String pname,
                                          PropertyDefinition pd) {
        // Ignore and warn if property multi-valued as not supported
        if (property.getType().isArray()) {
            log.warn(
                    "[{}] Ignoring ordered property {} of type {} for path {} as multivalued ordered property not supported",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), true), path);
            return false;
        }

        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            log.debug(
                    "[{}] Ordered property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), path);
            tag = idxDefinedTag;
        }
        return indexTypeOrderedFields(doc, pname, tag, property, pd);
    }

    protected boolean includePropertyValue(PropertyState property, int i, PropertyDefinition pd) {
        if (property.getType().tag() == PropertyType.BINARY){
            return true;
        }

        if (pd.valuePattern.matchesAll()) {
            return true;
        }

        return includePropertyValue(property.getValue(Type.STRING, i), pd);
    }

    protected boolean includePropertyValue(String value, PropertyDefinition pd){
        return pd.valuePattern.matches(value);
    }

    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }

    private List<String> newBinary(
        PropertyState property, NodeState state, String path) {
        if (textExtractor == null){
            //Skip text extraction for sync indexing
            return Collections.emptyList();
        }

        return textExtractor.newBinary(property, state, path);
    }

    // TODO : extract more generic SPI for augmentor factory
    protected abstract boolean augmentCustomFields(final String path, final D doc, final NodeState document);// {
//        boolean dirty = false;
//
//        if (augmentorFactory != null) {
//            Iterable<Field> augmentedFields = augmentorFactory
//                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
//                    .getAugmentedFields(path, document, definition.getDefinitionNodeState());
//
//            for (Field field : augmentedFields) {
//                fields.add(field);
//                dirty = true;
//            }
//        }
//
//        return dirty;
//    }

    //~-------------------------------------------------------< NullCheck Support >

    private boolean indexNotNullCheckEnabledProps(String path, D doc, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNotNullCheckEnabledProperties()) {
            if (isPropertyNotNull(state, pd)) {
                indexNotNullProperty(doc, pd);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }


    private boolean indexNullCheckEnabledProps(String path, D doc, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNullCheckEnabledProperties()) {
            if (isPropertyNull(state, pd)) {
                indexNullProperty(doc, pd);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexFunctionRestrictions(String path, D fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getFunctionRestrictions()) {
            PropertyState functionValue = calculateValue(path, state, pd.functionCode);
            if (functionValue != null) {
                if (pd.ordered) {
                    addTypedOrderedFields(fields, functionValue, pd.function, pd);
                }
                addTypedFields(fields, functionValue, pd.function, pd);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private PropertyState calculateValue(String path, NodeState state, String[] functionCode) {
        try {
            return FunctionIndexProcessor.tryCalculateValue(path, state, functionCode);
        } catch (RuntimeException e) {
            log.error("Failed to calculate function value for {} at {}",
                    Arrays.toString(functionCode), path, e);
            throw e;
        }
    }

    private boolean indexIfSinglePropertyRemoved(List<PropertyState> propertiesModified) {
        boolean dirty = false;
        for (PropertyState ps : propertiesModified) {
            PropertyDefinition pd = indexingRule.getConfig(ps.getName());
            if (pd != null
                    && pd.index
                    && (pd.includePropertyType(ps.getType().tag())
                            || indexingRule.includePropertyType(ps.getType().tag()))) {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    /*
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * For relative property if the intermediate nodes do not exist then property is
     * not considered to be null
     *
     */
    private boolean isPropertyNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return !propertyNode.hasProperty(pd.nonRelativeName);
    }

    /*
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * For relative property if the intermediate nodes do not exist then property is
     * considered to be null
     */
    private boolean isPropertyNotNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return propertyNode.hasProperty(pd.nonRelativeName);
    }

    private static NodeState getPropertyNode(NodeState nodeState, PropertyDefinition pd) {
        if (!pd.relative){
            return nodeState;
        }
        NodeState node = nodeState;
        for (String name : pd.ancestors) {
            node = node.getChildNode(name);
        }
        return node;
    }

    /*
     * index aggregates on a certain path
     */
    private boolean[] indexAggregates(final String path, final D document,
                                    final NodeState state) {
        final AtomicBoolean dirtyFlag = new AtomicBoolean();
        final AtomicBoolean facetFlag = new AtomicBoolean();
        indexingRule.getAggregate().collectAggregates(state, new Aggregate.ResultCollector() {
            @Override
            public void onResult(Aggregate.NodeIncludeResult result) {
                boolean dirty = indexAggregatedNode(path, document, result);
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }

            @Override
            public void onResult(Aggregate.PropertyIncludeResult result) {
                boolean dirty = false;
                if (result.pd.ordered) {
                    dirty |= addTypedOrderedFields(document, result.propertyState,
                            result.propertyPath, result.pd);
                }
                dirty |= indexProperty(path, document, state, result.propertyState,
                        result.propertyPath, result.pd);

                if (result.pd.facet) {
                    facetFlag.set(true);
                }
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }
        });
        return new boolean[]{dirtyFlag.get(), facetFlag.get()};
    }

    /*
     * Create the fulltext field from the aggregated nodes. If result is for aggregate for a relative node
     * include then
     */
    private boolean indexAggregatedNode(String path, D doc, Aggregate.NodeIncludeResult result) {
        //rule for node being aggregated might be null if such nodes
        //are not indexed on there own. In such cases we rely in current
        //rule for some checks
        IndexDefinition.IndexingRule ruleAggNode = definition
                .getApplicableIndexingRule(getPrimaryTypeName(result.nodeState));
        boolean dirty = false;

        for (PropertyState property : result.nodeState.getProperties()){
            String pname = property.getName();
            String propertyPath = PathUtils.concat(result.nodePath, pname);

            if (!isVisible(pname)) {
                continue;
            }

            //Check if type is indexed
            int type = property.getType().tag();
            if (ruleAggNode != null ) {
                if (!ruleAggNode.includePropertyType(type)) {
                    continue;
                }
            } else if (!indexingRule.includePropertyType(type)){
                continue;
            }

            //Check if any explicit property defn is defined via relative path
            // and is marked to exclude this property from being indexed. We exclude
            //it from aggregation if
            // 1. Its not to be indexed i.e. index=false
            // 2. Its explicitly excluded from aggregation i.e. excludeFromAggregation=true
            PropertyDefinition pdForRootNode = indexingRule.getConfig(propertyPath);
            if (pdForRootNode != null && (!pdForRootNode.index || pdForRootNode.excludeFromAggregate)) {
                continue;
            }

            if (Type.BINARY == property.getType()) {
                String aggreagtedNodePath = PathUtils.concat(path, result.nodePath);
                //Here the fulltext is being created for aggregate root hence nodePath passed
                //should be null
                String nodePath = result.isRelativeNode() ? result.rootIncludePath : null;
                List<String> binaryValues = newBinary(property, result.nodeState, aggreagtedNodePath + "@" + pname);
                addBinary(doc, nodePath, binaryValues);
                dirty = true;
            } else {
                PropertyDefinition pd = null;
                if (ruleAggNode != null){
                    pd = ruleAggNode.getConfig(pname);
                }

                if (pd != null && !pd.nodeScopeIndex){
                    continue;
                }

                for (String value : property.getValue(Type.STRINGS)) {
                    indexAggregateValue(doc, result, value, pd);
                    dirty = true;
                }
            }
        }
        return dirty;
    }

    protected String getIndexName() {
        return definition.getIndexName();
    }

    /*
     * Extracts the local name of the current node ignoring any namespace prefix
     */
    private void addNodeNameField(D doc, String name) {
        //TODO Need to check if it covers all cases
        int colon = name.indexOf(':');
        String value = colon < 0 ? name : name.substring(colon + 1);

        //For now just add a single term. Later we can look into using different analyzer
        //to analyze the node name and add multiple terms. Like add multiple terms for a
        //cameCase file name to allow faster like search
        indexNodeName(doc, value);
    }


}
