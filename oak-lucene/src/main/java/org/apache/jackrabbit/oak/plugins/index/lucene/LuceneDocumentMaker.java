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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.binary.BinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FunctionIndexProcessor;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newAncestorsField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newDepthField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newFulltextField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPropertyField;
import static org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil.getPrimaryTypeName;

public class LuceneDocumentMaker {
    private static final Logger log = LoggerFactory.getLogger(LuceneDocumentMaker.class);
    private final BinaryTextExtractor textExtractor;
    private final FacetsConfigProvider facetsConfigProvider;
    private final IndexDefinition definition;
    private final IndexingRule indexingRule;
    private final IndexAugmentorFactory augmentorFactory;
    private final String path;

    public LuceneDocumentMaker(@Nonnull IndexDefinition definition,
                               @Nonnull IndexingRule indexingRule,
                               @Nonnull String path) {
        this(null, null, null, definition, indexingRule, path);
    }

    public LuceneDocumentMaker(@Nullable BinaryTextExtractor textExtractor,
                               @Nullable FacetsConfigProvider facetsConfigProvider,
                               @Nullable IndexAugmentorFactory augmentorFactory,
                               @Nonnull  IndexDefinition definition,
                               @Nonnull IndexingRule indexingRule,
                               @Nonnull String path) {
        this.textExtractor = textExtractor;
        this.facetsConfigProvider = facetsConfigProvider;
        this.definition = checkNotNull(definition);
        this.indexingRule = checkNotNull(indexingRule);
        this.augmentorFactory = augmentorFactory;
        this.path = checkNotNull(path);
    }

    @CheckForNull
    public Document makeDocument(NodeState state) throws IOException {
        return makeDocument(state, false, Collections.<PropertyState>emptyList());
    }

    @CheckForNull
    public Document makeDocument(NodeState state, boolean isUpdate, List<PropertyState> propertiesModified) throws IOException {
        boolean facet = false;

        List<Field> fields = new ArrayList<Field>();
        boolean dirty = false;

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
                dirty |= addTypedOrderedFields(fields, property, pname, pd);
            }

            dirty |= indexProperty(path, fields, state, property, pname, pd);

            facet |= pd.facet;
        }

        boolean[] dirties = indexAggregates(path, fields, state);
        dirty |= dirties[0]; // any (aggregate) indexing happened
        facet |= dirties[1]; // facet indexing during (index-time) aggregation
        dirty |= indexNullCheckEnabledProps(path, fields, state);
        dirty |= indexFunctionRestrictions(path, fields, state);
        dirty |= indexNotNullCheckEnabledProps(path, fields, state);

        dirty |= augmentCustomFields(path, fields, state);

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
            addNodeNameField(fields, name);
            dirty = true;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!indexingRule.indexesAllNodesOfMatchingType() && !dirty){
            return null;
        }

        Document document = new Document();
        document.add(newPathField(path));


        if (indexingRule.isFulltextEnabled()) {
            document.add(newFulltextField(name));
        }

        if (definition.evaluatePathRestrictions()){
            document.add(newAncestorsField(PathUtils.getParentPath(path)));
            document.add(newDepthField(path));
        }

        // because of LUCENE-5833 we have to merge the suggest fields into a single one
        Field suggestField = null;
        for (Field f : fields) {
            if (FieldNames.SUGGEST.equals(f.name())) {
                if (suggestField == null) {
                    suggestField = FieldFactory.newSuggestField(f.stringValue());
                } else {
                    suggestField = FieldFactory.newSuggestField(suggestField.stringValue(), f.stringValue());
                }
            } else {
                document.add(f);
            }
        }
        if (suggestField != null) {
            document.add(suggestField);
        }

        if (facet && isFacetingEnabled()) {
            document = getFacetsConfig().build(document);
        }

        //TODO Boost at document level

        return document;
    }

    private boolean addFacetFields(List<Field> fields, PropertyState property, String pname, PropertyDefinition pd) {
        String facetFieldName = FieldNames.createFacetFieldName(pname);
        getFacetsConfig().setIndexFieldName(pname, facetFieldName);
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

        boolean fieldAdded = false;
        try {
            if (tag == Type.STRINGS.tag() && property.isArray()) {
                getFacetsConfig().setMultiValued(pname, true);
                Iterable<String> values = property.getValue(Type.STRINGS);
                for (String value : values) {
                    if (value != null && value.length() > 0) {
                        fields.add(new SortedSetDocValuesFacetField(pname, value));
                    }
                }
                fieldAdded = true;
            } else if (tag == Type.STRING.tag()) {
                String value = property.getValue(Type.STRING);
                if (value.length() > 0) {
                    fields.add(new SortedSetDocValuesFacetField(pname, value));
                    fieldAdded = true;
                }
            }

        } catch (Throwable e) {
            log.warn("[{}] Ignoring facet property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), path, e);
        }
        return fieldAdded;
    }

    private boolean indexProperty(String path,
                                  List<Field> fields,
                                  NodeState state,
                                  PropertyState property,
                                  String pname,
                                  PropertyDefinition pd) {
        boolean includeTypeForFullText = indexingRule.includePropertyType(property.getType().tag());

        boolean dirty = false;
        if (Type.BINARY.tag() == property.getType().tag()
                && includeTypeForFullText) {
            fields.addAll(newBinary(property, state, null, path + "@" + pname));
            dirty = true;
        } else {
            if (pd.propertyIndex && pd.includePropertyType(property.getType().tag())) {
                dirty |= addTypedFields(fields, property, pname, pd);
            }

            if (pd.fulltextEnabled() && includeTypeForFullText) {
                for (String value : property.getValue(Type.STRINGS)) {

                    if (!includePropertyValue(value, pd)){
                        continue;
                    }

                    if (pd.analyzed && pd.includePropertyType(property.getType().tag())) {
                        String analyzedPropName = constructAnalyzedPropertyName(pname);
                        fields.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
                    }

                    if (pd.useInSuggest) {
                        fields.add(FieldFactory.newSuggestField(value));
                    }

                    if (pd.useInSpellcheck) {
                        fields.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
                    }

                    if (pd.nodeScopeIndex) {
                        Field field = newFulltextField(value);
                        fields.add(field);
                    }
                    dirty = true;
                }
            }
            if (pd.facet && isFacetingEnabled()) {
                dirty |= addFacetFields(fields, property, pname, pd);
            }

        }

        return dirty;
    }

    private String constructAnalyzedPropertyName(String pname) {
        if (definition.getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    private boolean addTypedFields(List<Field> fields, PropertyState property, String pname, PropertyDefinition pd) {
        int tag = property.getType().tag();
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f;
            if (tag == Type.LONG.tag()) {
                f = new LongField(pname, property.getValue(Type.LONG, i), Field.Store.NO);
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE, i);
                f = new LongField(pname, FieldFactory.dateToLong(date), Field.Store.NO);
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleField(pname, property.getValue(Type.DOUBLE, i), Field.Store.NO);
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new StringField(pname, property.getValue(Type.BOOLEAN, i).toString(), Field.Store.NO);
            } else {
                f = new StringField(pname, property.getValue(Type.STRING, i), Field.Store.NO);
            }

            if (includePropertyValue(property, i, pd)){
                fields.add(f);
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean addTypedOrderedFields(List<Field> fields,
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

        String name = FieldNames.createDocValFieldName(pname);
        boolean fieldAdded = false;
        Field f = null;
        try {
            if (tag == Type.LONG.tag()) {
                //TODO Distinguish fields which need to be used for search and for sort
                //If a field is only used for Sort then it can be stored with less precision
                f = new NumericDocValuesField(name, property.getValue(Type.LONG));
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE);
                f = new NumericDocValuesField(name, FieldFactory.dateToLong(date));
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleDocValuesField(name, property.getValue(Type.DOUBLE));
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new SortedDocValuesField(name,
                    new BytesRef(property.getValue(Type.BOOLEAN).toString()));
            } else if (tag == Type.STRING.tag()) {
                f = new SortedDocValuesField(name,
                    new BytesRef(property.getValue(Type.STRING)));
            }

            if (f != null && includePropertyValue(property, 0, pd)) {
                fields.add(f);
                fieldAdded = true;
            }
        } catch (Exception e) {
            log.warn(
                    "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), path, e);
        }
        return fieldAdded;
    }

    private boolean includePropertyValue(PropertyState property, int i, PropertyDefinition pd) {
        if (property.getType().tag() == PropertyType.BINARY){
            return true;
        }

        if (pd.valuePattern.matchesAll()) {
            return true;
        }

        return includePropertyValue(property.getValue(Type.STRING, i), pd);
    }

    private boolean includePropertyValue(String value, PropertyDefinition pd){
        return pd.valuePattern.matches(value);
    }

    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }

    private List<Field> newBinary(
            PropertyState property, NodeState state, String nodePath, String path) {
        if (textExtractor == null){
            //Skip text extraction for sync indexing
            return Collections.emptyList();
        }

        return textExtractor.newBinary(property, state, nodePath, path);
    }

    private boolean augmentCustomFields(final String path, final List<Field> fields,
                                        final NodeState document) {
        boolean dirty = false;

        if (augmentorFactory != null) {
            Iterable<Field> augmentedFields = augmentorFactory
                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
                    .getAugmentedFields(path, document, definition.getDefinitionNodeState());

            for (Field field : augmentedFields) {
                fields.add(field);
                dirty = true;
            }
        }

        return dirty;
    }

    //~-------------------------------------------------------< NullCheck Support >

    private boolean indexNotNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNotNullCheckEnabledProperties()) {
            if (isPropertyNotNull(state, pd)) {
                fields.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNullCheckEnabledProperties()) {
            if (isPropertyNull(state, pd)) {
                fields.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexFunctionRestrictions(String path, List<Field> fields, NodeState state) {
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

    private static PropertyState calculateValue(String path, NodeState state, String[] functionCode) {
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

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * <bold>not</bold> considered to be null</p>
     *
     * @return true if the property does not exist
     */
    private boolean isPropertyNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return !propertyNode.hasProperty(pd.nonRelativeName);
    }

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * considered to be null</p>
     *
     * @return true if the property exists
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

    /**
     * index aggregates on a certain path
     * @param path the path of the node
     * @param fields the list of fields
     * @param state the node state
     * @return an array of booleans whose first element is {@code true} if any indexing has happened
     * and the second element is {@code true} if facets on any (aggregate) property have been indexed
     */
    private boolean[] indexAggregates(final String path, final List<Field> fields,
                                    final NodeState state) {
        final AtomicBoolean dirtyFlag = new AtomicBoolean();
        final AtomicBoolean facetFlag = new AtomicBoolean();
        indexingRule.getAggregate().collectAggregates(state, new Aggregate.ResultCollector() {
            @Override
            public void onResult(Aggregate.NodeIncludeResult result) {
                boolean dirty = indexAggregatedNode(path, fields, result);
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }

            @Override
            public void onResult(Aggregate.PropertyIncludeResult result) {
                boolean dirty = false;
                if (result.pd.ordered) {
                    dirty |= addTypedOrderedFields(fields, result.propertyState,
                            result.propertyPath, result.pd);
                }
                dirty |= indexProperty(path, fields, state, result.propertyState,
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
    /**
     * Create the fulltext field from the aggregated nodes. If result is for aggregate for a relative node
     * include then
     * @param path current node path
     * @param fields indexed fields
     * @param result aggregate result
     * @return true if a field was created for passed node result
     */
    private boolean indexAggregatedNode(String path, List<Field> fields, Aggregate.NodeIncludeResult result) {
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
                fields.addAll(newBinary(property, result.nodeState, nodePath, aggreagtedNodePath + "@" + pname));
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
                    Field field = result.isRelativeNode() ?
                            newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
                    if (pd != null) {
                        field.setBoost(pd.boost);
                    }
                    fields.add(field);
                    dirty = true;
                }
            }
        }
        return dirty;
    }

    private String getIndexName() {
        return definition.getIndexName();
    }

    private boolean isFacetingEnabled(){
        return facetsConfigProvider != null;
    }

    private FacetsConfig getFacetsConfig(){
        return facetsConfigProvider.getFacetsConfig();
    }

    /**
     * Extracts the local name of the current node ignoring any namespace prefix
     *
     * @param name node name
     */
    private static void addNodeNameField(List<Field> fields, String name) {
        //TODO Need to check if it covers all cases
        int colon = name.indexOf(':');
        String value = colon < 0 ? name : name.substring(colon + 1);

        //For now just add a single term. Later we can look into using different analyzer
        //to analyze the node name and add multiple terms. Like add multiple terms for a
        //cameCase file name to allow faster like search
        fields.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }
}
